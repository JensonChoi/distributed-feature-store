from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from feature_store.db import RegistryLifecycleRecord, RegistryRecord
from feature_store.models import (
    ApplyResult,
    BatchSource,
    Entity,
    FeatureService,
    FeatureView,
    RegistryDeprecation,
    RegistryDescriptor,
    RegistryDifference,
    RegistryDiffOperation,
    RegistryIssue,
    RegistryManifest,
    RegistryMetadata,
    RegistryMetadataPatch,
    RegistryObjectIdentity,
    RegistryObjectPlan,
    RegistryObjectStatus,
    RegistryPlan,
    RegistryPlanSummary,
    RegistryProvenance,
    RegistryTarget,
    RegistryWarning,
    StreamSource,
)


class RegistryConflictError(ValueError):
    pass


class RegistryNotFoundError(KeyError):
    pass


def _fingerprint(spec: dict[str, Any]) -> str:
    raw = json.dumps(spec, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


def _json_pointer(path: str, part: str | int) -> str:
    escaped = str(part).replace("~", "~0").replace("/", "~1")
    return f"{path}/{escaped}"


def _differences(existing: Any, proposed: Any, path: str = "") -> list[RegistryDifference]:
    if isinstance(existing, dict) and isinstance(proposed, dict):
        output: list[RegistryDifference] = []
        for key in sorted(existing.keys() | proposed.keys()):
            child_path = _json_pointer(path, key)
            if key not in existing:
                output.append(
                    RegistryDifference(
                        path=child_path,
                        operation=RegistryDiffOperation.ADDED,
                        proposed=proposed[key],
                    )
                )
            elif key not in proposed:
                output.append(
                    RegistryDifference(
                        path=child_path,
                        operation=RegistryDiffOperation.REMOVED,
                        existing=existing[key],
                    )
                )
            else:
                output.extend(_differences(existing[key], proposed[key], child_path))
        return output
    if isinstance(existing, list) and isinstance(proposed, list):
        output = []
        for index in range(max(len(existing), len(proposed))):
            child_path = _json_pointer(path, index)
            if index >= len(existing):
                output.append(
                    RegistryDifference(
                        path=child_path,
                        operation=RegistryDiffOperation.ADDED,
                        proposed=proposed[index],
                    )
                )
            elif index >= len(proposed):
                output.append(
                    RegistryDifference(
                        path=child_path,
                        operation=RegistryDiffOperation.REMOVED,
                        existing=existing[index],
                    )
                )
            else:
                output.extend(_differences(existing[index], proposed[index], child_path))
        return output
    if existing != proposed:
        return [
            RegistryDifference(
                path=path or "/",
                operation=RegistryDiffOperation.CHANGED,
                existing=existing,
                proposed=proposed,
            )
        ]
    return []


class Registry:
    def __init__(self, session: Session):
        self.session = session

    def apply(self, manifest: RegistryManifest) -> ApplyResult:
        plan = self.plan(manifest)
        rejected = [item for item in plan.objects if item.status == RegistryObjectStatus.REJECTED]
        if rejected:
            issue_owner, issue = next(
                (
                    (item, issue)
                    for item in rejected
                    for issue in item.issues
                    if issue.code != "immutable_conflict"
                ),
                (rejected[0], rejected[0].issues[0]),
            )
            if issue.code == "immutable_conflict":
                identity = issue_owner.identity
                suffix = f"@{identity.version}" if identity.version else ""
                raise RegistryConflictError(
                    f"immutable registry object changed: {identity.name}{suffix}"
                )
            raise RegistryNotFoundError(issue.message)
        objects = self._manifest_objects(manifest)
        for object_plan, (kind, name, version, spec) in zip(plan.objects, objects, strict=True):
            if object_plan.status != RegistryObjectStatus.CREATED:
                continue
            self.session.add(
                RegistryRecord(
                    kind=kind,
                    name=name,
                    version=version,
                    fingerprint=_fingerprint(spec),
                    spec=spec,
                )
            )
        self.session.commit()
        return ApplyResult(
            fingerprint=plan.fingerprint,
            created=plan.summary.created,
            unchanged=plan.summary.unchanged,
            warnings=plan.warnings,
        )

    def validate(self, manifest: RegistryManifest) -> RegistryPlan:
        return self.plan(manifest)

    def plan(self, manifest: RegistryManifest) -> RegistryPlan:
        with self.session.no_autoflush:
            stored = {
                (record.kind, record.name, record.version): record
                for record in self.session.scalars(select(RegistryRecord))
            }
        available_entities = {name for kind, name, _ in stored if kind == "entity"}
        available_batch_sources = {name for kind, name, _ in stored if kind == "batch_source"}
        available_stream_sources = {name for kind, name, _ in stored if kind == "stream_source"}
        available_views: dict[tuple[str, str], FeatureView] = {
            (name, version): FeatureView.model_validate(record.spec)
            for (kind, name, version), record in stored.items()
            if kind == "feature_view"
        }
        plans: list[RegistryObjectPlan] = []
        for kind, name, version, spec in self._manifest_objects(manifest):
            identity = RegistryObjectIdentity(kind=kind, name=name, version=version or None)
            existing = stored.get((kind, name, version))
            differences: list[RegistryDifference] = []
            issues: list[RegistryIssue] = []
            if existing is not None and existing.fingerprint != _fingerprint(spec):
                differences = _differences(existing.spec, spec)
                suffix = f"@{version}" if version else ""
                issues.append(
                    RegistryIssue(
                        code="immutable_conflict",
                        path="/",
                        message=f"immutable registry object changed: {name}{suffix}",
                    )
                )
            if kind == "feature_view":
                view = FeatureView.model_validate(spec)
                if view.entity not in available_entities:
                    issues.append(
                        RegistryIssue(
                            code="missing_entity",
                            path="/entity",
                            message=f"unknown entity: {view.entity}",
                        )
                    )
                if view.batch_source not in available_batch_sources:
                    issues.append(
                        RegistryIssue(
                            code="missing_batch_source",
                            path="/batch_source",
                            message=f"unknown batch source: {view.batch_source}",
                        )
                    )
                if view.stream_source and view.stream_source not in available_stream_sources:
                    issues.append(
                        RegistryIssue(
                            code="missing_stream_source",
                            path="/stream_source",
                            message=f"unknown stream source: {view.stream_source}",
                        )
                    )
            if kind == "feature_service":
                service = FeatureService.model_validate(spec)
                for index, ref in enumerate(service.features):
                    view_ref, feature_name = ref.rsplit(":", 1)
                    view_name, view_version = parse_view_ref(view_ref)
                    referenced_view = available_views.get((view_name, view_version))
                    if referenced_view is None:
                        issues.append(
                            RegistryIssue(
                                code="missing_feature_view",
                                path=f"/features/{index}",
                                message=f"unknown feature view: {view_ref}",
                            )
                        )
                    elif feature_name not in {feature.name for feature in referenced_view.features}:
                        issues.append(
                            RegistryIssue(
                                code="missing_feature",
                                path=f"/features/{index}",
                                message=f"unknown feature: {ref}",
                            )
                        )
            if issues:
                status = RegistryObjectStatus.REJECTED
            elif existing is not None:
                status = RegistryObjectStatus.UNCHANGED
            else:
                status = RegistryObjectStatus.CREATED
            plans.append(
                RegistryObjectPlan(
                    identity=identity,
                    status=status,
                    issues=issues,
                    differences=differences,
                )
            )
            if status in {RegistryObjectStatus.CREATED, RegistryObjectStatus.UNCHANGED}:
                if kind == "entity":
                    available_entities.add(name)
                elif kind == "batch_source":
                    available_batch_sources.add(name)
                elif kind == "stream_source":
                    available_stream_sources.add(name)
                elif kind == "feature_view":
                    available_views[(name, version)] = FeatureView.model_validate(spec)
        return RegistryPlan(
            fingerprint=manifest.fingerprint(),
            summary=RegistryPlanSummary(
                created=sum(item.status == RegistryObjectStatus.CREATED for item in plans),
                unchanged=sum(item.status == RegistryObjectStatus.UNCHANGED for item in plans),
                rejected=sum(item.status == RegistryObjectStatus.REJECTED for item in plans),
            ),
            objects=plans,
            warnings=self.warnings_for_targets(self._manifest_warning_targets(manifest)),
        )

    def list_records(self, kind: str | None = None) -> list[dict[str, Any]]:
        statement = select(RegistryRecord).order_by(
            RegistryRecord.kind, RegistryRecord.name, RegistryRecord.version
        )
        if kind:
            statement = statement.where(RegistryRecord.kind == kind)
        output: list[dict[str, Any]] = []
        for row in self.session.scalars(statement):
            target = self._target_for_record(row)
            descriptor = self._descriptor(target, row)
            output.append(
                {
                    "kind": row.kind,
                    "name": row.name,
                    "version": row.version or None,
                    "fingerprint": row.fingerprint,
                    "spec": row.spec,
                    "metadata": descriptor.metadata.model_dump(mode="json"),
                    "provenance": descriptor.provenance.model_dump(mode="json"),
                    "deprecation": descriptor.deprecation.model_dump(mode="json"),
                    "updated_at": descriptor.updated_at,
                }
            )
        return output

    def describe(self, target: RegistryTarget) -> RegistryDescriptor:
        record = self._record_for_target(target)
        return self._descriptor(target, record)

    describe_target = describe

    def patch_metadata(
        self, target: RegistryTarget, patch: RegistryMetadataPatch
    ) -> RegistryDescriptor:
        record = self._record_for_target(target)
        lifecycle = self._lifecycle(record, target.feature)
        if lifecycle is None:
            lifecycle = RegistryLifecycleRecord(
                registry_record_id=record.id,
                feature_name=target.feature,
                owners=[],
                tags={},
                documentation_links=[],
                status="active",
                updated_at=datetime.now(UTC),
            )
            self.session.add(lifecycle)
        fields = patch.model_fields_set
        if "owners" in fields:
            lifecycle.owners = list(patch.owners or [])
        if "tags" in fields:
            lifecycle.tags = dict(patch.tags or {})
        if "documentation_links" in fields:
            lifecycle.documentation_links = list(patch.documentation_links or [])
        lifecycle.updated_at = datetime.now(UTC)
        self.session.commit()
        return self._descriptor(target, record)

    def deprecate(
        self,
        target: RegistryTarget,
        message: str | None = None,
        replacement: RegistryTarget | None = None,
    ) -> RegistryDescriptor:
        record = self._record_for_target(target)
        if replacement is not None:
            self._validate_replacement(target, replacement)
        for dependent_lifecycle in self.session.scalars(
            select(RegistryLifecycleRecord).where(
                RegistryLifecycleRecord.status == "deprecated",
                RegistryLifecycleRecord.replacement.is_not(None),
            )
        ):
            if dependent_lifecycle.replacement == target.model_dump(mode="json"):
                dependent = self._target_for_lifecycle(dependent_lifecycle)
                raise RegistryConflictError(
                    f"cannot deprecate active replacement {target.ref}; "
                    f"repoint dependent {dependent.ref} first"
                )
        lifecycle = self._lifecycle(record, target.feature)
        if lifecycle is None:
            lifecycle = RegistryLifecycleRecord(
                registry_record_id=record.id,
                feature_name=target.feature,
                owners=[],
                tags={},
                documentation_links=[],
            )
            self.session.add(lifecycle)
        now = datetime.now(UTC)
        lifecycle.status = "deprecated"
        lifecycle.deprecated_at = now
        lifecycle.deprecation_message = message
        lifecycle.replacement = (
            replacement.model_dump(mode="json") if replacement is not None else None
        )
        lifecycle.updated_at = now
        self.session.commit()
        return self._descriptor(target, record)

    def reactivate(self, target: RegistryTarget) -> RegistryDescriptor:
        record = self._record_for_target(target)
        lifecycle = self._lifecycle(record, target.feature)
        if lifecycle is not None:
            lifecycle.status = "active"
            lifecycle.deprecated_at = None
            lifecycle.deprecation_message = None
            lifecycle.replacement = None
            lifecycle.updated_at = datetime.now(UTC)
            self.session.commit()
        return self._descriptor(target, record)

    def warnings_for_query(
        self, features: Iterable[str], feature_service: str | None = None
    ) -> list[RegistryWarning]:
        refs = list(features)
        targets: list[RegistryTarget] = []
        if feature_service:
            targets.append(RegistryTarget(kind="feature_service", name=feature_service))
            refs = self.feature_service(feature_service).features
        for ref in refs:
            view_ref, feature_name = ref.rsplit(":", 1)
            view_name, view_version = parse_view_ref(view_ref)
            view = self.feature_view(view_ref)
            targets.extend(
                [
                    RegistryTarget(
                        kind="feature_view",
                        name=view_name,
                        version=view_version,
                        feature=feature_name,
                    ),
                    RegistryTarget(kind="feature_view", name=view_name, version=view_version),
                    RegistryTarget(kind="entity", name=view.entity),
                    RegistryTarget(kind="batch_source", name=view.batch_source),
                ]
            )
            if view.stream_source:
                targets.append(RegistryTarget(kind="stream_source", name=view.stream_source))
        return self.warnings_for_targets(targets)

    def warnings_for_targets(self, targets: Iterable[RegistryTarget]) -> list[RegistryWarning]:
        warnings: dict[tuple[str, str, str, str, str], RegistryWarning] = {}
        for target in targets:
            try:
                record = self._record_for_target(target)
            except RegistryNotFoundError:
                continue
            direct = self._lifecycle(record, target.feature)
            if direct is not None and direct.status == "deprecated":
                warning = self._warning(target, direct)
                warnings[self._warning_key(warning)] = warning
            if target.feature is not None:
                parent_target = target.model_copy(update={"feature": None})
                parent = self._lifecycle(record, None)
                if parent is not None and parent.status == "deprecated":
                    warning = self._warning(target, parent, inherited_from=parent_target)
                    warnings[self._warning_key(warning)] = warning
        return sorted(warnings.values(), key=self._warning_key)

    resolve_warnings = warnings_for_targets

    def entity(self, name: str) -> Entity:
        return Entity.model_validate(self._get("entity", name).spec)

    def batch_source(self, name: str) -> BatchSource:
        return BatchSource.model_validate(self._get("batch_source", name).spec)

    def stream_source(self, name: str) -> StreamSource:
        return StreamSource.model_validate(self._get("stream_source", name).spec)

    def feature_view(self, ref: str) -> FeatureView:
        name, version = parse_view_ref(ref)
        return FeatureView.model_validate(self._get("feature_view", name, version).spec)

    def feature_service(self, name: str) -> FeatureService:
        return FeatureService.model_validate(self._get("feature_service", name).spec)

    def resolve_features(
        self, features: Iterable[str], feature_service: str | None = None
    ) -> list[str]:
        refs = list(features)
        if feature_service:
            refs = self.feature_service(feature_service).features
        if not refs:
            raise RegistryNotFoundError("no features requested")
        output_names: set[str] = set()
        for ref in refs:
            view_ref, feature_name = ref.rsplit(":", 1)
            view = self.feature_view(view_ref)
            if feature_name not in {feature.name for feature in view.features}:
                raise RegistryNotFoundError(f"unknown feature: {ref}")
            output_name = f"{view.name}__{feature_name}"
            if output_name in output_names:
                raise ValueError(
                    f"ambiguous output {output_name}; query duplicate or multi-version "
                    "features separately"
                )
            output_names.add(output_name)
        return refs

    def _get(self, kind: str, name: str, version: str = "") -> RegistryRecord:
        record = self.session.scalar(
            select(RegistryRecord).where(
                RegistryRecord.kind == kind,
                RegistryRecord.name == name,
                RegistryRecord.version == version,
            )
        )
        if not record:
            suffix = f"@{version}" if version else ""
            raise RegistryNotFoundError(f"unknown {kind}: {name}{suffix}")
        return record

    def _record_for_target(self, target: RegistryTarget) -> RegistryRecord:
        record = self._get(target.kind.value, target.name, target.version or "")
        if target.feature is not None:
            view = FeatureView.model_validate(record.spec)
            if target.feature not in {feature.name for feature in view.features}:
                raise RegistryNotFoundError(f"unknown feature: {target.ref}")
        return record

    def _lifecycle(
        self, record: RegistryRecord, feature_name: str | None
    ) -> RegistryLifecycleRecord | None:
        return self.session.scalar(
            select(RegistryLifecycleRecord).where(
                RegistryLifecycleRecord.registry_record_id == record.id,
                RegistryLifecycleRecord.feature_name == feature_name,
            )
        )

    def _descriptor(self, target: RegistryTarget, record: RegistryRecord) -> RegistryDescriptor:
        lifecycle = self._lifecycle(record, target.feature)
        spec = record.spec
        if target.feature is not None:
            view = FeatureView.model_validate(record.spec)
            spec = next(
                feature.registry_spec()
                for feature in view.features
                if feature.name == target.feature
            )
        metadata = RegistryMetadata(
            owners=list(lifecycle.owners) if lifecycle else [],
            tags=dict(lifecycle.tags) if lifecycle else {},
            documentation_links=list(lifecycle.documentation_links) if lifecycle else [],
        )
        deprecation = RegistryDeprecation(
            status=lifecycle.status if lifecycle else "active",
            deprecated_at=(
                self._utc(lifecycle.deprecated_at)
                if lifecycle and lifecycle.deprecated_at
                else None
            ),
            message=lifecycle.deprecation_message if lifecycle else None,
            replacement=(
                RegistryTarget.model_validate(lifecycle.replacement)
                if lifecycle and lifecycle.replacement
                else None
            ),
        )
        return RegistryDescriptor(
            target=target,
            fingerprint=record.fingerprint,
            spec=spec,
            provenance=RegistryProvenance(
                created_at=self._utc(record.created_at),
                manifest_fingerprint=record.fingerprint,
            ),
            metadata=metadata,
            deprecation=deprecation,
            updated_at=(
                self._utc(lifecycle.updated_at) if lifecycle and lifecycle.updated_at else None
            ),
        )

    def _validate_replacement(self, target: RegistryTarget, replacement: RegistryTarget) -> None:
        if replacement == target:
            raise RegistryConflictError("a deprecated target cannot replace itself")
        if replacement.kind != target.kind or bool(replacement.feature) != bool(target.feature):
            raise RegistryConflictError(
                "replacement must match the target kind and feature granularity"
            )
        replacement_record = self._record_for_target(replacement)
        replacement_lifecycle = self._lifecycle(replacement_record, replacement.feature)
        if replacement_lifecycle is not None and replacement_lifecycle.status == "deprecated":
            raise RegistryConflictError(f"replacement is deprecated: {replacement.ref}")
        if replacement.feature is not None:
            parent = self._lifecycle(replacement_record, None)
            if parent is not None and parent.status == "deprecated":
                raise RegistryConflictError(f"replacement parent is deprecated: {replacement.ref}")
        current = replacement
        visited: set[str] = set()
        while current.ref not in visited:
            if current == target:
                raise RegistryConflictError("replacement would create a lifecycle cycle")
            visited.add(current.ref)
            current_record = self._record_for_target(current)
            lifecycle = self._lifecycle(current_record, current.feature)
            if lifecycle is None or not lifecycle.replacement:
                return
            current = RegistryTarget.model_validate(lifecycle.replacement)
        raise RegistryConflictError("replacement chain contains a lifecycle cycle")

    def _warning(
        self,
        target: RegistryTarget,
        lifecycle: RegistryLifecycleRecord,
        *,
        inherited_from: RegistryTarget | None = None,
    ) -> RegistryWarning:
        deprecated_at = lifecycle.deprecated_at or lifecycle.updated_at
        source = inherited_from or target
        message = lifecycle.deprecation_message or f"registry target is deprecated: {source.ref}"
        return RegistryWarning(
            target=target,
            message=message,
            deprecated_at=self._utc(deprecated_at),
            replacement=(
                RegistryTarget.model_validate(lifecycle.replacement)
                if lifecycle.replacement
                else None
            ),
            inherited_from=inherited_from,
        )

    @staticmethod
    def _warning_key(warning: RegistryWarning) -> tuple[str, str, str, str, str]:
        target = warning.target
        inherited = warning.inherited_from.ref if warning.inherited_from else ""
        return (
            target.kind.value,
            target.name,
            target.version or "",
            target.feature or "",
            inherited,
        )

    def _target_for_lifecycle(self, lifecycle: RegistryLifecycleRecord) -> RegistryTarget:
        record = self.session.get(RegistryRecord, lifecycle.registry_record_id)
        if record is None:
            raise RegistryNotFoundError("lifecycle metadata has no registry record")
        return self._target_for_record(record, lifecycle.feature_name)

    @staticmethod
    def _target_for_record(
        record: RegistryRecord, feature_name: str | None = None
    ) -> RegistryTarget:
        return RegistryTarget(
            kind=record.kind,
            name=record.name,
            version=record.version or None,
            feature=feature_name,
        )

    @staticmethod
    def _utc(value: datetime) -> datetime:
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

    @staticmethod
    def _manifest_targets(manifest: RegistryManifest) -> list[RegistryTarget]:
        targets: list[RegistryTarget] = []
        targets.extend(RegistryTarget(kind="entity", name=item.name) for item in manifest.entities)
        targets.extend(
            RegistryTarget(kind="batch_source", name=item.name) for item in manifest.batch_sources
        )
        targets.extend(
            RegistryTarget(kind="stream_source", name=item.name) for item in manifest.stream_sources
        )
        for view in manifest.feature_views:
            targets.extend(
                [
                    RegistryTarget(kind="feature_view", name=view.name, version=view.version),
                    RegistryTarget(kind="entity", name=view.entity),
                    RegistryTarget(kind="batch_source", name=view.batch_source),
                ]
            )
            targets.extend(
                RegistryTarget(
                    kind="feature_view",
                    name=view.name,
                    version=view.version,
                    feature=feature.name,
                )
                for feature in view.features
            )
            if view.stream_source:
                targets.append(RegistryTarget(kind="stream_source", name=view.stream_source))
        for service in manifest.feature_services:
            targets.append(RegistryTarget(kind="feature_service", name=service.name))
            for ref in service.features:
                view_ref, feature_name = ref.rsplit(":", 1)
                view_name, view_version = parse_view_ref(view_ref)
                targets.extend(
                    [
                        RegistryTarget(
                            kind="feature_view",
                            name=view_name,
                            version=view_version,
                            feature=feature_name,
                        ),
                        RegistryTarget(kind="feature_view", name=view_name, version=view_version),
                    ]
                )
        return targets

    def _manifest_warning_targets(self, manifest: RegistryManifest) -> list[RegistryTarget]:
        targets = self._manifest_targets(manifest)
        for service in manifest.feature_services:
            for ref in service.features:
                view_ref, _ = ref.rsplit(":", 1)
                try:
                    view = self.feature_view(view_ref)
                except RegistryNotFoundError:
                    continue
                targets.extend(
                    [
                        RegistryTarget(kind="entity", name=view.entity),
                        RegistryTarget(kind="batch_source", name=view.batch_source),
                    ]
                )
                if view.stream_source:
                    targets.append(RegistryTarget(kind="stream_source", name=view.stream_source))
        return targets

    @staticmethod
    def _manifest_objects(
        manifest: RegistryManifest,
    ) -> list[tuple[str, str, str, dict[str, Any]]]:
        objects: list[tuple[str, str, str, dict[str, Any]]] = []
        objects.extend(
            ("entity", item.name, "", item.model_dump(mode="json")) for item in manifest.entities
        )
        objects.extend(
            ("batch_source", item.name, "", item.model_dump(mode="json"))
            for item in manifest.batch_sources
        )
        objects.extend(
            ("stream_source", item.name, "", item.model_dump(mode="json"))
            for item in manifest.stream_sources
        )
        objects.extend(
            ("feature_view", item.name, item.version, item.registry_spec())
            for item in manifest.feature_views
        )
        objects.extend(
            ("feature_service", item.name, "", item.model_dump(mode="json"))
            for item in manifest.feature_services
        )
        return objects


def parse_view_ref(ref: str) -> tuple[str, str]:
    try:
        name, version = ref.split("@", 1)
    except ValueError as exc:
        raise ValueError("feature view reference must be name@x.y.z") from exc
    return name, version
