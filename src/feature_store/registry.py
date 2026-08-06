from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from feature_store.db import RegistryRecord
from feature_store.models import (
    ApplyResult,
    BatchSource,
    Entity,
    FeatureService,
    FeatureView,
    RegistryDifference,
    RegistryDiffOperation,
    RegistryIssue,
    RegistryManifest,
    RegistryObjectIdentity,
    RegistryObjectPlan,
    RegistryObjectStatus,
    RegistryPlan,
    RegistryPlanSummary,
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
            identity = RegistryObjectIdentity(
                kind=kind, name=name, version=version or None
            )
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
                    elif feature_name not in {
                        feature.name for feature in referenced_view.features
                    }:
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
        )

    def list_records(self, kind: str | None = None) -> list[dict[str, Any]]:
        statement = select(RegistryRecord).order_by(
            RegistryRecord.kind, RegistryRecord.name, RegistryRecord.version
        )
        if kind:
            statement = statement.where(RegistryRecord.kind == kind)
        return [
            {
                "kind": row.kind,
                "name": row.name,
                "version": row.version or None,
                "fingerprint": row.fingerprint,
                "spec": row.spec,
            }
            for row in self.session.scalars(statement)
        ]

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
            ("feature_view", item.name, item.version, item.model_dump(mode="json"))
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
