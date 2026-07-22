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
    RegistryManifest,
    StreamSource,
)


class RegistryConflictError(ValueError):
    pass


class RegistryNotFoundError(KeyError):
    pass


def _fingerprint(spec: dict[str, Any]) -> str:
    raw = json.dumps(spec, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


class Registry:
    def __init__(self, session: Session):
        self.session = session

    def apply(self, manifest: RegistryManifest) -> ApplyResult:
        self._validate_references(manifest)
        created = 0
        unchanged = 0
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
        for kind, name, version, spec in objects:
            fingerprint = _fingerprint(spec)
            existing = self.session.scalar(
                select(RegistryRecord).where(
                    RegistryRecord.kind == kind,
                    RegistryRecord.name == name,
                    RegistryRecord.version == version,
                )
            )
            if existing:
                if existing.fingerprint != fingerprint:
                    suffix = f"@{version}" if version else ""
                    self.session.rollback()
                    raise RegistryConflictError(
                        f"immutable registry object changed: {name}{suffix}"
                    )
                unchanged += 1
                continue
            self.session.add(
                RegistryRecord(
                    kind=kind,
                    name=name,
                    version=version,
                    fingerprint=fingerprint,
                    spec=spec,
                )
            )
            created += 1
        self.session.commit()
        return ApplyResult(fingerprint=manifest.fingerprint(), created=created, unchanged=unchanged)

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

    def _validate_references(self, manifest: RegistryManifest) -> None:
        entities = {item.name for item in manifest.entities} | set(self._names("entity"))
        batch_sources = {item.name for item in manifest.batch_sources} | set(
            self._names("batch_source")
        )
        stream_sources = {item.name for item in manifest.stream_sources} | set(
            self._names("stream_source")
        )
        views = {(item.name, item.version): item for item in manifest.feature_views}
        for view in manifest.feature_views:
            if view.entity not in entities:
                raise RegistryNotFoundError(f"unknown entity: {view.entity}")
            if view.batch_source not in batch_sources:
                raise RegistryNotFoundError(f"unknown batch source: {view.batch_source}")
            if view.stream_source and view.stream_source not in stream_sources:
                raise RegistryNotFoundError(f"unknown stream source: {view.stream_source}")
        for service in manifest.feature_services:
            for ref in service.features:
                view_ref, feature_name = ref.rsplit(":", 1)
                name, version = parse_view_ref(view_ref)
                manifest_view = views.get((name, version))
                if manifest_view is None:
                    try:
                        manifest_view = self.feature_view(view_ref)
                    except RegistryNotFoundError as exc:
                        raise RegistryNotFoundError(f"unknown feature view: {view_ref}") from exc
                if feature_name not in {item.name for item in manifest_view.features}:
                    raise RegistryNotFoundError(f"unknown feature: {ref}")

    def _names(self, kind: str) -> list[str]:
        return list(
            self.session.scalars(
                select(RegistryRecord.name).where(RegistryRecord.kind == kind).distinct()
            )
        )


def parse_view_ref(ref: str) -> tuple[str, str]:
    try:
        name, version = ref.split("@", 1)
    except ValueError as exc:
        raise ValueError("feature view reference must be name@x.y.z") from exc
    return name, version
