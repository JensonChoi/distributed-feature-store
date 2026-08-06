from __future__ import annotations

from datetime import UTC

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from feature_store.api import app, get_session
from feature_store.db import RegistryLifecycleRecord
from feature_store.models import (
    Feature,
    FeatureView,
    RegistryManifest,
    RegistryMetadataPatch,
    RegistryTarget,
    ValueType,
)
from feature_store.registry import Registry, RegistryConflictError, RegistryNotFoundError


def target(
    kind: str,
    name: str,
    *,
    version: str | None = None,
    feature: str | None = None,
) -> RegistryTarget:
    return RegistryTarget(kind=kind, name=name, version=version, feature=feature)  # type: ignore[arg-type]


def add_second_view(registry: Registry) -> None:
    registry.apply(
        RegistryManifest(
            feature_views=[
                FeatureView(
                    name="account_stats",
                    version="2.0.0",
                    entity="account",
                    features=[
                        Feature(name="amount", dtype=ValueType.FLOAT64),
                        Feature(name="count", dtype=ValueType.INT64),
                    ],
                    batch_source="transactions",
                    stream_source="account_stream",
                    batch_sql="SELECT amount, count FROM source",
                )
            ]
        )
    )


def test_metadata_is_sparse_typed_and_does_not_change_immutable_provenance(
    session: Session, manifest: RegistryManifest
) -> None:
    registry = Registry(session)
    registry.apply(manifest)
    view = target("feature_view", "account_stats", version="1.0.0")
    before = registry.describe(view)
    feature = registry.describe(
        target("feature_view", "account_stats", version="1.0.0", feature="amount")
    )
    assert feature.metadata.model_dump() == {
        "owners": [],
        "tags": {},
        "documentation_links": [],
    }
    assert feature.updated_at is None

    updated = registry.patch_metadata(
        view,
        RegistryMetadataPatch(
            owners=["fraud-team"],
            tags={"tier": "critical"},
            documentation_links=["https://docs.example.com/features/account-stats"],
        ),
    )
    assert updated.metadata.owners == ["fraud-team"]
    assert updated.metadata.tags == {"tier": "critical"}
    assert updated.provenance.created_at.tzinfo == UTC
    assert updated.provenance == before.provenance
    assert updated.fingerprint == before.fingerprint
    assert registry.list_records("feature_view")[0]["fingerprint"] == before.fingerprint

    cleared = registry.patch_metadata(view, RegistryMetadataPatch(owners=[]))
    assert cleared.metadata.owners == []
    assert cleared.metadata.tags == {"tier": "critical"}


def test_feature_targets_must_exist(session: Session, manifest: RegistryManifest) -> None:
    registry = Registry(session)
    registry.apply(manifest)
    with pytest.raises(RegistryNotFoundError, match="unknown feature"):
        registry.describe(
            target("feature_view", "account_stats", version="1.0.0", feature="missing")
        )


def test_replacement_rules_and_reactivation_preserve_metadata(
    session: Session, manifest: RegistryManifest
) -> None:
    registry = Registry(session)
    registry.apply(manifest)
    add_second_view(registry)
    old = target("feature_view", "account_stats", version="1.0.0", feature="amount")
    new = target("feature_view", "account_stats", version="2.0.0", feature="amount")
    registry.patch_metadata(old, RegistryMetadataPatch(owners=["fraud-team"]))

    deprecated = registry.deprecate(old, "use v2", new)
    assert deprecated.deprecation.status == "deprecated"
    assert deprecated.deprecation.replacement == new
    with pytest.raises(RegistryConflictError, match="active replacement"):
        registry.deprecate(new)
    with pytest.raises(RegistryConflictError, match="replace itself"):
        registry.deprecate(old, replacement=old)
    with pytest.raises(RegistryConflictError, match="granularity"):
        registry.deprecate(
            old, replacement=target("feature_view", "account_stats", version="2.0.0")
        )

    active = registry.reactivate(old)
    assert active.deprecation.status == "active"
    assert active.deprecation.message is None
    assert active.deprecation.replacement is None
    assert active.metadata.owners == ["fraud-team"]
    registry.deprecate(new)


def test_failed_replacement_validation_is_atomic(
    session: Session, manifest: RegistryManifest
) -> None:
    registry = Registry(session)
    registry.apply(manifest)
    old = target("feature_view", "account_stats", version="1.0.0")
    with pytest.raises(RegistryNotFoundError):
        registry.deprecate(
            old,
            replacement=target("feature_view", "missing_view", version="2.0.0"),
        )
    count = session.scalar(select(func.count()).select_from(RegistryLifecycleRecord))
    assert count == 0
    assert registry.describe(old).deprecation.status == "active"


def test_warnings_include_feature_inheritance_and_dependency_chain(
    session: Session, manifest: RegistryManifest
) -> None:
    registry = Registry(session)
    registry.apply(manifest)
    view = target("feature_view", "account_stats", version="1.0.0")
    entity = target("entity", "account")
    service = target("feature_service", "fraud_v1")
    registry.deprecate(view, "view retiring")
    registry.deprecate(entity, "entity retiring")
    registry.deprecate(service, "service retiring")

    warnings = registry.warnings_for_query([], "fraud_v1")
    assert warnings == sorted(warnings, key=registry._warning_key)
    assert {
        (warning.target.ref, warning.inherited_from.ref if warning.inherited_from else None)
        for warning in warnings
    } == {
        ("entity:account", None),
        ("feature_service:fraud_v1", None),
        ("feature_view:account_stats@1.0.0", None),
        ("feature_view:account_stats@1.0.0:amount", "feature_view:account_stats@1.0.0"),
    }

    planned = registry.validate(manifest)
    assert planned.warnings
    applied = registry.apply(manifest)
    assert applied.warnings == planned.warnings


def test_lifecycle_api_and_inline_historical_warnings(
    session: Session, manifest: RegistryManifest
) -> None:
    def override_session():  # type: ignore[no-untyped-def]
        yield session

    app.dependency_overrides[get_session] = override_session
    client = TestClient(app, raise_server_exceptions=False)
    try:
        assert (
            client.post("/v1/registry/apply", json=manifest.model_dump(mode="json")).status_code
            == 200
        )
        params = {
            "kind": "feature_view",
            "name": "account_stats",
            "version": "1.0.0",
            "feature": "amount",
        }
        described = client.get("/v1/registry/object", params=params)
        assert described.status_code == 200
        assert described.json()["metadata"]["owners"] == []

        target_payload = {"kind": "feature_view", "name": "account_stats", "version": "1.0.0"}
        patched = client.patch(
            "/v1/registry/object/metadata",
            json={"target": target_payload, "patch": {"owners": ["fraud-team"]}},
        )
        assert patched.status_code == 200
        deprecated = client.post(
            "/v1/registry/object:deprecate",
            json={"target": target_payload, "message": "retiring"},
        )
        assert deprecated.status_code == 200

        response = client.post(
            "/v1/historical-features:query",
            json={"observations": [], "features": [], "feature_service": "fraud_v1"},
        )
        assert response.status_code == 200
        assert response.json()["warnings"]
        activated = client.post("/v1/registry/object:activate", json={"target": target_payload})
        assert activated.status_code == 200
        assert activated.json()["metadata"]["owners"] == ["fraud-team"]
    finally:
        client.close()
        app.dependency_overrides.clear()
