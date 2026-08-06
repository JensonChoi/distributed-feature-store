from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from feature_store.models import (
    BatchSource,
    Entity,
    Feature,
    FeatureService,
    FeatureView,
    RegistryManifest,
    RegistryObjectStatus,
    ValueType,
)
from feature_store.registry import Registry, RegistryConflictError, RegistryNotFoundError


def test_apply_is_idempotent(session: Session, manifest: RegistryManifest) -> None:
    registry = Registry(session)
    first = registry.apply(manifest)
    second = registry.apply(manifest)
    assert first.created == 5
    assert second.created == 0
    assert second.unchanged == 5
    assert first.fingerprint == second.fingerprint


def test_changed_feature_version_conflicts(session: Session, manifest: RegistryManifest) -> None:
    registry = Registry(session)
    registry.apply(manifest)
    changed = manifest.model_copy(deep=True)
    changed.feature_views[0].features.append(Feature(name="count", dtype=ValueType.INT64))
    with pytest.raises(RegistryConflictError, match="account_stats@1.0.0"):
        registry.apply(changed)


def test_feature_service_resolves_exact_reference(
    session: Session, manifest: RegistryManifest
) -> None:
    registry = Registry(session)
    registry.apply(manifest)
    assert registry.resolve_features([], "fraud_v1") == ["account_stats@1.0.0:amount"]


def test_plan_classifies_objects_in_manifest_order_without_writes(
    session: Session, manifest: RegistryManifest
) -> None:
    registry = Registry(session)
    plan = registry.plan(manifest)
    assert plan.fingerprint == manifest.fingerprint()
    assert plan.summary.model_dump() == {"created": 5, "unchanged": 0, "rejected": 0}
    assert [item.identity.kind for item in plan.objects] == [
        "entity",
        "batch_source",
        "stream_source",
        "feature_view",
        "feature_service",
    ]
    assert {item.status for item in plan.objects} == {RegistryObjectStatus.CREATED}
    assert registry.list_records() == []

    registry.apply(manifest)
    unchanged = registry.validate(manifest)
    assert unchanged.summary.model_dump() == {"created": 0, "unchanged": 5, "rejected": 0}
    assert {item.status for item in unchanged.objects} == {RegistryObjectStatus.UNCHANGED}
    assert len(registry.list_records()) == 5


def test_plan_reports_recursive_conflict_differences(
    session: Session, manifest: RegistryManifest
) -> None:
    registry = Registry(session)
    existing = manifest.model_copy(deep=True)
    existing.feature_views[0].features.append(Feature(name="count", dtype=ValueType.INT64))
    registry.apply(existing)

    changed = manifest.model_copy(deep=True)
    changed.feature_views[0].features[0].dtype = ValueType.INT64
    plan = registry.plan(changed)
    view = next(item for item in plan.objects if item.identity.kind == "feature_view")
    assert view.status == RegistryObjectStatus.REJECTED
    assert [issue.code for issue in view.issues] == ["immutable_conflict"]
    assert [(diff.path, diff.operation.value) for diff in view.differences] == [
        ("/features/0/dtype", "changed"),
        ("/features/1", "removed"),
    ]
    assert view.differences[0].existing == "float64"
    assert view.differences[0].proposed == "int64"

    added = existing.model_copy(deep=True)
    added.feature_views[0].features.append(Feature(name="score", dtype=ValueType.FLOAT64))
    added_view = next(
        item for item in registry.plan(added).objects if item.identity.kind == "feature_view"
    )
    assert added_view.differences[-1].path == "/features/2"
    assert added_view.differences[-1].operation.value == "added"


def test_plan_aggregates_missing_references_and_rejects_dependencies(
    session: Session, manifest: RegistryManifest
) -> None:
    registry = Registry(session)
    registry.apply(manifest)
    proposed = RegistryManifest(
        feature_views=[
            FeatureView(
                name="missing_inputs",
                version="1.0.0",
                entity="absent_entity",
                features=[Feature(name="amount", dtype=ValueType.FLOAT64)],
                batch_source="absent_batch",
                stream_source="absent_stream",
                batch_sql="SELECT amount FROM source",
            )
        ],
        feature_services=[
            FeatureService(
                name="broken_service",
                features=[
                    "missing_inputs@1.0.0:amount",
                    "account_stats@1.0.0:absent_feature",
                ],
            )
        ],
    )
    plan = registry.validate(proposed)
    assert plan.summary.model_dump() == {"created": 0, "unchanged": 0, "rejected": 2}
    assert [issue.code for issue in plan.objects[0].issues] == [
        "missing_entity",
        "missing_batch_source",
        "missing_stream_source",
    ]
    assert [issue.code for issue in plan.objects[1].issues] == [
        "missing_feature_view",
        "missing_feature",
    ]
    assert len(registry.list_records()) == 5


def test_partial_manifest_can_reference_stored_objects(
    session: Session, manifest: RegistryManifest
) -> None:
    registry = Registry(session)
    registry.apply(manifest)
    partial = RegistryManifest(
        feature_views=[
            FeatureView(
                name="account_totals",
                version="1.0.0",
                entity="account",
                features=[Feature(name="total", dtype=ValueType.FLOAT64)],
                batch_source="transactions",
                stream_source="account_stream",
                batch_sql="SELECT total FROM source",
            )
        ],
        feature_services=[
            FeatureService(name="totals_service", features=["account_totals@1.0.0:total"])
        ],
    )
    assert registry.plan(partial).summary.model_dump() == {
        "created": 2,
        "unchanged": 0,
        "rejected": 0,
    }


def test_apply_preflight_is_atomic_for_reference_errors(session: Session) -> None:
    registry = Registry(session)
    invalid = RegistryManifest(
        entities=[Entity(name="account", join_keys={"account_id": ValueType.STRING})],
        batch_sources=[BatchSource(name="transactions", uri="file:///tmp/source")],
        feature_views=[
            FeatureView(
                name="bad_view",
                version="1.0.0",
                entity="missing_entity",
                features=[Feature(name="amount", dtype=ValueType.FLOAT64)],
                batch_source="transactions",
                batch_sql="SELECT amount FROM source",
            )
        ],
    )
    with pytest.raises(RegistryNotFoundError, match="unknown entity"):
        registry.apply(invalid)
    assert registry.list_records() == []
