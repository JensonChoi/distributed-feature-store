from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from feature_store.models import Feature, RegistryManifest, ValueType
from feature_store.registry import Registry, RegistryConflictError


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
