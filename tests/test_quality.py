from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest
from pydantic import ValidationError

from feature_store.models import Feature, FeatureQuality, FeatureView, RegistryManifest, ValueType
from feature_store.quality import validate_quality_event, validate_quality_table


def test_contract_schema_and_dtype_compatibility() -> None:
    assert FeatureQuality().nullable is True
    with pytest.raises(ValidationError, match="cannot exceed"):
        FeatureQuality(minimum=2, maximum=1)
    with pytest.raises(ValidationError, match="must be unique"):
        FeatureQuality(accepted_values=["a", "a"])
    with pytest.raises(ValidationError, match="int64 bounds"):
        Feature(name="count", dtype="int64", quality={"minimum": 0.5})
    with pytest.raises(ValidationError, match="int64 or float64"):
        Feature(name="country", dtype="string", quality={"minimum": 0})
    with pytest.raises(ValidationError, match="match the feature dtype"):
        Feature(name="count", dtype="int64", quality={"accepted_values": ["one"]})


def test_absent_quality_fields_preserve_registry_spec_and_fingerprint(
    manifest: RegistryManifest,
) -> None:
    view = FeatureView(
        name="account_stats",
        version="1.0.0",
        entity="account",
        features=[Feature(name="amount", dtype="float64")],
        batch_source="transactions",
        batch_sql="SELECT * FROM source",
    )
    assert "quality_policy" not in view.registry_spec()
    assert "quality" not in view.registry_spec()["features"][0]
    assert manifest.fingerprint() == (
        "2681b2ab71302a2705c482b46724ed8b122cbbb9c8c70818f76b63a5fea36407"
    )


def test_table_validation_has_inclusive_boundaries_and_stable_order() -> None:
    cutoff = datetime(2025, 1, 2, tzinfo=UTC)
    features = [
        Feature(
            name="amount",
            dtype=ValueType.FLOAT64,
            quality=FeatureQuality(
                nullable=False,
                minimum=0,
                maximum=10,
                accepted_values=[0, 5, 10],
                unique=True,
                max_age_seconds=60,
            ),
        )
    ]
    table = pa.Table.from_pylist(
        [
            {"event_timestamp": cutoff - timedelta(seconds=60), "amount": 0.0},
            {"event_timestamp": cutoff - timedelta(seconds=61), "amount": 12.0},
            {"event_timestamp": cutoff, "amount": 0.0},
            {"event_timestamp": cutoff, "amount": None},
        ]
    )
    result = validate_quality_table(table, features, reference_time=cutoff)
    assert [(item.row_index, item.constraint.value) for item in result.violations] == [
        (0, "unique"),
        (1, "maximum"),
        (1, "accepted_values"),
        (1, "max_age_seconds"),
        (2, "unique"),
        (3, "nullable"),
    ]
    assert result.invalid_row_indexes == [0, 1, 2, 3]


def test_stream_event_is_trivially_unique_and_freshness_boundary_is_inclusive() -> None:
    now = datetime(2025, 1, 2, tzinfo=UTC)
    feature = Feature(
        name="category",
        dtype="string",
        quality={"unique": True, "max_age_seconds": 10, "accepted_values": ["ok"]},
    )
    valid = validate_quality_event(
        {"category": "ok"},
        [feature],
        event_timestamp=now - timedelta(seconds=10),
        ingestion_time=now,
    )
    assert valid.violations == []
