from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from feature_store.models import (
    Feature,
    FeatureService,
    FeatureView,
    HistoricalQuery,
    Observation,
    ValueType,
    validate_feature_value,
)


def test_feature_view_requires_semantic_version() -> None:
    with pytest.raises(ValidationError, match="semantic"):
        FeatureView(
            name="account_stats",
            version="v1",
            entity="account",
            features=[Feature(name="amount", dtype=ValueType.FLOAT64)],
            batch_source="transactions",
            batch_sql="SELECT * FROM source",
        )


def test_feature_service_requires_pinned_reference() -> None:
    with pytest.raises(ValidationError, match="view@x.y.z"):
        FeatureService(name="fraud", features=["account_stats:amount"])


def test_historical_query_requires_one_selector() -> None:
    observation = Observation(entity_values={"account_id": "a"}, event_timestamp=datetime.now(UTC))
    with pytest.raises(ValidationError, match="exactly one"):
        HistoricalQuery(observations=[observation])
    with pytest.raises(ValidationError, match="exactly one"):
        HistoricalQuery(
            observations=[observation],
            features=["account_stats@1.0.0:amount"],
            feature_service="fraud",
        )


def test_stream_feature_values_are_strictly_typed() -> None:
    validate_feature_value(ValueType.INT64, 3)
    validate_feature_value(ValueType.FLOAT64, 3)
    validate_feature_value(ValueType.TIMESTAMP, "2025-01-01T00:00:00+00:00")
    with pytest.raises(ValueError, match="int64"):
        validate_feature_value(ValueType.INT64, True)
    with pytest.raises(ValueError, match="timestamp"):
        validate_feature_value(ValueType.TIMESTAMP, "2025-01-01")


def test_observation_requires_timezone() -> None:
    with pytest.raises(ValidationError, match="timezone"):
        Observation(
            entity_values={"account_id": "a"},
            event_timestamp=datetime(2025, 1, 1),
        )
