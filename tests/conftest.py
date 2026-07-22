from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy.orm import Session, sessionmaker

from feature_store.db import Base, make_engine
from feature_store.models import (
    BatchSource,
    Entity,
    Feature,
    FeatureService,
    FeatureView,
    RegistryManifest,
    StreamSource,
    ValueType,
)


@pytest.fixture
def session() -> Iterator[Session]:
    engine = make_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    maker = sessionmaker(engine, expire_on_commit=False)
    with maker() as value:
        yield value
    engine.dispose()


@pytest.fixture
def manifest() -> RegistryManifest:
    return RegistryManifest(
        entities=[Entity(name="account", join_keys={"account_id": ValueType.STRING})],
        batch_sources=[BatchSource(name="transactions", uri="file:///tmp/source")],
        stream_sources=[StreamSource(name="account_stream", topic="account-features")],
        feature_views=[
            FeatureView(
                name="account_stats",
                version="1.0.0",
                entity="account",
                features=[Feature(name="amount", dtype=ValueType.FLOAT64)],
                batch_source="transactions",
                batch_sql=("SELECT account_id, event_timestamp, event_id, amount FROM source"),
                stream_source="account_stream",
                ttl_seconds=3600,
            )
        ],
        feature_services=[FeatureService(name="fraud_v1", features=["account_stats@1.0.0:amount"])],
    )
