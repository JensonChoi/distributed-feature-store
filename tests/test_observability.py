from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
from prometheus_client import CollectorRegistry
from sqlalchemy.orm import Session

from feature_store.db import JobRecord, MaterializationState, StreamEventRecord
from feature_store.models import (
    JobKind,
    JobStatus,
    Observation,
    RegistryManifest,
    StreamEventState,
    StreamFeatureEvent,
)
from feature_store.observability import Metrics, update_operational_gauges
from feature_store.offline import OfflineStore
from feature_store.online import OnlineStore
from feature_store.pit import HistoricalRetriever
from feature_store.registry import Registry


class MemoryRedis:
    def __init__(self) -> None:
        self.data: dict[str, dict[str, str]] = {}

    def eval(self, _: str, __: int, key: str, *args: str) -> int:
        stored = self.data.setdefault(key, {})
        if stored.get("__event_timestamp", "") >= args[0]:
            return 0
        stored.update({"__event_timestamp": args[0], "__event_id": args[1]})
        for index in range(2, len(args), 2):
            stored[args[index]] = args[index + 1]
        return 1

    def hgetall(self, key: str) -> dict[str, str]:
        return self.data.get(key, {})


class FailingRedis(MemoryRedis):
    def eval(self, _: str, __: int, key: str, *args: str) -> int:
        raise ConnectionError("redis unavailable")


class LocalOfflineStore(OfflineStore):
    def __init__(self, root: Path):
        super().__init__()
        self.root = root

    def view_uri(self, view_ref: str) -> str:
        return str(self.root / view_ref.replace("@", "_"))


def sample(metrics_registry: CollectorRegistry, name: str, labels: dict[str, str]) -> float:
    value = metrics_registry.get_sample_value(name, labels)
    assert value is not None
    return value


def test_online_metrics_capture_results_age_and_update_outcomes(
    session: Session, manifest: RegistryManifest
) -> None:
    registry = Registry(session)
    registry.apply(manifest)
    collector = CollectorRegistry()
    metrics = Metrics(collector)
    store = OnlineStore(client=MemoryRedis(), metrics=metrics)  # type: ignore[arg-type]
    event = StreamFeatureEvent(
        event_id="event-1",
        feature_view="account_stats@1.0.0",
        entity_values={"account_id": "a"},
        event_timestamp=datetime.now(UTC) - timedelta(seconds=30),
        values={"amount": 12.5},
    )

    assert store.upsert(event)
    assert not store.upsert(event)
    store.read(
        registry,
        [{"account_id": "a"}, {"account_id": "missing"}],
        ["account_stats@1.0.0:amount"],
    )

    view = "account_stats@1.0.0"
    assert sample(collector, "feature_store_online_updates_total", {
        "feature_view": view, "outcome": "accepted"
    }) == 1
    assert sample(collector, "feature_store_online_updates_total", {
        "feature_view": view, "outcome": "skipped"
    }) == 1
    assert sample(collector, "feature_store_online_entity_results_total", {
        "feature_view": view, "result": "present"
    }) == 1
    assert sample(collector, "feature_store_online_entity_results_total", {
        "feature_view": view, "result": "missing"
    }) == 1
    assert sample(collector, "feature_store_online_served_value_age_seconds_count", {
        "feature_view": view
    }) == 1


def test_historical_metrics_do_not_expose_internal_age_column(
    tmp_path: Path, session: Session, manifest: RegistryManifest
) -> None:
    registry = Registry(session)
    registry.apply(manifest)
    collector = CollectorRegistry()
    offline = LocalOfflineStore(tmp_path)
    observed_at = datetime(2025, 1, 1, 12, tzinfo=UTC)
    offline.append(
        offline.view_uri("account_stats@1.0.0"),
        pa.Table.from_pylist(
            [{
                "account_id": "a",
                "event_timestamp": observed_at - timedelta(minutes=5),
                "event_id": "event-1",
                "amount": 10.0,
                "event_date": "2025-01-01",
            }]
        ),
        "event_date",
    )
    result = HistoricalRetriever(registry, offline, Metrics(collector)).query(
        [Observation(entity_values={"account_id": "a"}, event_timestamp=observed_at)],
        ["account_stats@1.0.0:amount"],
    )

    assert all(not key.startswith("__feature_store_age") for key in result.rows[0])
    labels = {"feature_view": "account_stats@1.0.0"}
    assert sample(
        collector, "feature_store_historical_served_value_age_seconds_sum", labels
    ) == 300
    assert sample(collector, "feature_store_historical_queries_total", {
        "mode": "inline", "outcome": "success"
    }) == 1


def test_online_upsert_error_has_bounded_outcome_metric() -> None:
    collector = CollectorRegistry()
    store = OnlineStore(client=FailingRedis(), metrics=Metrics(collector))  # type: ignore[arg-type]
    event = StreamFeatureEvent(
        event_id="private-event-id",
        feature_view="account_stats@1.0.0",
        entity_values={"account_id": "private-entity-id"},
        event_timestamp=datetime.now(UTC),
        values={"amount": 12.5},
    )

    try:
        store.upsert(event)
    except ConnectionError:
        pass
    else:
        raise AssertionError("Redis error should propagate")

    assert sample(
        collector,
        "feature_store_online_requests_total",
        {"operation": "upsert", "outcome": "error"},
    ) == 1
    rendered = "".join(family.name for family in collector.collect())
    assert "private-event-id" not in rendered
    assert "private-entity-id" not in rendered


def test_database_backlog_and_freshness_gauges(
    session: Session, manifest: RegistryManifest
) -> None:
    now = datetime(2025, 1, 1, 12, tzinfo=UTC)
    session.add_all(
        [
            JobRecord(
                kind=JobKind.BACKFILL,
                status=JobStatus.PENDING,
                payload={},
                created_at=now - timedelta(minutes=10),
            ),
            StreamEventRecord(
                feature_view="account_stats@1.0.0",
                event_id="event-1",
                fingerprint="fingerprint",
                payload={},
                state=StreamEventState.STAGED,
                created_at=now - timedelta(minutes=5),
            ),
            MaterializationState(
                feature_view="account_stats@1.0.0",
                watermark=now - timedelta(minutes=2),
                source_freshness_at=now - timedelta(minutes=3),
            ),
        ]
    )
    session.commit()
    collector = CollectorRegistry()
    update_operational_gauges(session, Metrics(collector), now=now)

    assert sample(collector, "feature_store_job_queue_depth", {"status": "pending"}) == 1
    assert sample(
        collector, "feature_store_job_queue_oldest_age_seconds", {"status": "pending"}
    ) == 600
    assert sample(collector, "feature_store_stream_ledger_depth", {"state": "staged"}) == 1
    assert sample(collector, "feature_store_materialization_watermark_age_seconds", {
        "feature_view": "account_stats@1.0.0"
    }) == 120
