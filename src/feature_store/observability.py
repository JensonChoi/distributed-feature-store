from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from feature_store.db import JobRecord, MaterializationState, StreamEventRecord
from feature_store.models import JobStatus, StreamEventState


class Metrics:
    """Prometheus instruments shared by API, worker, and stream processes.

    Every label is deliberately selected from an operator-controlled or enumerated set. Runtime
    identifiers, paths, offsets, and exception text must never be added here.
    """

    def __init__(self, registry: CollectorRegistry = REGISTRY):
        self.http_requests = Counter(
            "feature_store_http_requests_total",
            "HTTP requests.",
            ["method", "path", "status"],
            registry=registry,
        )
        self.http_duration = Histogram(
            "feature_store_http_request_seconds",
            "HTTP request duration.",
            ["path"],
            registry=registry,
        )
        self.online_requests = Counter(
            "feature_store_online_requests_total",
            "Online store operations.",
            ["operation", "outcome"],
            registry=registry,
        )
        self.online_duration = Histogram(
            "feature_store_online_request_duration_seconds",
            "Online store operation duration.",
            ["operation"],
            registry=registry,
        )
        self.online_updates = Counter(
            "feature_store_online_updates_total",
            "Online updates accepted or skipped.",
            ["feature_view", "outcome"],
            registry=registry,
        )
        self.online_entity_results = Counter(
            "feature_store_online_entity_results_total",
            "Online entity results by presence.",
            ["feature_view", "result"],
            registry=registry,
        )
        self.online_served_age = Histogram(
            "feature_store_online_served_value_age_seconds",
            "Age of values returned by online serving.",
            ["feature_view"],
            registry=registry,
        )
        self.historical_queries = Counter(
            "feature_store_historical_queries_total",
            "Historical retrieval operations.",
            ["mode", "outcome"],
            registry=registry,
        )
        self.historical_duration = Histogram(
            "feature_store_historical_query_duration_seconds",
            "Historical retrieval duration.",
            ["mode", "outcome"],
            registry=registry,
        )
        self.historical_observations = Counter(
            "feature_store_historical_observations_total",
            "Observations processed by historical retrieval.",
            ["mode"],
            registry=registry,
        )
        self.historical_entity_results = Counter(
            "feature_store_historical_entity_results_total",
            "Historical entity results by presence and TTL state.",
            ["feature_view", "result"],
            registry=registry,
        )
        self.historical_served_age = Histogram(
            "feature_store_historical_served_value_age_seconds",
            "Point-in-time age of values returned by historical retrieval.",
            ["feature_view"],
            registry=registry,
        )
        self.job_claimed = Counter(
            "feature_store_job_attempts_claimed_total",
            "Job attempts claimed by workers.",
            ["kind"],
            registry=registry,
        )
        self.job_completed = Counter(
            "feature_store_job_attempts_completed_total",
            "Job attempts completed by terminal attempt outcome.",
            ["kind", "outcome"],
            registry=registry,
        )
        self.job_duration = Histogram(
            "feature_store_job_execution_duration_seconds",
            "Job attempt execution duration.",
            ["kind", "outcome"],
            registry=registry,
        )
        self.job_queue_depth = Gauge(
            "feature_store_job_queue_depth",
            "Jobs in each active queue state.",
            ["status"],
            registry=registry,
        )
        self.job_oldest_age = Gauge(
            "feature_store_job_queue_oldest_age_seconds",
            "Age of the oldest job in each active queue state.",
            ["status"],
            registry=registry,
        )
        self.materialization_watermark_age = Gauge(
            "feature_store_materialization_watermark_age_seconds",
            "Age of each materialization watermark.",
            ["feature_view"],
            registry=registry,
        )
        self.materialization_source_age = Gauge(
            "feature_store_materialization_source_freshness_age_seconds",
            "Age of the freshest source event seen by materialization.",
            ["feature_view"],
            registry=registry,
        )
        self.stream_ledger_depth = Gauge(
            "feature_store_stream_ledger_depth",
            "Stream ledger records awaiting application.",
            ["state"],
            registry=registry,
        )
        self.stream_ledger_oldest_age = Gauge(
            "feature_store_stream_ledger_oldest_record_age_seconds",
            "Age of the oldest stream ledger record by state.",
            ["state"],
            registry=registry,
        )
        self.stream_events = Counter(
            "feature_store_stream_events_total",
            "Stream events by bounded processing result.",
            ["result"],
            registry=registry,
        )
        self.stream_processing_duration = Histogram(
            "feature_store_stream_processing_duration_seconds",
            "Stream message processing duration.",
            ["outcome"],
            registry=registry,
        )
        self.stream_flush_duration = Histogram(
            "feature_store_stream_flush_duration_seconds",
            "Stream batch flush duration.",
            ["outcome"],
            registry=registry,
        )
        self.stream_staged_events = Counter(
            "feature_store_stream_staged_events_total",
            "Events durably staged for offline append.",
            ["feature_view"],
            registry=registry,
        )
        self.stream_online_writes = Counter(
            "feature_store_stream_online_writes_total",
            "Online writes attempted by the stream consumer.",
            ["feature_view", "outcome"],
            registry=registry,
        )
        self.stream_ingestion_lag = Histogram(
            "feature_store_stream_event_ingestion_lag_seconds",
            "Wall-clock lag from event time to stream consumption.",
            ["feature_view"],
            registry=registry,
        )
        self.stream_dead_letters = Counter(
            "feature_store_stream_dead_letters_total",
            "Dead-lettered messages by fixed reason category.",
            ["reason"],
            registry=registry,
        )


METRICS = Metrics()


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def update_operational_gauges(
    session: Session, metrics: Metrics = METRICS, *, now: datetime | None = None
) -> None:
    """Refresh database-backed gauges; intended for each worker/consumer polling cycle."""
    observed_at = now or datetime.now(UTC)
    for status in (JobStatus.PENDING, JobStatus.RETRYING, JobStatus.RUNNING):
        count, oldest = session.execute(
            select(func.count(JobRecord.id), func.min(JobRecord.created_at)).where(
                JobRecord.status == status
            )
        ).one()
        metrics.job_queue_depth.labels(status).set(count)
        age = max(0.0, (observed_at - _utc(oldest)).total_seconds()) if oldest else 0.0
        metrics.job_oldest_age.labels(status).set(age)

    for ledger_state in (StreamEventState.PENDING, StreamEventState.STAGED):
        count, oldest = session.execute(
            select(func.count(StreamEventRecord.id), func.min(StreamEventRecord.created_at)).where(
                StreamEventRecord.state == ledger_state
            )
        ).one()
        metrics.stream_ledger_depth.labels(ledger_state).set(count)
        age = max(0.0, (observed_at - _utc(oldest)).total_seconds()) if oldest else 0.0
        metrics.stream_ledger_oldest_age.labels(ledger_state).set(age)

    for materialization_state in session.scalars(select(MaterializationState)):
        if materialization_state.watermark is not None:
            metrics.materialization_watermark_age.labels(materialization_state.feature_view).set(
                max(0.0, (observed_at - _utc(materialization_state.watermark)).total_seconds())
            )
        if materialization_state.source_freshness_at is not None:
            metrics.materialization_source_age.labels(materialization_state.feature_view).set(
                max(
                    0.0,
                    (observed_at - _utc(materialization_state.source_freshness_at)).total_seconds(),
                )
            )


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in ("request_id", "job_id", "job_kind", "job_status"):
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"), default=str)


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
