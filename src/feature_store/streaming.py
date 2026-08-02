from __future__ import annotations

import json
import logging
import time
import uuid
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
from confluent_kafka import Consumer, KafkaError, Message, Producer
from prometheus_client import start_http_server
from pydantic import ValidationError
from sqlalchemy.orm import Session

from feature_store.config import get_settings
from feature_store.db import SessionLocal, StreamEventRecord, init_db
from feature_store.jobs import JobService
from feature_store.ledger import RegistrationResult, StreamEventLedger
from feature_store.models import StreamEventState, StreamFeatureEvent, validate_feature_value
from feature_store.observability import (
    METRICS,
    Metrics,
    configure_logging,
    update_operational_gauges,
)
from feature_store.offline import OfflineStore
from feature_store.online import OnlineStore
from feature_store.registry import Registry

logger = logging.getLogger(__name__)


@dataclass
class BufferedEvent:
    record_id: str
    event: StreamFeatureEvent
    messages: list[Message] = field(default_factory=list)


class StreamConsumer:
    def __init__(
        self,
        consumer: Consumer,
        producer: Producer,
        offline: OfflineStore | None = None,
        online: OnlineStore | None = None,
        session_factory: Callable[[], Session] = SessionLocal,
        metrics: Metrics = METRICS,
    ):
        self.consumer = consumer
        self.producer = producer
        self.offline = offline or OfflineStore()
        self.online = online or OnlineStore()
        self.session_factory = session_factory
        self.settings = get_settings()
        self.metrics = metrics
        self.buffers: dict[str, dict[str, BufferedEvent]] = defaultdict(dict)
        self.last_flush = time.monotonic()

    def subscribe(self) -> None:
        with self.session_factory() as session:
            records = Registry(session).list_records("stream_source")
            topics = sorted({record["spec"]["topic"] for record in records})
        if not topics:
            raise RuntimeError("no stream sources registered")
        self.consumer.subscribe(topics)
        logger.info("subscribed to %s", topics)

    def loop(self) -> None:
        self.recover_pending()
        self.subscribe()
        try:
            while True:
                message = self.consumer.poll(0.5)
                if message is not None:
                    self._handle(message)
                if self._should_flush():
                    self.flush()
        finally:
            self.flush()
            self.consumer.close()

    def _handle(self, message: Message) -> None:
        started = time.perf_counter()
        outcome = "processing_failure"
        error = message.error()
        if error:
            if error.code() != KafkaError._PARTITION_EOF:
                logger.error("consumer error: %s", error)
            self.metrics.stream_processing_duration.labels("consumer_error").observe(
                time.perf_counter() - started
            )
            return
        try:
            raw_value = message.value()
            if raw_value is None:
                raise ValueError("message value is empty")
            payload = json.loads(raw_value)
            event = StreamFeatureEvent.model_validate(payload)
            if event.event_timestamp.tzinfo is None:
                raise ValueError("event_timestamp must include a timezone")
            ingestion_lag = (
                datetime.now(UTC) - event.event_timestamp.astimezone(UTC)
            ).total_seconds()
            self.metrics.stream_ingestion_lag.labels(event.feature_view).observe(
                max(0.0, ingestion_lag)
            )
            with self.session_factory() as session:
                registry = Registry(session)
                view = registry.feature_view(event.feature_view)
                if not view.stream_source:
                    raise ValueError(f"{view.ref} has no stream source")
                stream = registry.stream_source(view.stream_source)
                if stream.topic != message.topic():
                    raise ValueError(f"{view.ref} is not configured for topic {message.topic()}")
                entity = registry.entity(view.entity)
                missing_keys = set(entity.join_keys) - set(event.entity_values)
                if missing_keys:
                    raise ValueError(f"missing entity keys: {sorted(missing_keys)}")
                expected = {feature.name for feature in view.features}
                if set(event.values) != expected:
                    raise ValueError(
                        f"feature values must exactly match schema; expected {sorted(expected)}"
                    )
                for feature in view.features:
                    validate_feature_value(feature.dtype, event.values[feature.name])
                registration = StreamEventLedger(session).register(
                    event,
                    source_topic=message.topic(),
                    source_partition=message.partition(),
                    source_offset=message.offset(),
                )
            if registration.result == RegistrationResult.CONFLICT:
                self._dead_letter(
                    message,
                    f"event identity {event.feature_view}:{event.event_id} "
                    "was reused with different content",
                )
                self.flush()
                self.consumer.commit(message=message, asynchronous=False)
                self.metrics.stream_events.labels("dead_lettered").inc()
                self.metrics.stream_dead_letters.labels("identity_conflict").inc()
                outcome = "dead_lettered"
                return
            if registration.record.state != StreamEventState.PENDING:
                self.flush()
                self.consumer.commit(message=message, asynchronous=False)
                self.metrics.stream_events.labels("duplicate").inc()
                outcome = "duplicate"
                return

            durable_event = StreamEventLedger.event(registration.record)
            if self._buffer(registration.record.id, durable_event, message):
                self._write_online(durable_event)
            result = (
                "accepted"
                if registration.result == RegistrationResult.NEW
                else "duplicate_pending"
            )
            self.metrics.stream_events.labels(result).inc()
            outcome = result
        except (ValueError, ValidationError, json.JSONDecodeError) as exc:
            self._dead_letter(message, str(exc))
            self.flush()
            self.consumer.commit(message=message, asynchronous=False)
            self.metrics.stream_events.labels("dead_lettered").inc()
            self.metrics.stream_dead_letters.labels("validation").inc()
            outcome = "dead_lettered"
        except Exception as exc:
            logger.exception("stream message processing failed")
            self._dead_letter(message, str(exc))
            self.flush()
            self.consumer.commit(message=message, asynchronous=False)
            self.metrics.stream_events.labels("dead_lettered").inc()
            self.metrics.stream_dead_letters.labels("processing_failure").inc()
            outcome = "dead_lettered"
        finally:
            self.metrics.stream_processing_duration.labels(outcome).observe(
                time.perf_counter() - started
            )

    def recover_pending(self) -> int:
        with self.session_factory() as session:
            records = StreamEventLedger(session).pending()
            for record in records:
                event = StreamEventLedger.event(record)
                if self._buffer(record.id, event):
                    self._write_online(event)
        if records:
            self.flush()
        return len(records)

    def _write_online(self, event: StreamFeatureEvent) -> bool:
        try:
            accepted = self.online.upsert(event)
        except Exception:
            self.metrics.stream_online_writes.labels(event.feature_view, "error").inc()
            raise
        outcome = "accepted" if accepted else "skipped"
        self.metrics.stream_online_writes.labels(event.feature_view, outcome).inc()
        return accepted

    def _buffer(
        self, record_id: str, event: StreamFeatureEvent, message: Message | None = None
    ) -> bool:
        buffered = self.buffers[event.feature_view].get(event.event_id)
        created = buffered is None
        if buffered is None:
            buffered = BufferedEvent(record_id=record_id, event=event)
            self.buffers[event.feature_view][event.event_id] = buffered
        if message is not None:
            buffered.messages.append(message)
        return created

    def _dead_letter(self, message: Message, error: str) -> None:
        topic = f"{message.topic()}.dlq"
        with self.session_factory() as session:
            for record in Registry(session).list_records("stream_source"):
                spec = record["spec"]
                if spec["topic"] == message.topic() and spec.get("dead_letter_topic"):
                    topic = spec["dead_letter_topic"]
                    break
        self.producer.produce(
            topic,
            key=message.key(),
            value=message.value(),
            headers={"feature-store-error": error[:500]},
        )
        self.producer.flush(10)

    def _should_flush(self) -> bool:
        count = sum(
            sum(max(1, len(item.messages)) for item in buffer.values())
            for buffer in self.buffers.values()
            if buffer
        )
        return count >= self.settings.stream_batch_size or (
            count > 0 and time.monotonic() - self.last_flush >= self.settings.stream_flush_seconds
        )

    def flush(self) -> None:
        started = time.perf_counter()
        try:
            staged_messages: list[Message] = []
            for view_ref, buffer in list(self.buffers.items()):
                if not buffer:
                    continue
                staged_messages.extend(self._flush_view(view_ref, list(buffer.values())))
                self.buffers[view_ref] = {}
            self._commit_latest(staged_messages)
            self.last_flush = time.monotonic()
        except Exception:
            outcome = "error"
            raise
        else:
            outcome = "success"
        finally:
            self.metrics.stream_flush_duration.labels(outcome).observe(
                time.perf_counter() - started
            )

    def _flush_view(self, view_ref: str, buffer: list[BufferedEvent]) -> list[Message]:
        with self.session_factory() as session:
            registry = Registry(session)
            view = registry.feature_view(view_ref)
            entity = registry.entity(view.entity)
            feature_names = [feature.name for feature in view.features]
            rows: list[dict[str, Any]] = []
            for item in buffer:
                event = item.event
                timestamp = event.event_timestamp.astimezone(UTC)
                rows.append(
                    {
                        **{key: event.entity_values[key] for key in entity.join_keys},
                        "event_timestamp": timestamp,
                        "event_id": event.event_id,
                        **{name: event.values[name] for name in feature_names},
                        "event_date": timestamp.date().isoformat(),
                    }
                )
            table = pa.Table.from_pylist(rows)
            staging_uri = (
                f"s3://{self.settings.offline_bucket}/staging/{view_ref.replace('@', '/')}/"
                f"{uuid.uuid4()}"
            )
            self.offline.append(staging_uri, table, partition_by="event_date")
            records = list(
                session.query(StreamEventRecord).filter(
                    StreamEventRecord.id.in_([item.record_id for item in buffer]),
                    StreamEventRecord.state == StreamEventState.PENDING,
                )
            )
            if len(records) != len(buffer):
                raise RuntimeError("stream event state changed before staging")
            job = JobService(session).create_offline_append(view_ref, staging_uri, commit=False)
            StreamEventLedger(session).mark_staged(records, job.id)
            session.commit()

        logger.info("staged %d events for %s", len(buffer), view_ref)
        self.metrics.stream_staged_events.labels(view_ref).inc(len(buffer))
        with self.session_factory() as metrics_session:
            update_operational_gauges(metrics_session, self.metrics)
        return [message for item in buffer for message in item.messages]

    def _commit_latest(self, messages: list[Message]) -> None:
        latest_by_partition: dict[tuple[str, int], Message] = {}
        for message in messages:
            topic = message.topic()
            partition = message.partition()
            offset = message.offset()
            if topic is None or partition is None or offset is None:
                raise ValueError("Kafka message is missing topic, partition, or offset")
            key = (topic, partition)
            previous = latest_by_partition.get(key)
            previous_offset = previous.offset() if previous is not None else None
            if previous is None or previous_offset is None or offset > previous_offset:
                latest_by_partition[key] = message
        for message in latest_by_partition.values():
            self.consumer.commit(message=message, asynchronous=False)


def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    init_db()
    start_http_server(settings.stream_metrics_port, addr=settings.metrics_host)
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": "feature-store-stream-consumer",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    StreamConsumer(consumer, producer).loop()


if __name__ == "__main__":
    run()
