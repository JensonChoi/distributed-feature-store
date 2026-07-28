from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from feature_store.db import JobRecord, StreamEventRecord
from feature_store.ledger import StreamEventLedger
from feature_store.models import JobKind, RegistryManifest, StreamEventState, StreamFeatureEvent
from feature_store.registry import Registry
from feature_store.streaming import StreamConsumer


class FakeMessage:
    def __init__(
        self, topic: str, partition: int, offset: int, payload: dict[str, Any] | None = None
    ):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._value = json.dumps(payload).encode() if payload is not None else b"{}"

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def value(self) -> bytes:
        return self._value

    def key(self) -> None:
        return None

    def error(self) -> None:
        return None


class FakeConsumer:
    def __init__(self) -> None:
        self.committed: list[FakeMessage] = []

    def commit(self, *, message: FakeMessage, asynchronous: bool) -> None:
        assert not asynchronous
        self.committed.append(message)


class FakeProducer:
    def __init__(self) -> None:
        self.produced: list[tuple[str, bytes, dict[str, str]]] = []

    def produce(
        self, topic: str, *, key: None, value: bytes, headers: dict[str, str]
    ) -> None:
        self.produced.append((topic, value, headers))

    def flush(self, timeout: int) -> None:
        assert timeout == 10


class FakeOfflineStore:
    def __init__(self) -> None:
        self.tables: dict[str, pa.Table] = {}

    def append(self, uri: str, table: pa.Table, partition_by: str | None = None) -> None:
        assert partition_by == "event_date"
        self.tables[uri] = table


class FakeOnlineStore:
    def __init__(self) -> None:
        self.events: list[StreamFeatureEvent] = []

    def upsert(self, event: StreamFeatureEvent) -> bool:
        self.events.append(event)
        return True


def payload(
    *,
    event_id: str = "event-1",
    account_id: str = "a",
    amount: float = 12.5,
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "feature_view": "account_stats@1.0.0",
        "entity_values": {"account_id": account_id},
        "event_timestamp": datetime(2025, 1, 1, tzinfo=UTC).isoformat(),
        "values": {"amount": amount},
    }


def make_stream(
    session: Session, manifest: RegistryManifest
) -> tuple[StreamConsumer, FakeConsumer, FakeProducer, FakeOfflineStore, FakeOnlineStore]:
    Registry(session).apply(manifest)
    factory = sessionmaker(session.bind, expire_on_commit=False)
    consumer = FakeConsumer()
    producer = FakeProducer()
    offline = FakeOfflineStore()
    online = FakeOnlineStore()
    stream = StreamConsumer(
        consumer,  # type: ignore[arg-type]
        producer,  # type: ignore[arg-type]
        offline=offline,  # type: ignore[arg-type]
        online=online,  # type: ignore[arg-type]
        session_factory=factory,
    )
    return stream, consumer, producer, offline, online


def test_stream_commit_uses_latest_offset_per_partition() -> None:
    consumer = FakeConsumer()
    stream = StreamConsumer.__new__(StreamConsumer)
    stream.consumer = consumer  # type: ignore[assignment]
    messages: list[Any] = [
        FakeMessage("features", 0, 1),
        FakeMessage("features", 1, 4),
        FakeMessage("features", 0, 3),
    ]
    stream._commit_latest(messages)
    assert [(item.partition(), item.offset()) for item in consumer.committed] == [(0, 3), (1, 4)]


def test_pending_event_is_recovered_into_redis_and_staging(
    session: Session, manifest: RegistryManifest
) -> None:
    stream, consumer, _, offline, online = make_stream(session, manifest)
    with stream.session_factory() as ledger_session:
        registration = StreamEventLedger(ledger_session).register(
            StreamFeatureEvent.model_validate(payload()),
            source_topic="account-features",
            source_partition=0,
            source_offset=1,
        )

    assert stream.recover_pending() == 1
    assert [event.event_id for event in online.events] == ["event-1"]
    assert sum(table.num_rows for table in offline.tables.values()) == 1
    assert consumer.committed == []
    with stream.session_factory() as check:
        record = check.get(StreamEventRecord, registration.record.id)
        assert record is not None and record.state == StreamEventState.STAGED
        job = check.scalar(select(JobRecord).where(JobRecord.id == record.job_id))
        assert job is not None and job.kind == JobKind.OFFLINE_APPEND


def test_duplicate_messages_stage_one_row_and_commit_latest_offset(
    session: Session, manifest: RegistryManifest
) -> None:
    stream, consumer, _, offline, online = make_stream(session, manifest)
    stream._handle(FakeMessage("account-features", 0, 1, payload()))  # type: ignore[arg-type]
    stream._handle(FakeMessage("account-features", 0, 2, payload()))  # type: ignore[arg-type]
    stream.flush()

    assert sum(table.num_rows for table in offline.tables.values()) == 1
    assert [message.offset() for message in consumer.committed] == [2]
    assert len(online.events) == 1


def test_staged_replay_skips_mutations_and_commits_safely(
    session: Session, manifest: RegistryManifest
) -> None:
    stream, consumer, _, offline, online = make_stream(session, manifest)
    stream._handle(FakeMessage("account-features", 0, 1, payload()))  # type: ignore[arg-type]
    stream.flush()
    stream._handle(FakeMessage("account-features", 0, 2, payload()))  # type: ignore[arg-type]

    assert sum(table.num_rows for table in offline.tables.values()) == 1
    assert [message.offset() for message in consumer.committed] == [1, 2]
    assert len(online.events) == 1


def test_conflicting_duplicate_is_dead_lettered_after_earlier_message_is_flushed(
    session: Session, manifest: RegistryManifest
) -> None:
    stream, consumer, producer, offline, online = make_stream(session, manifest)
    stream._handle(FakeMessage("account-features", 0, 1, payload()))  # type: ignore[arg-type]
    stream._handle(
        FakeMessage("account-features", 0, 2, payload(account_id="different"))  # type: ignore[arg-type]
    )

    assert sum(table.num_rows for table in offline.tables.values()) == 1
    assert len(online.events) == 1
    assert [message.offset() for message in consumer.committed] == [1, 2]
    assert producer.produced[0][0] == "account-features.dlq"
    assert b"different" in producer.produced[0][1]
