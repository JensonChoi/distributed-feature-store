from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
from sqlalchemy.orm import Session

from feature_store.db import StreamEventRecord
from feature_store.jobs import JobExecutor, JobService
from feature_store.ledger import StreamEventLedger
from feature_store.models import (
    JobKind,
    JobRequest,
    JobStatus,
    RegistryManifest,
    StreamEventState,
    StreamFeatureEvent,
)
from feature_store.offline import OfflineStore
from feature_store.registry import Registry


class LocalOfflineStore(OfflineStore):
    def __init__(self, root: Path):
        super().__init__()
        self.root = root

    def view_uri(self, view_ref: str) -> str:
        return str(self.root / "views" / view_ref.replace("@", "_"))


class NoCleanupOfflineStore(LocalOfflineStore):
    def delete(self, uri: str) -> bool:
        return False


def stream_table(*, event_id: str = "event-1", amount: float = 12.5) -> pa.Table:
    timestamp = datetime(2025, 1, 1, tzinfo=UTC)
    return pa.Table.from_pylist(
        [
            {
                "account_id": "a",
                "event_timestamp": timestamp,
                "event_id": event_id,
                "amount": amount,
                "event_date": timestamp.date().isoformat(),
            }
        ]
    )


def test_backfill_job_writes_versioned_offline_table(
    tmp_path: Path, session: Session, manifest: RegistryManifest
) -> None:
    offline = LocalOfflineStore(tmp_path)
    source_uri = str(tmp_path / "source")
    changed = manifest.model_copy(deep=True)
    changed.batch_sources[0].uri = source_uri
    Registry(session).apply(changed)
    start = datetime(2025, 1, 1, tzinfo=UTC)
    offline.append(
        source_uri,
        pa.Table.from_pylist(
            [
                {
                    "account_id": "a",
                    "event_timestamp": start + timedelta(minutes=1),
                    "event_id": "event-1",
                    "amount": 12.5,
                }
            ]
        ),
    )
    job = JobService(session).create(
        JobKind.BACKFILL,
        JobRequest(
            feature_view="account_stats@1.0.0",
            start=start,
            end=start + timedelta(days=1),
        ),
    )
    executor = JobExecutor(session, offline=offline)
    executor.execute(job)
    assert job.status == JobStatus.SUCCEEDED
    result = offline.load(offline.view_uri("account_stats@1.0.0"))
    assert result.num_rows == 1
    assert result["amount"].to_pylist() == [12.5]
    assert len(job.checkpoints) == 1

    partial = JobService(session).create(
        JobKind.BACKFILL,
        JobRequest(
            feature_view="account_stats@1.0.0",
            start=start + timedelta(hours=12),
            end=start + timedelta(hours=13),
        ),
    )
    executor.execute(partial)
    preserved = offline.load(offline.view_uri("account_stats@1.0.0"))
    assert partial.status == JobStatus.SUCCEEDED, partial.error
    assert preserved["amount"].to_pylist() == [12.5]


def test_interrupted_jobs_are_recovered_and_failed_jobs_can_retry(
    session: Session, manifest: RegistryManifest
) -> None:
    Registry(session).apply(manifest)
    service = JobService(session)
    start = datetime(2025, 1, 1, tzinfo=UTC)
    running = service.create(
        JobKind.BACKFILL,
        JobRequest(
            feature_view="account_stats@1.0.0",
            start=start,
            end=start + timedelta(days=1),
        ),
    )
    running.status = JobStatus.RUNNING
    session.commit()
    assert JobExecutor(session).recover_interrupted() == 1
    assert running.status == JobStatus.PENDING

    running.status = JobStatus.FAILED
    running.error = "transient failure"
    session.commit()
    retried = service.retry(running.id)
    assert retried.status == JobStatus.PENDING
    assert retried.error is None


def test_offline_append_is_idempotent_across_jobs_and_legacy_jobs(
    tmp_path: Path, session: Session
) -> None:
    offline = LocalOfflineStore(tmp_path)
    service = JobService(session)
    for index in (1, 2):
        staging = str(tmp_path / f"staging-{index}")
        offline.append(staging, stream_table(), partition_by="event_date")
        job = service.create_offline_append("account_stats@1.0.0", staging)
        JobExecutor(session, offline=offline).execute(job)
        assert job.status == JobStatus.SUCCEEDED, job.error

    result = offline.load(offline.view_uri("account_stats@1.0.0"))
    assert result.num_rows == 1


def test_offline_append_retry_after_delta_write_does_not_duplicate(
    tmp_path: Path, session: Session
) -> None:
    offline = NoCleanupOfflineStore(tmp_path)
    staging = str(tmp_path / "staging")
    offline.append(staging, stream_table(), partition_by="event_date")
    job = JobService(session).create_offline_append("account_stats@1.0.0", staging)
    executor = JobExecutor(session, offline=offline)
    executor.execute(job)
    job.status = JobStatus.PENDING
    session.commit()
    executor.execute(job)

    assert job.status == JobStatus.SUCCEEDED, job.error
    assert offline.load(offline.view_uri("account_stats@1.0.0")).num_rows == 1
    assert "duplicates:1" in job.checkpoints


def test_offline_append_rejects_conflicting_existing_event(
    tmp_path: Path, session: Session
) -> None:
    offline = LocalOfflineStore(tmp_path)
    target = offline.view_uri("account_stats@1.0.0")
    offline.append(target, stream_table(amount=1.0), partition_by="event_date")
    staging = str(tmp_path / "staging")
    offline.append(staging, stream_table(amount=2.0), partition_by="event_date")
    job = JobService(session).create_offline_append("account_stats@1.0.0", staging)
    JobExecutor(session, offline=offline).execute(job)

    assert job.status == JobStatus.FAILED
    assert job.error is not None and "conflicts with existing offline content" in job.error
    assert offline.load(target)["amount"].to_pylist() == [1.0]


def test_offline_append_marks_ledger_applied_with_job_success(
    tmp_path: Path, session: Session
) -> None:
    offline = LocalOfflineStore(tmp_path)
    staging = str(tmp_path / "staging")
    offline.append(staging, stream_table(), partition_by="event_date")
    event = StreamFeatureEvent(
        event_id="event-1",
        feature_view="account_stats@1.0.0",
        entity_values={"account_id": "a"},
        event_timestamp=datetime(2025, 1, 1, tzinfo=UTC),
        values={"amount": 12.5},
    )
    registration = StreamEventLedger(session).register(
        event, source_topic="features", source_partition=0, source_offset=1
    )
    job = JobService(session).create_offline_append(
        "account_stats@1.0.0", staging, commit=False
    )
    StreamEventLedger(session).mark_staged([registration.record], job.id)
    session.commit()

    JobExecutor(session, offline=offline).execute(job)
    session.refresh(registration.record)
    assert job.status == JobStatus.SUCCEEDED
    assert registration.record.state == StreamEventState.APPLIED
    assert registration.record.applied_at is not None
    assert session.get(StreamEventRecord, registration.record.id) is not None


class CapturingOnlineStore:
    def __init__(self) -> None:
        self.events: list[StreamFeatureEvent] = []

    def upsert(self, event: StreamFeatureEvent) -> bool:
        self.events.append(event)
        return True


def test_materialization_uses_larger_event_id_at_equal_timestamp(
    tmp_path: Path, session: Session, manifest: RegistryManifest
) -> None:
    offline = LocalOfflineStore(tmp_path)
    Registry(session).apply(manifest)
    timestamp = datetime(2025, 1, 1, tzinfo=UTC)
    rows = pa.concat_tables(
        [stream_table(event_id="a", amount=1.0), stream_table(event_id="b", amount=2.0)]
    )
    offline.append(
        offline.view_uri("account_stats@1.0.0"), rows, partition_by="event_date"
    )
    job = JobService(session).create(
        JobKind.MATERIALIZE,
        JobRequest(
            feature_view="account_stats@1.0.0",
            start=timestamp - timedelta(seconds=1),
            end=timestamp + timedelta(seconds=1),
        ),
    )
    online = CapturingOnlineStore()
    JobExecutor(session, offline=offline, online=online).execute(job)  # type: ignore[arg-type]

    assert job.status == JobStatus.SUCCEEDED, job.error
    assert [(event.event_id, event.values["amount"]) for event in online.events] == [("b", 2.0)]
