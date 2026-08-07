from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest
from sqlalchemy.orm import Session

from feature_store.config import Settings
from feature_store.db import JobRecord, MaterializationState, StreamEventRecord
from feature_store.jobs import JobExecutor, JobService, LeaseLostError, serialize_job
from feature_store.ledger import StreamEventLedger
from feature_store.models import (
    FeatureQuality,
    IncrementalMaterializationRequest,
    JobKind,
    JobRequest,
    JobStatus,
    QualityPolicy,
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


def claim(executor: JobExecutor, expected_id: str) -> JobRecord:
    job = executor.claim_next()
    assert job is not None
    assert job.id == expected_id
    return job


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
    executor.execute(claim(executor, job.id))
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
    executor.execute(claim(executor, partial.id))
    preserved = offline.load(offline.view_uri("account_stats@1.0.0"))
    assert partial.status == JobStatus.SUCCEEDED, partial.error
    assert preserved["amount"].to_pylist() == [12.5]


@pytest.mark.parametrize(
    ("policy", "expected_status", "expected_rows"),
    [
        (QualityPolicy.REJECT, JobStatus.FAILED, None),
        (QualityPolicy.QUARANTINE, JobStatus.SUCCEEDED, [2.0]),
        (QualityPolicy.REPORT, JobStatus.SUCCEEDED, [-1.0, 2.0]),
    ],
)
def test_backfill_quality_policies(
    tmp_path: Path,
    session: Session,
    manifest: RegistryManifest,
    policy: QualityPolicy,
    expected_status: JobStatus,
    expected_rows: list[float] | None,
) -> None:
    class QualityOfflineStore(LocalOfflineStore):
        def quarantine_uri(self, view_ref: str, job_id: str) -> str:
            return str(self.root / "quarantine" / view_ref.replace("@", "_") / job_id)

    offline = QualityOfflineStore(tmp_path)
    source_uri = str(tmp_path / "source")
    changed = manifest.model_copy(deep=True)
    changed.batch_sources[0].uri = source_uri
    changed.feature_views[0].quality_policy = policy
    changed.feature_views[0].features[0].quality = FeatureQuality(nullable=False, minimum=0)
    Registry(session).apply(changed)
    start = datetime(2025, 1, 1, tzinfo=UTC)
    offline.append(
        source_uri,
        pa.Table.from_pylist(
            [
                {
                    "account_id": "a",
                    "event_timestamp": start + timedelta(minutes=1),
                    "event_id": "invalid",
                    "amount": -1.0,
                },
                {
                    "account_id": "b",
                    "event_timestamp": start + timedelta(minutes=2),
                    "event_id": "valid",
                    "amount": 2.0,
                },
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
    executor.execute(claim(executor, job.id))

    assert job.status == expected_status, job.error
    target = offline.view_uri("account_stats@1.0.0")
    if expected_rows is None:
        assert not offline.exists(target)
        assert job.result_metadata is not None
        assert job.result_metadata["evaluated_rows"] == 2
        assert job.result_metadata["invalid_rows"] == 1
        return
    assert offline.load(target)["amount"].to_pylist() == expected_rows
    assert job.result_metadata is not None
    assert job.result_metadata["evaluated_rows"] == 2
    assert job.result_metadata["invalid_rows"] == 1
    assert job.result_metadata["counts_by_constraint"] == {"minimum": 1}
    if policy == QualityPolicy.QUARANTINE:
        quarantine = offline.load(offline.quarantine_uri("account_stats@1.0.0", job.id))
        assert quarantine["amount"].to_pylist() == [-1.0]
        assert quarantine["quality_errors"].to_pylist() == [["amount:minimum"]]
        assert job.result_metadata["quarantined_rows"] == 1

        job.status = JobStatus.PENDING
        job.worker_id = None
        job.lease_token = None
        job.lease_expires_at = None
        session.commit()
        executor.execute(claim(executor, job.id))
        assert offline.load(offline.quarantine_uri("account_stats@1.0.0", job.id)).num_rows == 1


def test_expired_jobs_are_reclaimed_and_failed_jobs_can_retry(
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
    executor = JobExecutor(session, worker_id="worker-a")
    running = claim(executor, running.id)
    running.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    session.commit()
    reclaimed = JobExecutor(session, worker_id="worker-b").claim_next()
    assert reclaimed is not None
    assert reclaimed.id == running.id
    assert reclaimed.status == JobStatus.RUNNING
    assert reclaimed.worker_id == "worker-b"

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
        executor = JobExecutor(session, offline=offline)
        executor.execute(claim(executor, job.id))
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
    executor.execute(claim(executor, job.id))
    job.status = JobStatus.PENDING
    job.worker_id = None
    job.lease_token = None
    job.lease_expires_at = None
    session.commit()
    executor.execute(claim(executor, job.id))

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
    executor = JobExecutor(session, offline=offline)
    executor.execute(claim(executor, job.id))

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
    job = JobService(session).create_offline_append("account_stats@1.0.0", staging, commit=False)
    StreamEventLedger(session).mark_staged([registration.record], job.id)
    session.commit()

    executor = JobExecutor(session, offline=offline)
    executor.execute(claim(executor, job.id))
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


class ReplaySafeOnlineStore:
    def __init__(self) -> None:
        self.values: dict[str, tuple[datetime, str, float]] = {}

    def upsert(self, event: StreamFeatureEvent) -> bool:
        entity = str(event.entity_values["account_id"])
        candidate = (event.event_timestamp, event.event_id)
        existing = self.values.get(entity)
        if existing is not None and (existing[0], existing[1]) >= candidate:
            return False
        self.values[entity] = (event.event_timestamp, event.event_id, event.values["amount"])
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
    offline.append(offline.view_uri("account_stats@1.0.0"), rows, partition_by="event_date")
    job = JobService(session).create(
        JobKind.MATERIALIZE,
        JobRequest(
            feature_view="account_stats@1.0.0",
            start=timestamp - timedelta(seconds=1),
            end=timestamp + timedelta(seconds=1),
        ),
    )
    online = CapturingOnlineStore()
    executor = JobExecutor(session, offline=offline, online=online)  # type: ignore[arg-type]
    executor.execute(claim(executor, job.id))

    assert job.status == JobStatus.SUCCEEDED, job.error
    assert [(event.event_id, event.values["amount"]) for event in online.events] == [("b", 2.0)]
    assert job.result_metadata == {
        "mode": "explicit",
        "effective_start": (timestamp - timedelta(seconds=1)).isoformat(),
        "effective_end": (timestamp + timedelta(seconds=1)).isoformat(),
        "lookback_seconds": 0,
        "scanned_rows": 2,
        "candidate_entities": 1,
        "updated_entities": 1,
        "skipped_entities": 0,
        "source_freshness_at": timestamp.isoformat(),
        "freshness_lag_seconds": 1.0,
        "resulting_watermark": None,
    }


def test_incremental_materialization_bootstraps_then_uses_watermark_and_lookback(
    tmp_path: Path, session: Session, manifest: RegistryManifest
) -> None:
    offline = LocalOfflineStore(tmp_path)
    Registry(session).apply(manifest)
    target = offline.view_uri("account_stats@1.0.0")
    first_cutoff = datetime(2025, 1, 2, tzinfo=UTC)
    rows = pa.Table.from_pylist(
        [
            {
                "account_id": "old",
                "event_timestamp": first_cutoff - timedelta(days=30),
                "event_id": "old-1",
                "amount": 1.0,
                "event_date": "2024-12-03",
            },
            {
                "account_id": "a",
                "event_timestamp": first_cutoff - timedelta(minutes=10),
                "event_id": "a-1",
                "amount": 2.0,
                "event_date": "2025-01-01",
            },
            {
                "account_id": "excluded",
                "event_timestamp": first_cutoff,
                "event_id": "excluded-1",
                "amount": 3.0,
                "event_date": "2025-01-02",
            },
        ]
    )
    offline.append(target, rows, partition_by="event_date")
    service = JobService(session)
    first = service.create_incremental_materialization(
        IncrementalMaterializationRequest(
            feature_view="account_stats@1.0.0", end=first_cutoff, lookback_seconds=3600
        )
    )
    assert (
        service.create_incremental_materialization(
            IncrementalMaterializationRequest(feature_view="account_stats@1.0.0")
        ).id
        == first.id
    )
    online = ReplaySafeOnlineStore()
    executor = JobExecutor(session, offline=offline, online=online)  # type: ignore[arg-type]
    executor.execute(claim(executor, first.id))

    state = session.get(MaterializationState, "account_stats@1.0.0")
    assert state is not None
    assert state.watermark == first_cutoff.replace(tzinfo=None)
    assert state.active_job_id is None
    assert state.last_successful_job_id == first.id
    assert first.result_metadata is not None
    assert first.result_metadata["effective_start"] is None
    assert first.result_metadata["scanned_rows"] == 2
    assert set(online.values) == {"old", "a"}

    second_cutoff = first_cutoff + timedelta(hours=1)
    late_rows = pa.Table.from_pylist(
        [
            {
                "account_id": "late_inside",
                "event_timestamp": first_cutoff - timedelta(minutes=30),
                "event_id": "inside-1",
                "amount": 4.0,
                "event_date": "2025-01-01",
            },
            {
                "account_id": "late_outside",
                "event_timestamp": first_cutoff - timedelta(hours=2),
                "event_id": "outside-1",
                "amount": 5.0,
                "event_date": "2025-01-01",
            },
        ],
        schema=rows.schema,
    )
    offline.append(target, late_rows)
    second = service.create_incremental_materialization(
        IncrementalMaterializationRequest(
            feature_view="account_stats@1.0.0", end=second_cutoff, lookback_seconds=3600
        )
    )
    executor.execute(claim(executor, second.id))
    assert second.result_metadata is not None
    assert second.result_metadata["effective_start"] == (
        first_cutoff - timedelta(hours=1)
    ).isoformat()
    assert second.result_metadata["scanned_rows"] == 3
    assert second.result_metadata["candidate_entities"] == 3
    assert second.result_metadata["updated_entities"] == 2
    assert second.result_metadata["skipped_entities"] == 1
    assert "late_inside" in online.values
    assert "late_outside" not in online.values

    empty_cutoff = second_cutoff + timedelta(days=1)
    empty = service.create_incremental_materialization(
        IncrementalMaterializationRequest(
            feature_view="account_stats@1.0.0", end=empty_cutoff, lookback_seconds=0
        )
    )
    executor.execute(claim(executor, empty.id))
    assert empty.result_metadata is not None
    assert empty.result_metadata["scanned_rows"] == 0
    assert empty.result_metadata["candidate_entities"] == 0
    assert empty.result_metadata["updated_entities"] == 0
    assert empty.result_metadata["source_freshness_at"] == first_cutoff.isoformat()


def test_incremental_versions_have_independent_reservations(
    session: Session, manifest: RegistryManifest
) -> None:
    changed = manifest.model_copy(deep=True)
    version_two = changed.feature_views[0].model_copy(update={"version": "2.0.0"})
    changed.feature_views.append(version_two)
    Registry(session).apply(changed)
    service = JobService(session)
    first = service.create_incremental_materialization(
        IncrementalMaterializationRequest(feature_view="account_stats@1.0.0")
    )
    second = service.create_incremental_materialization(
        IncrementalMaterializationRequest(feature_view="account_stats@2.0.0")
    )
    assert first.id != second.id
    first_state = session.get(MaterializationState, "account_stats@1.0.0")
    second_state = session.get(MaterializationState, "account_stats@2.0.0")
    assert first_state is not None and first_state.active_job_id == first.id
    assert second_state is not None and second_state.active_job_id == second.id


def test_incremental_terminal_failure_releases_reservation_and_retry_reacquires(
    session: Session, manifest: RegistryManifest
) -> None:
    Registry(session).apply(manifest)
    service = JobService(session)
    job = service.create_incremental_materialization(
        IncrementalMaterializationRequest(feature_view="account_stats@1.0.0")
    )
    executor = JobExecutor(session, offline=FailingOfflineStore(ValueError("bad table")))
    executor.execute(claim(executor, job.id))
    state = session.get(MaterializationState, "account_stats@1.0.0")
    assert job.status == JobStatus.FAILED
    assert state is not None and state.active_job_id is None and state.watermark is None

    other = service.create_incremental_materialization(
        IncrementalMaterializationRequest(feature_view="account_stats@1.0.0")
    )
    with pytest.raises(ValueError, match="another incremental materialization is active"):
        service.retry(job.id)
    service.cancel(other.id)
    assert service.retry(job.id).status == JobStatus.PENDING
    assert state.active_job_id == job.id


def test_workers_claim_distinct_jobs_and_not_an_unexpired_job(session: Session) -> None:
    service = JobService(session)
    first = service.create_offline_append("account_stats@1.0.0", "/tmp/first")
    second = service.create_offline_append("account_stats@1.0.0", "/tmp/second")
    other_session = Session(session.get_bind(), expire_on_commit=False)
    try:
        first_claim = JobExecutor(session, worker_id="worker-a").claim_next()
        second_claim = JobExecutor(other_session, worker_id="worker-b").claim_next()
        assert first_claim is not None and first_claim.id == first.id
        assert second_claim is not None and second_claim.id == second.id
        assert JobExecutor(other_session, worker_id="worker-b").claim_next() is None
        assert first_claim.lease_token != second_claim.lease_token
    finally:
        other_session.close()


def test_heartbeat_requires_the_matching_unexpired_lease(session: Session) -> None:
    start = datetime.now(UTC)
    JobService(session).create_offline_append("account_stats@1.0.0", "/tmp/staging")
    executor = JobExecutor(session, worker_id="worker-a")
    claimed = executor.claim_next(now=start)
    assert claimed is not None and claimed.lease_token is not None
    original_expiry = claimed.lease_expires_at

    assert not executor.heartbeat(claimed.id, "stale-token", now=start + timedelta(seconds=5))
    assert executor.heartbeat(claimed.id, claimed.lease_token, now=start + timedelta(seconds=5))
    session.refresh(claimed)
    assert claimed.lease_expires_at is not None
    assert original_expiry is not None and claimed.lease_expires_at > original_expiry
    assert not executor.heartbeat(
        claimed.id, claimed.lease_token, now=start + timedelta(seconds=40)
    )

    response = serialize_job(claimed)
    assert "lease_token" not in response
    assert {
        "attempt_count",
        "max_attempts",
        "next_attempt_at",
        "worker_id",
        "lease_expires_at",
        "last_heartbeat_at",
        "failure_kind",
    } <= response.keys()


def test_reclaim_fences_the_previous_worker_from_checkpoints_and_success(
    session: Session,
) -> None:
    start = datetime.now(UTC)
    JobService(session).create_offline_append("account_stats@1.0.0", "/tmp/staging")
    first_executor = JobExecutor(session, worker_id="worker-a")
    first = first_executor.claim_next(now=start)
    assert first is not None and first.lease_token is not None
    stale_token = first.lease_token

    second_executor = JobExecutor(session, worker_id="worker-b")
    second = second_executor.claim_next(now=start + timedelta(seconds=31))
    assert second is not None
    assert second.id == first.id
    assert second.attempt_count == 2
    assert second.lease_token != stale_token

    with pytest.raises(LeaseLostError):
        first_executor._commit_checkpoints(first, stale_token, ["stale"])  # noqa: SLF001
    with pytest.raises(LeaseLostError):
        first_executor._finalize_success(first, stale_token)  # noqa: SLF001
    session.refresh(second)
    assert second.status == JobStatus.RUNNING
    assert second.checkpoints == []


def test_expired_final_attempt_becomes_exhausted(session: Session) -> None:
    start = datetime.now(UTC)
    JobService(session).create_offline_append("account_stats@1.0.0", "/tmp/staging")
    executor = JobExecutor(session, worker_id="worker-a")
    claimed = executor.claim_next(now=start)
    assert claimed is not None
    claimed.attempt_count = claimed.max_attempts
    claimed.lease_expires_at = start - timedelta(seconds=1)
    session.commit()

    assert executor.claim_next(worker_id="worker-b", now=start) is None
    session.refresh(claimed)
    assert claimed.status == JobStatus.EXHAUSTED
    assert claimed.failure_kind == "lease_expired"
    assert claimed.finished_at is not None
    assert claimed.lease_token is None


class FailingOfflineStore(OfflineStore):
    def __init__(self, error: Exception):
        super().__init__()
        self.error = error

    def load(self, uri: str, *, version: int | None = None) -> pa.Table:
        raise self.error

    def load_range(
        self, uri: str, timestamp_field: str, start: datetime | None, end: datetime
    ) -> pa.Table:
        raise self.error


class FlakyOfflineStore(OfflineStore):
    def __init__(self) -> None:
        super().__init__()
        self.load_count = 0

    def load(self, uri: str, *, version: int | None = None) -> pa.Table:
        self.load_count += 1
        if self.load_count == 1:
            raise RuntimeError("object store unavailable")
        return stream_table()

    def view_uri(self, view_ref: str) -> str:
        return "/tmp/nonexistent-target"

    def exists(self, uri: str) -> bool:
        return False

    def append(self, uri: str, table: pa.Table, partition_by: str | None = None) -> None:
        return None

    def delete(self, uri: str) -> bool:
        return True


def test_retryable_failure_uses_backoff_and_success_clears_failure(
    session: Session,
) -> None:
    job = JobService(session).create_offline_append("account_stats@1.0.0", "/tmp/staging")
    executor = JobExecutor(session, offline=FlakyOfflineStore(), worker_id="worker-a")
    first = claim(executor, job.id)
    failed_at = datetime.now(UTC)
    executor.execute(first)

    assert first.status == JobStatus.RETRYING
    assert first.failure_kind == "retryable"
    assert first.error is not None and "object store unavailable" in first.error
    assert first.finished_at is None
    assert first.next_attempt_at is not None
    retry_at = first.next_attempt_at.replace(tzinfo=UTC)
    assert timedelta(seconds=4) <= retry_at - failed_at <= timedelta(seconds=6)
    assert executor.claim_next(now=retry_at - timedelta(microseconds=1)) is None

    second = executor.claim_next(now=retry_at)
    assert second is not None and second.attempt_count == 2
    executor.execute(second)
    assert second.status == JobStatus.SUCCEEDED
    assert second.error is None
    assert second.failure_kind is None
    assert second.next_attempt_at is None


def test_terminal_failure_stops_immediately(session: Session) -> None:
    job = JobService(session).create_offline_append("account_stats@1.0.0", "/tmp/staging")
    executor = JobExecutor(
        session,
        offline=FailingOfflineStore(ValueError("invalid staging schema")),
        worker_id="worker-a",
    )
    claimed = claim(executor, job.id)
    executor.execute(claimed)

    assert claimed.status == JobStatus.FAILED
    assert claimed.failure_kind == "terminal"
    assert claimed.attempt_count == 1
    assert claimed.next_attempt_at is None
    assert claimed.finished_at is not None
    assert executor.claim_next() is None


def test_repeated_retryable_failures_exhaust_attempt_budget(session: Session) -> None:
    settings = Settings(job_retry_base_seconds=5, job_retry_max_seconds=60)
    clock = [datetime.now(UTC)]
    job = JobService(session, settings).create_offline_append("account_stats@1.0.0", "/tmp/staging")
    executor = JobExecutor(
        session,
        offline=FailingOfflineStore(RuntimeError("temporary outage")),
        worker_id="worker-a",
        settings=settings,
        now=lambda: clock[0],
    )
    claimed = claim(executor, job.id)
    executor.execute(claimed)
    assert claimed.status == JobStatus.RETRYING
    first_retry = claimed.next_attempt_at
    assert first_retry is not None

    clock[0] = first_retry.replace(tzinfo=UTC)
    claimed = executor.claim_next()
    assert claimed is not None
    executor.execute(claimed)
    assert claimed.status == JobStatus.RETRYING
    second_retry = claimed.next_attempt_at
    assert second_retry is not None
    first_retry_utc = first_retry.replace(tzinfo=UTC)
    second_retry_utc = second_retry.replace(tzinfo=UTC)
    assert timedelta(seconds=9) <= second_retry_utc - first_retry_utc <= timedelta(seconds=11)

    clock[0] = second_retry_utc
    claimed = executor.claim_next()
    assert claimed is not None
    executor.execute(claimed)
    assert claimed.status == JobStatus.EXHAUSTED
    assert claimed.attempt_count == 3
    assert claimed.finished_at is not None
    assert claimed.next_attempt_at is None


@pytest.mark.parametrize("status", [JobStatus.FAILED, JobStatus.EXHAUSTED])
def test_manual_retry_resets_failure_and_lease_metadata(
    session: Session, status: JobStatus
) -> None:
    service = JobService(session)
    job = service.create_offline_append("account_stats@1.0.0", "/tmp/staging")
    job.status = status
    job.error = "failure"
    job.failure_kind = "terminal"
    job.attempt_count = job.max_attempts
    job.next_attempt_at = datetime.now(UTC)
    job.worker_id = "old-worker"
    job.lease_token = "old-token"
    job.lease_expires_at = datetime.now(UTC)
    job.last_heartbeat_at = datetime.now(UTC)
    job.started_at = datetime.now(UTC)
    job.finished_at = datetime.now(UTC)
    session.commit()

    retried = service.retry(job.id)
    assert retried.status == JobStatus.PENDING
    assert retried.attempt_count == 0
    assert retried.error is None
    assert retried.failure_kind is None
    assert retried.next_attempt_at is None
    assert retried.worker_id is None
    assert retried.lease_token is None
    assert retried.lease_expires_at is None
    assert retried.last_heartbeat_at is None
    assert retried.started_at is None
    assert retried.finished_at is None


def test_running_cancellation_revokes_lease_and_cannot_be_overwritten(
    session: Session,
) -> None:
    job = JobService(session).create_offline_append("account_stats@1.0.0", "/tmp/staging")
    executor = JobExecutor(session, worker_id="worker-a")
    claimed = claim(executor, job.id)
    JobService(session).cancel(job.id)

    executor.execute(claimed)
    session.refresh(claimed)
    assert claimed.status == JobStatus.CANCELLED
    assert claimed.lease_token is None
    assert claimed.finished_at is not None
