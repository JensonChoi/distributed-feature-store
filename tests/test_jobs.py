from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
from sqlalchemy.orm import Session

from feature_store.jobs import JobExecutor, JobService
from feature_store.models import JobKind, JobRequest, JobStatus, RegistryManifest
from feature_store.offline import OfflineStore
from feature_store.registry import Registry


class LocalOfflineStore(OfflineStore):
    def __init__(self, root: Path):
        super().__init__()
        self.root = root

    def view_uri(self, view_ref: str) -> str:
        return str(self.root / "views" / view_ref.replace("@", "_"))


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
