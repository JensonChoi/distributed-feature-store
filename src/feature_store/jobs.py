from __future__ import annotations

import json
import traceback
import uuid
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from deltalake.exceptions import SchemaMismatchError
from pydantic import ValidationError
from sqlalchemy import and_, or_, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import Session

from feature_store.config import Settings, get_settings
from feature_store.db import JobRecord, StreamEventRecord
from feature_store.models import (
    Feature,
    JobFailureKind,
    JobKind,
    JobRequest,
    JobStatus,
    StreamEventState,
    StreamFeatureEvent,
    ValueType,
)
from feature_store.offline import OfflineStore, normalize_uri
from feature_store.online import OnlineStore
from feature_store.registry import Registry, RegistryConflictError, RegistryNotFoundError


class LeaseLostError(RuntimeError):
    """The worker no longer owns the job and must not persist execution state."""


def serialize_job(job: JobRecord) -> dict[str, Any]:
    return {
        "id": job.id,
        "kind": job.kind,
        "status": job.status,
        "payload": job.payload,
        "checkpoints": job.checkpoints,
        "error": job.error,
        "failure_kind": job.failure_kind,
        "attempt_count": job.attempt_count,
        "max_attempts": job.max_attempts,
        "next_attempt_at": job.next_attempt_at,
        "worker_id": job.worker_id,
        "lease_expires_at": job.lease_expires_at,
        "last_heartbeat_at": job.last_heartbeat_at,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
    }


class JobService:
    def __init__(self, session: Session, settings: Settings | None = None):
        self.session = session
        self.settings = settings or get_settings()

    def create(self, kind: JobKind, request: JobRequest) -> JobRecord:
        payload = request.model_dump(mode="json")
        job = JobRecord(
            kind=kind,
            status=JobStatus.PENDING,
            payload=payload,
            checkpoints=[],
            max_attempts=self.settings.job_max_attempts,
        )
        self.session.add(job)
        self.session.commit()
        self.session.refresh(job)
        return job

    def create_offline_append(
        self, feature_view: str, staging_uri: str, *, commit: bool = True
    ) -> JobRecord:
        job = JobRecord(
            kind=JobKind.OFFLINE_APPEND,
            status=JobStatus.PENDING,
            payload={"feature_view": feature_view, "staging_uri": staging_uri},
            checkpoints=[],
            max_attempts=self.settings.job_max_attempts,
        )
        self.session.add(job)
        if commit:
            self.session.commit()
            self.session.refresh(job)
        else:
            self.session.flush()
        return job

    def get(self, job_id: str) -> JobRecord:
        job = self.session.get(JobRecord, job_id)
        if not job:
            raise KeyError(f"unknown job: {job_id}")
        return job

    def list(self, limit: int = 100) -> list[JobRecord]:
        statement = select(JobRecord).order_by(JobRecord.created_at.desc()).limit(limit)
        return list(self.session.scalars(statement))

    def cancel(self, job_id: str) -> JobRecord:
        job = self.get(job_id)
        if job.status not in (JobStatus.PENDING, JobStatus.RETRYING, JobStatus.RUNNING):
            raise ValueError("only pending, retrying, or running jobs can be cancelled")
        result = cast(
            CursorResult[Any],
            self.session.execute(
                update(JobRecord)
                .execution_options(synchronize_session=False)
                .where(
                    JobRecord.id == job_id,
                    JobRecord.status.in_(
                        (JobStatus.PENDING, JobStatus.RETRYING, JobStatus.RUNNING)
                    ),
                )
                .values(
                    status=JobStatus.CANCELLED,
                    finished_at=datetime.now(UTC),
                    next_attempt_at=None,
                    worker_id=None,
                    lease_token=None,
                    lease_expires_at=None,
                    last_heartbeat_at=None,
                )
            ),
        )
        if not result.rowcount:
            self.session.rollback()
            self.session.refresh(job)
            raise ValueError("only pending, retrying, or running jobs can be cancelled")
        self.session.commit()
        self.session.refresh(job)
        return job

    def retry(self, job_id: str) -> JobRecord:
        job = self.get(job_id)
        if job.status not in (JobStatus.FAILED, JobStatus.EXHAUSTED):
            raise ValueError("only failed or exhausted jobs can be retried")
        result = cast(
            CursorResult[Any],
            self.session.execute(
                update(JobRecord)
                .execution_options(synchronize_session=False)
                .where(
                    JobRecord.id == job_id,
                    JobRecord.status.in_((JobStatus.FAILED, JobStatus.EXHAUSTED)),
                )
                .values(
                    status=JobStatus.PENDING,
                    error=None,
                    failure_kind=None,
                    attempt_count=0,
                    max_attempts=self.settings.job_max_attempts,
                    next_attempt_at=None,
                    worker_id=None,
                    lease_token=None,
                    lease_expires_at=None,
                    last_heartbeat_at=None,
                    started_at=None,
                    finished_at=None,
                )
            ),
        )
        if not result.rowcount:
            self.session.rollback()
            self.session.refresh(job)
            raise ValueError("only failed or exhausted jobs can be retried")
        self.session.commit()
        self.session.refresh(job)
        return job


class JobExecutor:
    def __init__(
        self,
        session: Session,
        offline: OfflineStore | None = None,
        online: OnlineStore | None = None,
        *,
        worker_id: str | None = None,
        settings: Settings | None = None,
        now: Callable[[], datetime] | None = None,
    ):
        self.session = session
        self.settings = settings or get_settings()
        self.worker_id = worker_id or f"worker-{uuid.uuid4()}"
        self.now = now or (lambda: datetime.now(UTC))
        self.registry = Registry(session)
        self.offline = offline or OfflineStore()
        self.online = online or OnlineStore()

    def claim_next(
        self, worker_id: str | None = None, *, now: datetime | None = None
    ) -> JobRecord | None:
        owner = worker_id or self.worker_id
        claimed_at = now or self.now()
        while True:
            eligible = or_(
                JobRecord.status == JobStatus.PENDING,
                and_(
                    JobRecord.status == JobStatus.RETRYING,
                    or_(
                        JobRecord.next_attempt_at.is_(None),
                        JobRecord.next_attempt_at <= claimed_at,
                    ),
                ),
                and_(
                    JobRecord.status == JobStatus.RUNNING,
                    or_(
                        JobRecord.lease_expires_at.is_(None),
                        JobRecord.lease_expires_at <= claimed_at,
                    ),
                ),
            )
            candidate = self.session.scalar(
                select(JobRecord)
                .where(eligible)
                .order_by(JobRecord.created_at, JobRecord.id)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if candidate is None:
                return None

            expired = candidate.status == JobStatus.RUNNING
            if candidate.attempt_count >= candidate.max_attempts:
                result = cast(
                    CursorResult[Any],
                    self.session.execute(
                        update(JobRecord)
                        .execution_options(synchronize_session=False)
                        .where(
                            JobRecord.id == candidate.id,
                            eligible,
                            JobRecord.attempt_count >= JobRecord.max_attempts,
                        )
                        .values(
                            status=JobStatus.EXHAUSTED,
                            failure_kind=(
                                JobFailureKind.LEASE_EXPIRED
                                if expired
                                else candidate.failure_kind or JobFailureKind.RETRYABLE
                            ),
                            error=candidate.error
                            or (
                                "job lease expired after the final attempt"
                                if expired
                                else "job attempt budget was exhausted"
                            ),
                            finished_at=claimed_at,
                            next_attempt_at=None,
                            worker_id=None,
                            lease_token=None,
                            lease_expires_at=None,
                            last_heartbeat_at=None,
                        )
                    ),
                )
                self.session.commit()
                if result.rowcount:
                    self.session.expire_all()
                continue

            lease_token = str(uuid.uuid4())
            result = cast(
                CursorResult[Any],
                self.session.execute(
                    update(JobRecord)
                    .execution_options(synchronize_session=False)
                    .where(
                        JobRecord.id == candidate.id,
                        eligible,
                        JobRecord.attempt_count < JobRecord.max_attempts,
                    )
                    .values(
                        status=JobStatus.RUNNING,
                        attempt_count=JobRecord.attempt_count + 1,
                        started_at=claimed_at,
                        finished_at=None,
                        next_attempt_at=None,
                        worker_id=owner,
                        lease_token=lease_token,
                        lease_expires_at=claimed_at
                        + timedelta(seconds=self.settings.job_lease_seconds),
                        last_heartbeat_at=claimed_at,
                        failure_kind=(
                            JobFailureKind.LEASE_EXPIRED if expired else candidate.failure_kind
                        ),
                    )
                ),
            )
            self.session.commit()
            if not result.rowcount:
                self.session.expire_all()
                continue
            job = self.session.get(JobRecord, candidate.id)
            if job is None:
                return None
            self.session.refresh(job)
            return job

    def heartbeat(self, job_id: str, lease_token: str, *, now: datetime | None = None) -> bool:
        heartbeat_at = now or self.now()
        result = cast(
            CursorResult[Any],
            self.session.execute(
                update(JobRecord)
                .execution_options(synchronize_session=False)
                .where(
                    JobRecord.id == job_id,
                    JobRecord.status == JobStatus.RUNNING,
                    JobRecord.lease_token == lease_token,
                    JobRecord.lease_expires_at > heartbeat_at,
                )
                .values(
                    lease_expires_at=heartbeat_at
                    + timedelta(seconds=self.settings.job_lease_seconds),
                    last_heartbeat_at=heartbeat_at,
                )
            ),
        )
        self.session.commit()
        return bool(result.rowcount)

    def execute(self, job: JobRecord) -> None:
        if not job.lease_token or not self._owns(job.id, job.lease_token):
            with suppress(Exception):
                self.session.refresh(job)
            return
        lease_token = job.lease_token
        cleanup_uri: str | None = None
        try:
            if job.kind == JobKind.BACKFILL:
                self._backfill(job, lease_token)
            elif job.kind == JobKind.MATERIALIZE:
                self._materialize(job, lease_token)
            elif job.kind == JobKind.OFFLINE_APPEND:
                cleanup_uri = self._offline_append(job, lease_token)
            else:
                raise ValueError(f"unsupported job kind: {job.kind}")
            self._finalize_success(job, lease_token)
        except LeaseLostError:
            self.session.rollback()
            with suppress(Exception):
                self.session.refresh(job)
            return
        except Exception as exc:
            error = traceback.format_exc(limit=10)
            self.session.rollback()
            self._finalize_failure(job, lease_token, exc, error)
            return
        if cleanup_uri:
            with suppress(Exception):
                self.offline.delete(cleanup_uri)

    def _owns(self, job_id: str, lease_token: str, *, now: datetime | None = None) -> bool:
        checked_at = now or self.now()
        owned = (
            self.session.scalar(
                select(JobRecord.id).where(
                    JobRecord.id == job_id,
                    JobRecord.status == JobStatus.RUNNING,
                    JobRecord.lease_token == lease_token,
                    JobRecord.lease_expires_at > checked_at,
                )
            )
            is not None
        )
        # Release read transactions before potentially long external calls so a heartbeat
        # session can update the same row on SQLite as well as Postgres.
        self.session.commit()
        return owned

    def _require_ownership(self, job: JobRecord, lease_token: str) -> None:
        if not self._owns(job.id, lease_token):
            raise LeaseLostError(f"job lease was lost: {job.id}")

    def _commit_checkpoints(self, job: JobRecord, lease_token: str, checkpoints: list[str]) -> None:
        now = self.now()
        result = cast(
            CursorResult[Any],
            self.session.execute(
                update(JobRecord)
                .execution_options(synchronize_session=False)
                .where(
                    JobRecord.id == job.id,
                    JobRecord.status == JobStatus.RUNNING,
                    JobRecord.lease_token == lease_token,
                    JobRecord.lease_expires_at > now,
                )
                .values(checkpoints=checkpoints)
            ),
        )
        if not result.rowcount:
            self.session.rollback()
            raise LeaseLostError(f"job lease was lost before checkpoint: {job.id}")
        self.session.commit()
        self.session.refresh(job)

    def _finalize_success(self, job: JobRecord, lease_token: str) -> None:
        now = self.now()
        result = cast(
            CursorResult[Any],
            self.session.execute(
                update(JobRecord)
                .execution_options(synchronize_session=False)
                .where(
                    JobRecord.id == job.id,
                    JobRecord.status == JobStatus.RUNNING,
                    JobRecord.lease_token == lease_token,
                    JobRecord.lease_expires_at > now,
                )
                .values(
                    status=JobStatus.SUCCEEDED,
                    error=None,
                    failure_kind=None,
                    finished_at=now,
                    next_attempt_at=None,
                    worker_id=None,
                    lease_token=None,
                    lease_expires_at=None,
                    last_heartbeat_at=None,
                )
            ),
        )
        if not result.rowcount:
            self.session.rollback()
            raise LeaseLostError(f"job lease was lost before success: {job.id}")
        if job.kind == JobKind.OFFLINE_APPEND:
            self._mark_stream_events_applied(job)
        self.session.commit()
        self.session.refresh(job)

    def _finalize_failure(
        self, job: JobRecord, lease_token: str, exc: Exception, error: str
    ) -> None:
        now = self.now()
        terminal = self._is_terminal_failure(exc)
        if terminal:
            status = JobStatus.FAILED
            failure_kind = JobFailureKind.TERMINAL
            next_attempt_at = None
            finished_at = now
        elif job.attempt_count >= job.max_attempts:
            status = JobStatus.EXHAUSTED
            failure_kind = JobFailureKind.RETRYABLE
            next_attempt_at = None
            finished_at = now
        else:
            status = JobStatus.RETRYING
            failure_kind = JobFailureKind.RETRYABLE
            delay = min(
                self.settings.job_retry_base_seconds * (2 ** (job.attempt_count - 1)),
                self.settings.job_retry_max_seconds,
            )
            next_attempt_at = now + timedelta(seconds=delay)
            finished_at = None
        result = cast(
            CursorResult[Any],
            self.session.execute(
                update(JobRecord)
                .execution_options(synchronize_session=False)
                .where(
                    JobRecord.id == job.id,
                    JobRecord.status == JobStatus.RUNNING,
                    JobRecord.lease_token == lease_token,
                    JobRecord.lease_expires_at > now,
                )
                .values(
                    status=status,
                    error=error,
                    failure_kind=failure_kind,
                    next_attempt_at=next_attempt_at,
                    finished_at=finished_at,
                    worker_id=None,
                    lease_token=None,
                    lease_expires_at=None,
                    last_heartbeat_at=None,
                )
            ),
        )
        if not result.rowcount:
            self.session.rollback()
            with suppress(Exception):
                self.session.refresh(job)
            return
        self.session.commit()
        self.session.refresh(job)

    @staticmethod
    def _is_terminal_failure(exc: Exception) -> bool:
        return isinstance(
            exc,
            (
                RegistryNotFoundError,
                RegistryConflictError,
                ValidationError,
                ValueError,
                KeyError,
                TypeError,
                pa.ArrowException,
                duckdb.Error,
                SchemaMismatchError,
            ),
        )

    def _backfill(self, job: JobRecord, lease_token: str) -> None:
        view = self.registry.feature_view(job.payload["feature_view"])
        source = self.registry.batch_source(view.batch_source)
        entity = self.registry.entity(view.entity)
        start = datetime.fromisoformat(job.payload["start"]).astimezone(UTC)
        end = datetime.fromisoformat(job.payload["end"]).astimezone(UTC)
        cursor = start
        while cursor < end:
            self._require_ownership(job, lease_token)
            chunk_end = min(
                end, (cursor + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            )
            if chunk_end <= cursor:
                chunk_end = min(end, cursor + timedelta(days=1))
            checkpoint = cursor.isoformat()
            if checkpoint not in job.checkpoints:
                self._require_ownership(job, lease_token)
                lookback = timedelta(seconds=view.ttl_seconds or 0)
                source_table = self.offline.load_range(
                    normalize_uri(source.uri),
                    source.event_timestamp_field,
                    cursor - lookback,
                    chunk_end,
                )
                output = self._transform(view.batch_sql, source_table)
                output = self._validate_output(output, entity.join_keys, view.features)
                mask = pc.and_(
                    pc.greater_equal(output["event_timestamp"], pa.scalar(cursor)),
                    pc.less(output["event_timestamp"], pa.scalar(chunk_end)),
                )
                output = output.filter(mask)
                event_dates = pc.strftime(output["event_timestamp"], format="%Y-%m-%d")
                output = output.append_column("event_date", event_dates)
                target = self.offline.view_uri(view.ref)
                date = cursor.date().isoformat()
                self._require_ownership(job, lease_token)
                if self.offline.exists(target):
                    existing = self.offline.load(target)
                    same_partition = pc.equal(existing["event_date"], pa.scalar(date))
                    outside_range = pc.or_(
                        pc.less(existing["event_timestamp"], pa.scalar(cursor)),
                        pc.greater_equal(existing["event_timestamp"], pa.scalar(chunk_end)),
                    )
                    preserved = existing.filter(pc.and_(same_partition, outside_range))
                    replacement = pa.concat_tables([preserved, output], promote_options="default")
                    self.offline.overwrite_partition(target, replacement, f"event_date = '{date}'")
                elif output.num_rows:
                    self.offline.append(target, output, partition_by="event_date")
                self._commit_checkpoints(job, lease_token, [*job.checkpoints, checkpoint])
            cursor = chunk_end

    def _materialize(self, job: JobRecord, lease_token: str) -> None:
        view = self.registry.feature_view(job.payload["feature_view"])
        entity = self.registry.entity(view.entity)
        start = datetime.fromisoformat(job.payload["start"]).astimezone(UTC)
        end = datetime.fromisoformat(job.payload["end"]).astimezone(UTC)
        self._require_ownership(job, lease_token)
        table = self.offline.load_range(
            self.offline.view_uri(view.ref), "event_timestamp", start, end
        )
        connection = duckdb.connect()
        connection.execute("SET TimeZone='UTC'")
        connection.register("rows", table)
        partition = ", ".join(f'"{key}"' for key in entity.join_keys)
        latest = connection.sql(
            f"SELECT * EXCLUDE (event_date) FROM rows QUALIFY ROW_NUMBER() OVER "
            f"(PARTITION BY {partition} ORDER BY event_timestamp DESC, event_id DESC) = 1"
        ).to_arrow_table()
        connection.close()
        feature_names = [feature.name for feature in view.features]
        for row in latest.to_pylist():
            self._require_ownership(job, lease_token)
            self.online.upsert(
                StreamFeatureEvent(
                    event_id=row["event_id"],
                    feature_view=view.ref,
                    entity_values={key: row[key] for key in entity.join_keys},
                    event_timestamp=row["event_timestamp"],
                    values={name: row[name] for name in feature_names},
                )
            )
        self._commit_checkpoints(job, lease_token, [f"materialized:{latest.num_rows}"])

    def _offline_append(self, job: JobRecord, lease_token: str) -> str:
        self._require_ownership(job, lease_token)
        staging = self.offline.load(job.payload["staging_uri"])
        target = self.offline.view_uri(job.payload["feature_view"])
        unseen = self._unseen_rows(staging, target)
        if unseen.num_rows:
            self._require_ownership(job, lease_token)
            self.offline.append(target, unseen, partition_by="event_date")
        self._commit_checkpoints(
            job,
            lease_token,
            [
                f"appended:{unseen.num_rows}",
                f"duplicates:{staging.num_rows - unseen.num_rows}",
            ],
        )
        return str(job.payload["staging_uri"])

    def _mark_stream_events_applied(self, job: JobRecord) -> None:
        now = self.now()
        records = self.session.scalars(
            select(StreamEventRecord).where(
                StreamEventRecord.job_id == job.id,
                StreamEventRecord.state == StreamEventState.STAGED,
            )
        )
        for record in records:
            record.state = StreamEventState.APPLIED
            record.applied_at = now
            record.updated_at = now

    def _unseen_rows(self, staging: pa.Table, target: str) -> pa.Table:
        if "event_id" not in staging.column_names:
            raise ValueError("offline append staging data is missing event_id")

        existing_by_id: dict[str, str] = {}
        if self.offline.exists(target):
            existing = self.offline.load(target)
            if "event_id" not in existing.column_names:
                raise ValueError("offline feature view is missing event_id")
            for row in existing.to_pylist():
                event_id = str(row["event_id"])
                fingerprint = self._canonical_row(row)
                prior = existing_by_id.setdefault(event_id, fingerprint)
                if prior != fingerprint:
                    raise ValueError(
                        f"offline feature view contains conflicting rows for event_id {event_id}"
                    )

        staged_by_id: dict[str, str] = {}
        staged_unseen_ids: set[str] = set()
        unseen: list[dict[str, Any]] = []
        for row in staging.to_pylist():
            event_id = str(row["event_id"])
            fingerprint = self._canonical_row(row)
            prior_staged = staged_by_id.setdefault(event_id, fingerprint)
            if prior_staged != fingerprint:
                raise ValueError(f"staging contains conflicting rows for event_id {event_id}")
            if event_id in staged_unseen_ids:
                continue
            prior_existing = existing_by_id.get(event_id)
            if prior_existing is not None:
                if prior_existing != fingerprint:
                    raise ValueError(f"event_id {event_id} conflicts with existing offline content")
                continue
            unseen.append(row)
            staged_unseen_ids.add(event_id)

        return pa.Table.from_pylist(unseen, schema=staging.schema)

    @classmethod
    def _canonical_row(cls, row: dict[str, Any]) -> str:
        def normalize(value: Any) -> Any:
            if isinstance(value, datetime):
                if value.tzinfo is None:
                    return value.isoformat(timespec="microseconds")
                return value.astimezone(UTC).isoformat(timespec="microseconds")
            if isinstance(value, date):
                return value.isoformat()
            if isinstance(value, dict):
                return {key: normalize(item) for key, item in sorted(value.items())}
            if isinstance(value, list):
                return [normalize(item) for item in value]
            return value

        return json.dumps(normalize(row), sort_keys=True, separators=(",", ":"), default=str)

    @staticmethod
    def _transform(sql: str, source: pa.Table) -> pa.Table:
        connection = duckdb.connect()
        connection.execute("SET TimeZone='UTC'")
        connection.register("source", source)
        try:
            return connection.sql(sql).to_arrow_table()
        finally:
            connection.close()

    @staticmethod
    def _validate_output(
        table: pa.Table, entity_keys: dict[str, ValueType], features: list[Feature]
    ) -> pa.Table:
        feature_names = [feature.name for feature in features]
        required = {*entity_keys, *feature_names, "event_timestamp", "event_id"}
        missing = required - set(table.column_names)
        if missing:
            raise ValueError(f"batch SQL missing required columns: {sorted(missing)}")
        result = table.select([*entity_keys, "event_timestamp", "event_id", *feature_names])
        types = {
            ValueType.STRING: pa.string(),
            ValueType.INT64: pa.int64(),
            ValueType.FLOAT64: pa.float64(),
            ValueType.BOOL: pa.bool_(),
            ValueType.TIMESTAMP: pa.timestamp("us", tz="UTC"),
        }
        declared = {**entity_keys, **{feature.name: feature.dtype for feature in features}}
        declared["event_id"] = ValueType.STRING
        declared["event_timestamp"] = ValueType.TIMESTAMP
        for name, dtype in declared.items():
            index = result.schema.get_field_index(name)
            result = result.set_column(index, name, pc.cast(result[name], types[dtype]))
        return result
