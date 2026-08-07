from __future__ import annotations

import json
import time
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
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from feature_store.artifacts import ArtifactStorage
from feature_store.config import Settings, get_settings
from feature_store.db import JobRecord, MaterializationState, StreamEventRecord
from feature_store.models import (
    DataQualitySummary,
    Feature,
    HistoricalQuery,
    IncrementalMaterializationRequest,
    JobFailureKind,
    JobKind,
    JobRequest,
    JobStatus,
    QualityPolicy,
    RegistryWarning,
    StreamEventState,
    StreamFeatureEvent,
    ValueType,
)
from feature_store.observability import METRICS, Metrics
from feature_store.offline import OfflineStore, normalize_uri
from feature_store.online import OnlineStore
from feature_store.pit import HistoricalRetriever
from feature_store.quality import QualityValidation, validate_quality_table
from feature_store.registry import Registry, RegistryConflictError, RegistryNotFoundError


class LeaseLostError(RuntimeError):
    """The worker no longer owns the job and must not persist execution state."""


class DataQualityError(ValueError):
    def __init__(self, message: str, summary: DataQualitySummary):
        super().__init__(message)
        self.summary = summary


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def serialize_job(job: JobRecord) -> dict[str, Any]:
    result = None
    if job.result_metadata:
        result = dict(job.result_metadata)
        if job.kind == JobKind.HISTORICAL_QUERY and job.artifact_expires_at:
            result.update(
                download_url=f"/v1/jobs/{job.id}/result",
                expires_at=job.artifact_expires_at,
                cleaned_up=job.artifacts_cleaned_at is not None,
            )
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
        "artifact_expires_at": job.artifact_expires_at,
        "artifacts_cleaned_at": job.artifacts_cleaned_at,
        "result": result,
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

    def create_historical_query(
        self,
        query: HistoricalQuery,
        resolved_features: list[str],
        artifacts: ArtifactStorage,
        *,
        warnings: list[RegistryWarning] | None = None,
    ) -> JobRecord:
        job_id = str(uuid.uuid4())
        artifact_uri = artifacts.input_uri(job_id)
        artifacts.write_json(artifact_uri, query.model_dump(mode="json"))
        job = JobRecord(
            id=job_id,
            kind=JobKind.HISTORICAL_QUERY,
            status=JobStatus.PENDING,
            payload={
                "observation_count": len(query.observations),
                "resolved_features": resolved_features,
                "warnings": [warning.model_dump(mode="json") for warning in (warnings or [])],
            },
            artifact_uri=artifact_uri,
            checkpoints=[],
            max_attempts=self.settings.job_max_attempts,
        )
        try:
            self.session.add(job)
            self.session.commit()
            self.session.refresh(job)
        except Exception:
            self.session.rollback()
            with suppress(Exception):
                artifacts.delete_job(job_id)
            raise
        return job

    def create_incremental_materialization(
        self,
        request: IncrementalMaterializationRequest,
        *,
        now: datetime | None = None,
    ) -> JobRecord:
        submitted_at = _as_utc(now or datetime.now(UTC))
        cutoff = _as_utc(request.end or submitted_at)
        lookback = (
            request.lookback_seconds
            if request.lookback_seconds is not None
            else self.settings.materialization_lookback_seconds
        )
        for attempt in range(2):
            state = self.session.scalar(
                select(MaterializationState)
                .where(MaterializationState.feature_view == request.feature_view)
                .with_for_update()
            )
            if state is not None and state.active_job_id:
                active = self.session.get(JobRecord, state.active_job_id)
                if active is not None and active.status in (
                    JobStatus.PENDING,
                    JobStatus.RETRYING,
                    JobStatus.RUNNING,
                ):
                    self.session.commit()
                    return active
                state.active_job_id = None
            if state is not None and state.watermark and cutoff < _as_utc(state.watermark):
                self.session.rollback()
                raise ValueError("end cannot precede the successful materialization watermark")
            if state is None:
                state = MaterializationState(
                    feature_view=request.feature_view,
                    updated_at=submitted_at,
                )
                self.session.add(state)
            job = JobRecord(
                id=str(uuid.uuid4()),
                kind=JobKind.MATERIALIZE,
                status=JobStatus.PENDING,
                payload={
                    "mode": "incremental",
                    "feature_view": request.feature_view,
                    "end": cutoff.isoformat(),
                    "lookback_seconds": lookback,
                },
                checkpoints=[],
                max_attempts=self.settings.job_max_attempts,
            )
            self.session.add(job)
            state.active_job_id = job.id
            state.updated_at = submitted_at
            try:
                self.session.commit()
            except IntegrityError:
                self.session.rollback()
                if attempt == 0:
                    continue
                raise
            self.session.refresh(job)
            return job
        raise RuntimeError("could not reserve incremental materialization")

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
        now = datetime.now(UTC)
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
                    finished_at=now,
                    artifact_expires_at=(
                        now + timedelta(seconds=self.settings.historical_result_ttl_seconds)
                        if job.kind == JobKind.HISTORICAL_QUERY
                        else job.artifact_expires_at
                    ),
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
        self._release_incremental_reservation(job, now)
        self.session.commit()
        self.session.refresh(job)
        return job

    def retry(self, job_id: str) -> JobRecord:
        job = self.get(job_id)
        retryable_statuses = (JobStatus.FAILED, JobStatus.EXHAUSTED)
        retryable_cancelled = job.status == JobStatus.CANCELLED and (
            job.kind == JobKind.HISTORICAL_QUERY or self._is_incremental(job)
        )
        if job.status not in retryable_statuses and not retryable_cancelled:
            raise ValueError("only failed or exhausted jobs can be retried")
        now = datetime.now(UTC)
        expires_at = job.artifact_expires_at
        if job.artifacts_cleaned_at is not None or (
            expires_at is not None and _as_utc(expires_at) <= now
        ):
            raise ValueError("job artifacts have expired and the job cannot be retried")
        if self._is_incremental(job):
            state = self.session.scalar(
                select(MaterializationState)
                .where(MaterializationState.feature_view == job.payload["feature_view"])
                .with_for_update()
            )
            if state is None:
                state = MaterializationState(feature_view=job.payload["feature_view"])
                self.session.add(state)
                self.session.flush()
            if state.active_job_id not in (None, job.id):
                active = self.session.get(JobRecord, state.active_job_id)
                if active is not None and active.status in (
                    JobStatus.PENDING,
                    JobStatus.RETRYING,
                    JobStatus.RUNNING,
                ):
                    self.session.rollback()
                    raise ValueError(
                        f"another incremental materialization is active: {state.active_job_id}"
                    )
            state.active_job_id = job.id
            state.updated_at = now
        result = cast(
            CursorResult[Any],
            self.session.execute(
                update(JobRecord)
                .execution_options(synchronize_session=False)
                .where(
                    JobRecord.id == job_id,
                    JobRecord.status.in_(
                        (JobStatus.FAILED, JobStatus.EXHAUSTED, JobStatus.CANCELLED)
                    ),
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
                    artifact_expires_at=None,
                    artifacts_cleaned_at=None,
                    result_uri=None,
                    result_metadata=None,
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

    @staticmethod
    def _is_incremental(job: JobRecord) -> bool:
        return job.kind == JobKind.MATERIALIZE and job.payload.get("mode") == "incremental"

    def _release_incremental_reservation(self, job: JobRecord, now: datetime) -> None:
        if self._is_incremental(job):
            self.session.execute(
                update(MaterializationState)
                .where(
                    MaterializationState.feature_view == job.payload["feature_view"],
                    MaterializationState.active_job_id == job.id,
                )
                .values(active_job_id=None, updated_at=now)
            )


class JobExecutor:
    def __init__(
        self,
        session: Session,
        offline: OfflineStore | None = None,
        online: OnlineStore | None = None,
        artifacts: ArtifactStorage | None = None,
        *,
        worker_id: str | None = None,
        settings: Settings | None = None,
        now: Callable[[], datetime] | None = None,
        metrics: Metrics = METRICS,
    ):
        self.session = session
        self.settings = settings or get_settings()
        self.worker_id = worker_id or f"worker-{uuid.uuid4()}"
        self.now = now or (lambda: datetime.now(UTC))
        self.registry = Registry(session)
        self.offline = offline or OfflineStore()
        self.online = online or OnlineStore()
        self.artifacts = artifacts or ArtifactStorage(self.settings)
        self.metrics = metrics

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
                            artifact_expires_at=(
                                claimed_at
                                + timedelta(seconds=self.settings.historical_result_ttl_seconds)
                                if candidate.kind == JobKind.HISTORICAL_QUERY
                                else candidate.artifact_expires_at
                            ),
                            next_attempt_at=None,
                            worker_id=None,
                            lease_token=None,
                            lease_expires_at=None,
                            last_heartbeat_at=None,
                        )
                    ),
                )
                if result.rowcount and JobService._is_incremental(candidate):
                    JobService(self.session, self.settings)._release_incremental_reservation(
                        candidate, claimed_at
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
            self.metrics.job_claimed.labels(job.kind).inc()
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
        started = time.perf_counter()
        self._execute(job)
        with suppress(Exception):
            self.session.refresh(job)
        outcome = "lease_lost" if job.status == JobStatus.RUNNING else str(job.status)
        self.metrics.job_completed.labels(job.kind, outcome).inc()
        self.metrics.job_duration.labels(job.kind, outcome).observe(time.perf_counter() - started)

    def _execute(self, job: JobRecord) -> None:
        if not job.lease_token or not self._owns(job.id, job.lease_token):
            with suppress(Exception):
                self.session.refresh(job)
            return
        lease_token = job.lease_token
        cleanup_uri: str | None = None
        attempt_result_uri: str | None = None
        result_metadata: dict[str, Any] | None = None
        try:
            if job.kind == JobKind.BACKFILL:
                result_metadata = self._backfill(job, lease_token)
            elif job.kind == JobKind.MATERIALIZE:
                result_metadata = self._materialize(job, lease_token)
            elif job.kind == JobKind.OFFLINE_APPEND:
                cleanup_uri = self._offline_append(job, lease_token)
            elif job.kind == JobKind.HISTORICAL_QUERY:
                attempt_result_uri, result_metadata = self._historical_query(job, lease_token)
            else:
                raise ValueError(f"unsupported job kind: {job.kind}")
            self._finalize_success(
                job,
                lease_token,
                result_uri=attempt_result_uri,
                result_metadata=result_metadata,
            )
            attempt_result_uri = None
        except LeaseLostError:
            self.session.rollback()
            with suppress(Exception):
                self.session.refresh(job)
            return
        except Exception as exc:
            error = traceback.format_exc(limit=10)
            self.session.rollback()
            failure_result = (
                exc.summary.model_dump(mode="json") if isinstance(exc, DataQualityError) else None
            )
            self._finalize_failure(
                job, lease_token, exc, error, result_metadata=failure_result
            )
            return
        finally:
            if attempt_result_uri:
                with suppress(Exception):
                    self.artifacts.delete(attempt_result_uri)
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

    def _finalize_success(
        self,
        job: JobRecord,
        lease_token: str,
        *,
        result_uri: str | None = None,
        result_metadata: dict[str, Any] | None = None,
    ) -> None:
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
                    result_uri=result_uri,
                    result_metadata=result_metadata,
                    artifact_expires_at=(
                        now + timedelta(seconds=self.settings.historical_result_ttl_seconds)
                        if job.kind == JobKind.HISTORICAL_QUERY
                        else job.artifact_expires_at
                    ),
                )
            ),
        )
        if not result.rowcount:
            self.session.rollback()
            raise LeaseLostError(f"job lease was lost before success: {job.id}")
        if JobService._is_incremental(job):
            source_freshness = None
            if result_metadata and result_metadata.get("source_freshness_at"):
                source_freshness = datetime.fromisoformat(
                    str(result_metadata["source_freshness_at"])
                )
            state_result = cast(
                CursorResult[Any],
                self.session.execute(
                    update(MaterializationState)
                    .where(
                        MaterializationState.feature_view == job.payload["feature_view"],
                        MaterializationState.active_job_id == job.id,
                    )
                    .values(
                        watermark=datetime.fromisoformat(job.payload["end"]),
                        source_freshness_at=source_freshness,
                        active_job_id=None,
                        last_successful_job_id=job.id,
                        updated_at=now,
                    )
                ),
            )
            if not state_result.rowcount:
                self.session.rollback()
                raise LeaseLostError(f"incremental materialization reservation was lost: {job.id}")
        if job.kind == JobKind.OFFLINE_APPEND:
            self._mark_stream_events_applied(job)
        self.session.commit()
        self.session.refresh(job)

    def _finalize_failure(
        self,
        job: JobRecord,
        lease_token: str,
        exc: Exception,
        error: str,
        *,
        result_metadata: dict[str, Any] | None = None,
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
        artifact_expires_at = (
            now + timedelta(seconds=self.settings.historical_result_ttl_seconds)
            if job.kind == JobKind.HISTORICAL_QUERY and finished_at is not None
            else None
        )
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
                    artifact_expires_at=artifact_expires_at,
                    result_metadata=result_metadata,
                )
            ),
        )
        if not result.rowcount:
            self.session.rollback()
            with suppress(Exception):
                self.session.refresh(job)
            return
        if status in (JobStatus.FAILED, JobStatus.EXHAUSTED):
            JobService(self.session, self.settings)._release_incremental_reservation(job, now)
        self.session.commit()
        self.session.refresh(job)

    def _historical_query(self, job: JobRecord, lease_token: str) -> tuple[str, dict[str, Any]]:
        if not job.artifact_uri:
            raise ValueError("historical query job is missing its input artifact")
        payload = self.artifacts.read_json(job.artifact_uri)
        query = HistoricalQuery.model_validate(payload)
        resolved = list(job.payload["resolved_features"])
        self._require_ownership(job, lease_token)
        table = HistoricalRetriever(self.registry, self.offline, self.metrics).query_table(
            query.observations, resolved_features=resolved, mode="async"
        )
        self._require_ownership(job, lease_token)
        uri = self.artifacts.result_uri(job.id, job.attempt_count, lease_token)
        size = self.artifacts.write_parquet(uri, table)
        return uri, {
            "format": "parquet",
            "content_type": "application/vnd.apache.parquet",
            "row_count": table.num_rows,
            "byte_size": size,
            "resolved_features": resolved,
            "warnings": list(job.payload.get("warnings", [])),
        }

    def cleanup_expired_artifacts(self, *, now: datetime | None = None) -> int:
        cleaned_at = now or self.now()
        terminal = (
            JobStatus.SUCCEEDED,
            JobStatus.FAILED,
            JobStatus.EXHAUSTED,
            JobStatus.CANCELLED,
        )
        jobs = list(
            self.session.scalars(
                select(JobRecord)
                .where(
                    JobRecord.kind == JobKind.HISTORICAL_QUERY,
                    JobRecord.status.in_(terminal),
                    JobRecord.artifact_expires_at.is_not(None),
                    JobRecord.artifact_expires_at <= cleaned_at,
                    JobRecord.artifacts_cleaned_at.is_(None),
                )
                .with_for_update(skip_locked=True)
            )
        )
        count = 0
        for job in jobs:
            self.artifacts.delete_job(job.id)
            result = cast(
                CursorResult[Any],
                self.session.execute(
                    update(JobRecord)
                    .execution_options(synchronize_session=False)
                    .where(JobRecord.id == job.id, JobRecord.artifacts_cleaned_at.is_(None))
                    .values(artifacts_cleaned_at=cleaned_at)
                ),
            )
            count += int(result.rowcount or 0)
        self.session.commit()
        self.session.expire_all()
        return count

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

    @staticmethod
    def _backfill_checkpoint(cursor: str, summary: DataQualitySummary) -> str:
        return "backfill:" + json.dumps(
            {"cursor": cursor, "quality": summary.model_dump(mode="json")},
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _has_backfill_checkpoint(checkpoints: list[str], cursor: str) -> bool:
        if cursor in checkpoints:
            return True
        for checkpoint in checkpoints:
            if checkpoint.startswith("backfill:"):
                payload = json.loads(checkpoint.removeprefix("backfill:"))
                if payload.get("cursor") == cursor:
                    return True
        return False

    @staticmethod
    def _quality_summary(
        checkpoints: list[str], policy: QualityPolicy
    ) -> DataQualitySummary:
        for checkpoint in reversed(checkpoints):
            if checkpoint.startswith("quality:"):
                return DataQualitySummary.model_validate_json(checkpoint.removeprefix("quality:"))
            if checkpoint.startswith("backfill:"):
                payload = json.loads(checkpoint.removeprefix("backfill:"))
                return DataQualitySummary.model_validate(payload["quality"])
        return DataQualitySummary(
            policy=policy,
            evaluated_rows=0,
            valid_rows=0,
            invalid_rows=0,
            quarantined_rows=0,
            counts_by_constraint={},
        )

    def _observe_quality(
        self,
        view_ref: str,
        policy: QualityPolicy,
        validation: QualityValidation,
        *,
        path: str,
    ) -> None:
        for constraint, count in validation.counts_by_constraint.items():
            self.metrics.quality_violations.labels(
                view_ref, path, policy, constraint
            ).inc(count)

    def _quarantine_batch(
        self,
        job: JobRecord,
        view_ref: str,
        table: pa.Table,
        validation: QualityValidation,
        date: str,
    ) -> None:
        indexes = validation.invalid_row_indexes
        if not indexes:
            return
        errors_by_row: dict[int, list[str]] = {}
        for violation in validation.violations:
            errors_by_row.setdefault(violation.row_index, []).append(
                f"{violation.feature}:{violation.constraint}"
            )
        quarantined = table.take(pa.array(indexes, type=pa.int64()))
        quarantined = quarantined.append_column(
            "quality_errors", pa.array([errors_by_row[index] for index in indexes])
        )
        quarantined = quarantined.append_column(
            "quality_policy", pa.array([QualityPolicy.QUARANTINE] * len(indexes))
        )
        target = self.offline.quarantine_uri(view_ref, job.id)
        if self.offline.exists(target):
            self.offline.overwrite_partition(target, quarantined, f"event_date = '{date}'")
        else:
            self.offline.append(target, quarantined, partition_by="event_date")

    def _backfill(self, job: JobRecord, lease_token: str) -> dict[str, Any]:
        view = self.registry.feature_view(job.payload["feature_view"])
        source = self.registry.batch_source(view.batch_source)
        entity = self.registry.entity(view.entity)
        start = datetime.fromisoformat(job.payload["start"]).astimezone(UTC)
        end = datetime.fromisoformat(job.payload["end"]).astimezone(UTC)
        policy = view.effective_quality_policy
        summary = self._quality_summary(job.checkpoints, policy)
        cursor = start
        while cursor < end:
            self._require_ownership(job, lease_token)
            chunk_end = min(
                end, (cursor + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            )
            if chunk_end <= cursor:
                chunk_end = min(end, cursor + timedelta(days=1))
            checkpoint = cursor.isoformat()
            if not self._has_backfill_checkpoint(job.checkpoints, checkpoint):
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
                validation = validate_quality_table(
                    output, view.features, reference_time=chunk_end
                )
                self._observe_quality(view.ref, policy, validation, path="batch")
                invalid_indexes = validation.invalid_row_indexes
                valid_indexes = validation.valid_row_indexes
                counts = dict(summary.counts_by_constraint)
                for constraint, count in validation.counts_by_constraint.items():
                    counts[constraint] = counts.get(constraint, 0) + count
                next_summary = DataQualitySummary(
                    policy=policy,
                    evaluated_rows=summary.evaluated_rows + validation.evaluated_rows,
                    valid_rows=summary.valid_rows + len(valid_indexes),
                    invalid_rows=summary.invalid_rows + len(invalid_indexes),
                    quarantined_rows=(
                        summary.quarantined_rows
                        + (len(invalid_indexes) if policy == QualityPolicy.QUARANTINE else 0)
                    ),
                    counts_by_constraint=counts,
                )
                if invalid_indexes and policy == QualityPolicy.REJECT:
                    raise DataQualityError(validation.bounded_message(), next_summary)
                target = self.offline.view_uri(view.ref)
                date = cursor.date().isoformat()
                self._require_ownership(job, lease_token)
                if policy == QualityPolicy.QUARANTINE:
                    self._quarantine_batch(job, view.ref, output, validation, date)
                    output = output.take(pa.array(valid_indexes, type=pa.int64()))
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
                summary = next_summary
                self._commit_checkpoints(
                    job,
                    lease_token,
                    [*job.checkpoints, self._backfill_checkpoint(checkpoint, summary)],
                )
            cursor = chunk_end
        return summary.model_dump(mode="json")

    def _materialize(self, job: JobRecord, lease_token: str) -> dict[str, Any]:
        view = self.registry.feature_view(job.payload["feature_view"])
        entity = self.registry.entity(view.entity)
        mode = str(job.payload.get("mode", "explicit"))
        end = datetime.fromisoformat(job.payload["end"]).astimezone(UTC)
        lookback_seconds = int(job.payload.get("lookback_seconds", 0))
        state = self.session.get(MaterializationState, view.ref)
        watermark = _as_utc(state.watermark) if state and state.watermark else None
        if mode == "incremental":
            start = watermark - timedelta(seconds=lookback_seconds) if watermark else None
        else:
            start = datetime.fromisoformat(job.payload["start"]).astimezone(UTC)
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
        scanned_freshness = max(
            (_as_utc(value) for value in table["event_timestamp"].to_pylist()), default=None
        )
        prior_freshness = (
            _as_utc(state.source_freshness_at)
            if mode == "incremental" and state and state.source_freshness_at
            else None
        )
        source_freshness = max(
            (value for value in (prior_freshness, scanned_freshness) if value is not None),
            default=None,
        )
        feature_names = [feature.name for feature in view.features]
        updated = 0
        for row in latest.to_pylist():
            self._require_ownership(job, lease_token)
            updated += int(
                self.online.upsert(
                    StreamFeatureEvent(
                        event_id=row["event_id"],
                        feature_view=view.ref,
                        entity_values={key: row[key] for key in entity.join_keys},
                        event_timestamp=row["event_timestamp"],
                        values={name: row[name] for name in feature_names},
                    )
                )
            )
        self._commit_checkpoints(job, lease_token, [f"materialized:{latest.num_rows}"])
        resulting_watermark = end if mode == "incremental" else watermark
        return {
            "mode": mode,
            "effective_start": start.isoformat() if start else None,
            "effective_end": end.isoformat(),
            "lookback_seconds": lookback_seconds,
            "scanned_rows": table.num_rows,
            "candidate_entities": latest.num_rows,
            "updated_entities": updated,
            "skipped_entities": latest.num_rows - updated,
            "source_freshness_at": source_freshness.isoformat() if source_freshness else None,
            "freshness_lag_seconds": (
                max(0.0, (end - source_freshness).total_seconds()) if source_freshness else None
            ),
            "resulting_watermark": (
                resulting_watermark.isoformat() if resulting_watermark else None
            ),
        }

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
