from __future__ import annotations

import json
import traceback
from contextlib import suppress
from datetime import UTC, date, datetime, timedelta
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from feature_store.db import JobRecord, StreamEventRecord
from feature_store.models import (
    Feature,
    JobKind,
    JobRequest,
    JobStatus,
    StreamEventState,
    StreamFeatureEvent,
    ValueType,
)
from feature_store.offline import OfflineStore, normalize_uri
from feature_store.online import OnlineStore
from feature_store.registry import Registry


def serialize_job(job: JobRecord) -> dict[str, Any]:
    return {
        "id": job.id,
        "kind": job.kind,
        "status": job.status,
        "payload": job.payload,
        "checkpoints": job.checkpoints,
        "error": job.error,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
    }


class JobService:
    def __init__(self, session: Session):
        self.session = session

    def create(self, kind: JobKind, request: JobRequest) -> JobRecord:
        payload = request.model_dump(mode="json")
        job = JobRecord(kind=kind, status=JobStatus.PENDING, payload=payload, checkpoints=[])
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
        if job.status not in (JobStatus.PENDING, JobStatus.RUNNING):
            raise ValueError("only pending or running jobs can be cancelled")
        job.status = JobStatus.CANCELLED
        job.finished_at = datetime.now(UTC)
        self.session.commit()
        return job

    def retry(self, job_id: str) -> JobRecord:
        job = self.get(job_id)
        if job.status != JobStatus.FAILED:
            raise ValueError("only failed jobs can be retried")
        job.status = JobStatus.PENDING
        job.error = None
        job.started_at = None
        job.finished_at = None
        self.session.commit()
        return job


class JobExecutor:
    def __init__(
        self,
        session: Session,
        offline: OfflineStore | None = None,
        online: OnlineStore | None = None,
    ):
        self.session = session
        self.registry = Registry(session)
        self.offline = offline or OfflineStore()
        self.online = online or OnlineStore()

    def recover_interrupted(self) -> int:
        """Requeue work owned by a prior instance of the single local worker."""
        interrupted = list(
            self.session.scalars(select(JobRecord.id).where(JobRecord.status == JobStatus.RUNNING))
        )
        if not interrupted:
            return 0
        self.session.execute(
            update(JobRecord)
            .where(JobRecord.id.in_(interrupted))
            .values(status=JobStatus.PENDING, started_at=None)
        )
        self.session.commit()
        return len(interrupted)

    def claim_next(self) -> JobRecord | None:
        statement = (
            select(JobRecord)
            .where(JobRecord.status == JobStatus.PENDING)
            .order_by(JobRecord.created_at)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        job = self.session.scalar(statement)
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.now(UTC)
            self.session.commit()
        return job

    def execute(self, job: JobRecord) -> None:
        cleanup_uri: str | None = None
        try:
            if job.kind == JobKind.BACKFILL:
                self._backfill(job)
            elif job.kind == JobKind.MATERIALIZE:
                self._materialize(job)
            elif job.kind == JobKind.OFFLINE_APPEND:
                cleanup_uri = self._offline_append(job)
            else:
                raise ValueError(f"unsupported job kind: {job.kind}")
            if job.status != JobStatus.CANCELLED:
                job.status = JobStatus.SUCCEEDED
                job.finished_at = datetime.now(UTC)
                if job.kind == JobKind.OFFLINE_APPEND:
                    self._mark_stream_events_applied(job)
                self.session.commit()
        except Exception:
            job.status = JobStatus.FAILED
            job.error = traceback.format_exc(limit=10)
            job.finished_at = datetime.now(UTC)
            self.session.commit()
            return
        if cleanup_uri:
            with suppress(Exception):
                self.offline.delete(cleanup_uri)

    def _backfill(self, job: JobRecord) -> None:
        view = self.registry.feature_view(job.payload["feature_view"])
        source = self.registry.batch_source(view.batch_source)
        entity = self.registry.entity(view.entity)
        start = datetime.fromisoformat(job.payload["start"]).astimezone(UTC)
        end = datetime.fromisoformat(job.payload["end"]).astimezone(UTC)
        cursor = start
        while cursor < end:
            self.session.refresh(job)
            if job.status == JobStatus.CANCELLED:
                return
            chunk_end = min(
                end, (cursor + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            )
            if chunk_end <= cursor:
                chunk_end = min(end, cursor + timedelta(days=1))
            checkpoint = cursor.isoformat()
            if checkpoint not in job.checkpoints:
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
                job.checkpoints = [*job.checkpoints, checkpoint]
                self.session.commit()
            cursor = chunk_end

    def _materialize(self, job: JobRecord) -> None:
        view = self.registry.feature_view(job.payload["feature_view"])
        entity = self.registry.entity(view.entity)
        start = datetime.fromisoformat(job.payload["start"]).astimezone(UTC)
        end = datetime.fromisoformat(job.payload["end"]).astimezone(UTC)
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
            self.online.upsert(
                StreamFeatureEvent(
                    event_id=row["event_id"],
                    feature_view=view.ref,
                    entity_values={key: row[key] for key in entity.join_keys},
                    event_timestamp=row["event_timestamp"],
                    values={name: row[name] for name in feature_names},
                )
            )
        job.checkpoints = [f"materialized:{latest.num_rows}"]
        self.session.commit()

    def _offline_append(self, job: JobRecord) -> str:
        staging = self.offline.load(job.payload["staging_uri"])
        target = self.offline.view_uri(job.payload["feature_view"])
        unseen = self._unseen_rows(staging, target)
        if unseen.num_rows:
            self.offline.append(target, unseen, partition_by="event_date")
        job.checkpoints = [
            f"appended:{unseen.num_rows}",
            f"duplicates:{staging.num_rows - unseen.num_rows}",
        ]
        return str(job.payload["staging_uri"])

    def _mark_stream_events_applied(self, job: JobRecord) -> None:
        now = datetime.now(UTC)
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
                    raise ValueError(
                        f"event_id {event_id} conflicts with existing offline content"
                    )
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
