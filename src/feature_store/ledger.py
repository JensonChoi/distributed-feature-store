from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from feature_store.db import StreamEventRecord
from feature_store.models import StreamEventState, StreamFeatureEvent


class RegistrationResult(StrEnum):
    NEW = "new"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"


@dataclass(frozen=True)
class Registration:
    result: RegistrationResult
    record: StreamEventRecord


def canonical_event_payload(event: StreamFeatureEvent) -> dict[str, Any]:
    payload = event.model_dump(mode="json")
    payload["event_timestamp"] = event.event_timestamp.astimezone(UTC).isoformat(
        timespec="microseconds"
    )
    return payload


def payload_fingerprint(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


class StreamEventLedger:
    def __init__(self, session: Session):
        self.session = session

    def register(
        self,
        event: StreamFeatureEvent,
        *,
        source_topic: str | None,
        source_partition: int | None,
        source_offset: int | None,
    ) -> Registration:
        payload = canonical_event_payload(event)
        fingerprint = payload_fingerprint(payload)
        existing = self.session.scalar(
            select(StreamEventRecord).where(
                StreamEventRecord.feature_view == event.feature_view,
                StreamEventRecord.event_id == event.event_id,
            )
        )
        if existing:
            result = (
                RegistrationResult.DUPLICATE
                if existing.fingerprint == fingerprint
                else RegistrationResult.CONFLICT
            )
            return Registration(result, existing)

        record = StreamEventRecord(
            feature_view=event.feature_view,
            event_id=event.event_id,
            fingerprint=fingerprint,
            payload=payload,
            state=StreamEventState.PENDING,
            source_topic=source_topic,
            source_partition=source_partition,
            source_offset=source_offset,
        )
        self.session.add(record)
        self.session.commit()
        self.session.refresh(record)
        return Registration(RegistrationResult.NEW, record)

    def pending(self) -> list[StreamEventRecord]:
        statement = (
            select(StreamEventRecord)
            .where(StreamEventRecord.state == StreamEventState.PENDING)
            .order_by(StreamEventRecord.created_at, StreamEventRecord.id)
        )
        return list(self.session.scalars(statement))

    @staticmethod
    def event(record: StreamEventRecord) -> StreamFeatureEvent:
        return StreamFeatureEvent.model_validate(record.payload)

    def mark_staged(self, records: list[StreamEventRecord], job_id: str) -> None:
        now = datetime.now(UTC)
        for record in records:
            record.state = StreamEventState.STAGED
            record.job_id = job_id
            record.staged_at = now
            record.updated_at = now
