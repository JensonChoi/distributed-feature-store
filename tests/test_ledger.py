from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy.orm import Session

from feature_store.ledger import RegistrationResult, StreamEventLedger
from feature_store.models import StreamEventState, StreamFeatureEvent


def event(
    *,
    feature_view: str = "account_stats@1.0.0",
    event_id: str = "event-1",
    account_id: str = "a",
    amount: float = 12.5,
) -> StreamFeatureEvent:
    return StreamFeatureEvent(
        event_id=event_id,
        feature_view=feature_view,
        entity_values={"account_id": account_id},
        event_timestamp=datetime(2025, 1, 1, tzinfo=UTC),
        values={"amount": amount},
    )


def test_ledger_registers_duplicates_conflicts_and_view_scoped_ids(session: Session) -> None:
    ledger = StreamEventLedger(session)
    created = ledger.register(
        event(), source_topic="features", source_partition=2, source_offset=10
    )
    assert created.result == RegistrationResult.NEW
    assert created.record.state == StreamEventState.PENDING
    assert created.record.payload["event_timestamp"] == "2025-01-01T00:00:00.000000+00:00"
    source = (
        created.record.source_topic,
        created.record.source_partition,
        created.record.source_offset,
    )
    assert source == (
        "features",
        2,
        10,
    )

    duplicate = ledger.register(
        event(), source_topic="features", source_partition=2, source_offset=11
    )
    assert duplicate.result == RegistrationResult.DUPLICATE
    assert duplicate.record.id == created.record.id

    conflict = ledger.register(
        event(account_id="different"), source_topic="features", source_partition=2, source_offset=12
    )
    assert conflict.result == RegistrationResult.CONFLICT
    assert conflict.record.id == created.record.id

    other_view = ledger.register(
        event(feature_view="other_stats@1.0.0"),
        source_topic="features",
        source_partition=2,
        source_offset=13,
    )
    assert other_view.result == RegistrationResult.NEW
    assert other_view.record.id != created.record.id
