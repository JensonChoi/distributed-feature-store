from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
from sqlalchemy.orm import Session

from feature_store.models import Observation, RegistryManifest
from feature_store.offline import OfflineStore
from feature_store.pit import HistoricalRetriever
from feature_store.registry import Registry


class LocalOfflineStore(OfflineStore):
    def __init__(self, root: Path):
        super().__init__()
        self.root = root

    def view_uri(self, view_ref: str) -> str:
        return str(self.root / view_ref.replace("@", "_"))


def test_point_in_time_join_blocks_future_and_applies_ttl(
    tmp_path: Path, session: Session, manifest: RegistryManifest
) -> None:
    registry = Registry(session)
    registry.apply(manifest)
    offline = LocalOfflineStore(tmp_path)
    base = datetime(2025, 1, 1, 10, tzinfo=UTC)
    table = pa.Table.from_pylist(
        [
            {
                "account_id": "a",
                "event_timestamp": base - timedelta(hours=1),
                "event_id": "event-1",
                "amount": 10.0,
                "event_date": "2025-01-01",
            },
            {
                "account_id": "a",
                "event_timestamp": base + timedelta(hours=1),
                "event_id": "event-2",
                "amount": 99.0,
                "event_date": "2025-01-01",
            },
        ]
    )
    offline.append(offline.view_uri("account_stats@1.0.0"), table, "event_date")
    result = HistoricalRetriever(registry, offline).query(
        [
            Observation(entity_values={"account_id": "a"}, event_timestamp=base),
            Observation(
                entity_values={"account_id": "a"},
                event_timestamp=base + timedelta(hours=2, minutes=1),
            ),
            Observation(entity_values={"account_id": "missing"}, event_timestamp=base),
        ],
        ["account_stats@1.0.0:amount"],
    )
    assert result.rows[0]["account_stats__amount"] == 10.0
    assert result.rows[0]["account_stats__status"] == "present"
    assert result.rows[1]["account_stats__amount"] is None
    assert result.rows[1]["account_stats__status"] == "expired"
    assert result.rows[2]["account_stats__status"] == "missing"


def test_point_in_time_tie_uses_largest_event_id(
    tmp_path: Path, session: Session, manifest: RegistryManifest
) -> None:
    registry = Registry(session)
    registry.apply(manifest)
    offline = LocalOfflineStore(tmp_path)
    timestamp = datetime(2025, 1, 1, 10, tzinfo=UTC)
    table = pa.Table.from_pylist(
        [
            {
                "account_id": "a",
                "event_timestamp": timestamp,
                "event_id": "a",
                "amount": 1.0,
                "event_date": "2025-01-01",
            },
            {
                "account_id": "a",
                "event_timestamp": timestamp,
                "event_id": "b",
                "amount": 2.0,
                "event_date": "2025-01-01",
            },
        ]
    )
    offline.append(offline.view_uri("account_stats@1.0.0"), table, "event_date")
    result = HistoricalRetriever(registry, offline).query(
        [Observation(entity_values={"account_id": "a"}, event_timestamp=timestamp)],
        ["account_stats@1.0.0:amount"],
    )
    assert result.rows[0]["account_stats__amount"] == 2.0
