from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy.orm import Session

from feature_store.models import RegistryManifest, StreamFeatureEvent
from feature_store.online import OnlineStore, entity_key
from feature_store.registry import Registry


class MemoryRedis:
    def __init__(self) -> None:
        self.data: dict[str, dict[str, str]] = {}

    def eval(self, _: str, __: int, key: str, *args: str) -> int:
        stored = self.data.setdefault(key, {})
        old = (stored.get("__event_timestamp", ""), stored.get("__event_id", ""))
        new = (args[0], args[1])
        if old[0] and old >= new:
            return 0
        stored["__event_timestamp"] = args[0]
        stored["__event_id"] = args[1]
        for index in range(2, len(args), 2):
            stored[args[index]] = args[index + 1]
        return 1

    def hgetall(self, key: str) -> dict[str, str]:
        return self.data.get(key, {})


def _event(timestamp: datetime, event_id: str, amount: float) -> StreamFeatureEvent:
    return StreamFeatureEvent(
        event_id=event_id,
        feature_view="account_stats@1.0.0",
        entity_values={"account_id": "a"},
        event_timestamp=timestamp,
        values={"amount": amount},
    )


def test_online_store_rejects_older_events_and_reads_latest(
    session: Session, manifest: RegistryManifest
) -> None:
    registry = Registry(session)
    registry.apply(manifest)
    client = MemoryRedis()
    store = OnlineStore(client=client)  # type: ignore[arg-type]
    now = datetime.now(UTC)
    assert store.upsert(_event(now, "b", 20.0))
    assert not store.upsert(_event(now - timedelta(seconds=1), "z", 99.0))
    assert not store.upsert(_event(now, "a", 30.0))
    response = store.read(registry, [{"account_id": "a"}], ["account_stats@1.0.0:amount"])
    assert response.rows[0]["account_stats__amount"] == 20.0
    assert response.rows[0]["account_stats__status"] == "present"


def test_entity_key_is_order_independent() -> None:
    assert entity_key("view@1.0.0", {"a": 1, "b": 2}) == entity_key("view@1.0.0", {"b": 2, "a": 1})
