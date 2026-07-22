from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import UTC
from typing import Any

from redis import Redis

from feature_store.config import Settings, get_settings
from feature_store.models import QueryResponse, StreamFeatureEvent
from feature_store.registry import Registry

UPSERT_SCRIPT = """
local old_ts = redis.call('HGET', KEYS[1], '__event_timestamp')
local old_id = redis.call('HGET', KEYS[1], '__event_id')
if old_ts and (old_ts > ARGV[1] or (old_ts == ARGV[1] and old_id >= ARGV[2])) then
  return 0
end
redis.call('HSET', KEYS[1], '__event_timestamp', ARGV[1], '__event_id', ARGV[2])
for i = 3, #ARGV, 2 do
  redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
end
return 1
"""


def entity_key(view_ref: str, entity_values: dict[str, Any]) -> str:
    canonical = json.dumps(entity_values, sort_keys=True, separators=(",", ":"), default=str)
    digest = hashlib.sha256(canonical.encode()).hexdigest()
    return f"fs:{view_ref}:{digest}"


class OnlineStore:
    def __init__(self, client: Redis | None = None, settings: Settings | None = None):
        self.settings = settings or get_settings()
        self.client = client or Redis.from_url(self.settings.redis_url, decode_responses=True)

    def upsert(self, event: StreamFeatureEvent) -> bool:
        timestamp = event.event_timestamp.astimezone(UTC).isoformat(timespec="microseconds")
        args: list[str] = [timestamp, event.event_id]
        for name, value in sorted(event.values.items()):
            args.extend((name, json.dumps(value, separators=(",", ":"), default=str)))
        result = self.client.eval(
            UPSERT_SCRIPT, 1, entity_key(event.feature_view, event.entity_values), *args
        )
        return bool(result)

    def read(
        self,
        registry: Registry,
        entities: list[dict[str, Any]],
        features: list[str],
        feature_service: str | None = None,
    ) -> QueryResponse:
        resolved = registry.resolve_features(features, feature_service)
        if not entities:
            return QueryResponse(resolved_features=resolved, rows=[])
        grouped: dict[str, list[str]] = defaultdict(list)
        for ref in resolved:
            view_ref, feature_name = ref.rsplit(":", 1)
            grouped[view_ref].append(feature_name)

        rows: list[dict[str, Any]] = []
        for values in entities:
            row: dict[str, Any] = dict(values)
            for view_ref, names in grouped.items():
                view = registry.feature_view(view_ref)
                entity = registry.entity(view.entity)
                missing_keys = set(entity.join_keys) - set(values)
                if missing_keys:
                    raise ValueError(f"missing entity keys for {view_ref}: {sorted(missing_keys)}")
                key_values = {key: values[key] for key in entity.join_keys}
                stored = self.client.hgetall(entity_key(view_ref, key_values))
                row[f"{view.name}__status"] = "present" if stored else "missing"
                for name in names:
                    raw = stored.get(name)
                    row[f"{view.name}__{name}"] = json.loads(raw) if raw is not None else None
            rows.append(row)
        return QueryResponse(resolved_features=resolved, rows=rows)
