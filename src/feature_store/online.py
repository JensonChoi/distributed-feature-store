from __future__ import annotations

import hashlib
import json
import time
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from redis import Redis

from feature_store.config import Settings, get_settings
from feature_store.models import QueryResponse, StreamFeatureEvent
from feature_store.observability import METRICS, Metrics
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
    def __init__(
        self,
        client: Redis | None = None,
        settings: Settings | None = None,
        metrics: Metrics = METRICS,
    ):
        self.settings = settings or get_settings()
        self.client = client or Redis.from_url(self.settings.redis_url, decode_responses=True)
        self.metrics = metrics

    def upsert(self, event: StreamFeatureEvent) -> bool:
        started = time.perf_counter()
        timestamp = event.event_timestamp.astimezone(UTC).isoformat(timespec="microseconds")
        args: list[str] = [timestamp, event.event_id]
        for name, value in sorted(event.values.items()):
            args.extend((name, json.dumps(value, separators=(",", ":"), default=str)))
        try:
            result = bool(
                self.client.eval(
                    UPSERT_SCRIPT, 1, entity_key(event.feature_view, event.entity_values), *args
                )
            )
        except Exception:
            self.metrics.online_requests.labels("upsert", "error").inc()
            self.metrics.online_updates.labels(event.feature_view, "error").inc()
            raise
        finally:
            self.metrics.online_duration.labels("upsert").observe(time.perf_counter() - started)
        outcome = "accepted" if result else "skipped"
        self.metrics.online_requests.labels("upsert", "success").inc()
        self.metrics.online_updates.labels(event.feature_view, outcome).inc()
        return result

    def read(
        self,
        registry: Registry,
        entities: list[dict[str, Any]],
        features: list[str],
        feature_service: str | None = None,
    ) -> QueryResponse:
        started = time.perf_counter()
        try:
            response = self._read(registry, entities, features, feature_service)
        except Exception:
            self.metrics.online_requests.labels("read", "error").inc()
            raise
        finally:
            self.metrics.online_duration.labels("read").observe(time.perf_counter() - started)
        self.metrics.online_requests.labels("read", "success").inc()
        return response

    def _read(
        self,
        registry: Registry,
        entities: list[dict[str, Any]],
        features: list[str],
        feature_service: str | None,
    ) -> QueryResponse:
        resolved = registry.resolve_features(features, feature_service)
        warnings = registry.warnings_for_query(features, feature_service)
        if not entities:
            return QueryResponse(resolved_features=resolved, rows=[], warnings=warnings)
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
                result = "present" if stored else "missing"
                row[f"{view.name}__status"] = result
                self.metrics.online_entity_results.labels(view_ref, result).inc()
                raw_timestamp = stored.get("__event_timestamp")
                if raw_timestamp is not None:
                    if isinstance(raw_timestamp, bytes):
                        raw_timestamp = raw_timestamp.decode()
                    event_timestamp = datetime.fromisoformat(raw_timestamp).astimezone(UTC)
                    self.metrics.online_served_age.labels(view_ref).observe(
                        max(0.0, (datetime.now(UTC) - event_timestamp).total_seconds())
                    )
                for name in names:
                    raw = stored.get(name)
                    row[f"{view.name}__{name}"] = json.loads(raw) if raw is not None else None
            rows.append(row)
        return QueryResponse(resolved_features=resolved, rows=rows, warnings=warnings)
