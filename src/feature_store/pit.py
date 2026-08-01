from __future__ import annotations

from collections import defaultdict
from datetime import UTC
from typing import Any

import duckdb
import pyarrow as pa

from feature_store.models import Observation, QueryResponse
from feature_store.offline import OfflineStore
from feature_store.registry import Registry


def _ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


class HistoricalRetriever:
    def __init__(self, registry: Registry, offline: OfflineStore | None = None):
        self.registry = registry
        self.offline = offline or OfflineStore()

    def query(
        self,
        observations: list[Observation],
        features: list[str],
        feature_service: str | None = None,
    ) -> QueryResponse:
        resolved = self.validate(observations, features, feature_service)
        table = self.query_table(observations, resolved_features=resolved)
        return QueryResponse(resolved_features=resolved, rows=table.to_pylist())

    def validate(
        self,
        observations: list[Observation],
        features: list[str],
        feature_service: str | None = None,
    ) -> list[str]:
        resolved = self.registry.resolve_features(features, feature_service)
        for ref in resolved:
            view_ref, _ = ref.rsplit(":", 1)
            view = self.registry.feature_view(view_ref)
            entity = self.registry.entity(view.entity)
            for index, observation in enumerate(observations):
                missing = set(entity.join_keys) - set(observation.entity_values)
                if missing:
                    raise ValueError(
                        f"missing observation entity keys at row {index}: {sorted(missing)}"
                    )
        return resolved

    def query_table(
        self, observations: list[Observation], *, resolved_features: list[str]
    ) -> pa.Table:
        resolved = resolved_features
        if not observations:
            return pa.Table.from_pylist([])
        rows = []
        for index, observation in enumerate(observations):
            if observation.event_timestamp.tzinfo is None:
                raise ValueError("observation timestamps must include a timezone")
            rows.append(
                {
                    "_row_id": index,
                    **observation.entity_values,
                    "event_timestamp": observation.event_timestamp.astimezone(UTC),
                }
            )
        current = pa.Table.from_pylist(rows)
        grouped: dict[str, list[str]] = defaultdict(list)
        for ref in resolved:
            view_ref, feature_name = ref.rsplit(":", 1)
            grouped[view_ref].append(feature_name)

        for view_ref, feature_names in grouped.items():
            view = self.registry.feature_view(view_ref)
            entity = self.registry.entity(view.entity)
            missing = set(entity.join_keys) - set(current.column_names)
            if missing:
                raise ValueError(f"missing observation entity keys: {sorted(missing)}")
            uri = self.offline.view_uri(view_ref)
            if not self.offline.exists(uri):
                current = self._append_missing(current, view.name, feature_names)
                continue
            feature_rows = self.offline.load(uri)
            if feature_rows.num_rows == 0:
                current = self._append_missing(current, view.name, feature_names)
                continue
            current = self._join_view(
                current, feature_rows, view.name, feature_names, entity.join_keys, view.ttl_seconds
            )

        final = current.drop(["_row_id"])
        return final

    def _join_view(
        self,
        observations: pa.Table,
        feature_rows: pa.Table,
        view_name: str,
        feature_names: list[str],
        join_keys: dict[str, Any],
        ttl_seconds: int | None,
    ) -> pa.Table:
        connection = duckdb.connect()
        connection.execute("SET TimeZone='UTC'")
        connection.register("observations", observations)
        connection.register("feature_rows", feature_rows)
        join = " AND ".join(f"o.{_ident(key)} = f.{_ident(key)}" for key in join_keys)
        ttl_expired = (
            f"f.event_timestamp < o.event_timestamp - INTERVAL {int(ttl_seconds)} SECOND"
            if ttl_seconds
            else "FALSE"
        )
        status = (
            f"CASE WHEN f.event_timestamp IS NULL THEN 'missing' "
            f"WHEN {ttl_expired} THEN 'expired' ELSE 'present' END"
        )
        feature_columns = ", ".join(
            f"CASE WHEN f.event_timestamp IS NOT NULL AND NOT ({ttl_expired}) "
            f"THEN f.{_ident(name)} ELSE NULL END AS {_ident(f'{view_name}__{name}')}"
            for name in feature_names
        )
        query = f"""
            SELECT o.*, {status} AS {_ident(f"{view_name}__status")}, {feature_columns}
            FROM observations o
            LEFT JOIN feature_rows f
              ON {join} AND f.event_timestamp <= o.event_timestamp
            QUALIFY ROW_NUMBER() OVER (
              PARTITION BY o._row_id
              ORDER BY f.event_timestamp DESC NULLS LAST, f.event_id DESC NULLS LAST
            ) = 1
            ORDER BY o._row_id
        """
        try:
            return connection.sql(query).to_arrow_table()
        finally:
            connection.close()

    @staticmethod
    def _append_missing(table: pa.Table, view_name: str, feature_names: list[str]) -> pa.Table:
        result = table.append_column(f"{view_name}__status", pa.array(["missing"] * table.num_rows))
        for name in feature_names:
            result = result.append_column(f"{view_name}__{name}", pa.nulls(table.num_rows))
        return result
