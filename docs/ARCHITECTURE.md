# Architecture

The distributed feature store is a local-first, dual-store system. Postgres provides the
control plane, Delta Lake on MinIO provides historical storage, Redis provides low-latency
online storage, and Redpanda connects the streaming path.

```text
                       ┌──────────── Control plane ────────────┐
CLI / Python SDK ────> │ FastAPI ─────> Postgres               │
                       │                 • registry definitions │
                       │                 • durable jobs         │
                       └───────┬─────────────────┬──────────────┘
                               │                 │
                    historical/online reads     job requests
                               │                 │
        ┌──────────────────────┘                 ▼
        │                                 Background worker
        │                                  • backfill
        │                                  • materialize
        │                                  • offline append
        │
┌───────▼──────── Offline path ───────────────────────────────────┐
│ Batch source on MinIO                                          │
│       │                                                        │
│       ▼                                                        │
│ DuckDB SQL transformations over PyArrow                        │
│       │                                                        │
│       ▼                                                        │
│ Versioned Delta tables on MinIO ───> DuckDB point-in-time join │
└──────────────────────────┬─────────────────────────────────────┘
                           │ materialize latest row/entity
                           ▼
                    Redis online store
                           ▲
                           │ atomic latest-value upsert
┌──────────────────────────┴── Streaming path ───────────────────┐
│ Feature producer ──> Redpanda ──> Stream consumer              │
│                                      │                         │
│                                      ├── validate + Redis      │
│                                      ├── stage batch on MinIO  │
│                                      ├── enqueue offline append│
│                                      └── invalid events → DLQ  │
└────────────────────────────────────────────────────────────────┘
```

## Service layer

Clients use the CLI, Python SDK, or HTTP API. The FastAPI service exposes registry management,
online retrieval, historical retrieval, and durable job operations. It also provides liveness
and readiness checks, Prometheus metrics, and request IDs.

The API process is intentionally thin and delegates behavior to the registry, online store,
offline store, historical retriever, and job service. The relevant implementations are
[`api.py`](../src/feature_store/api.py) and [`sdk.py`](../src/feature_store/sdk.py).

## Registry and job control plane

Postgres stores two kinds of state:

- Immutable registry records for entities, batch sources, stream sources, feature views, and
  feature services.
- Durable backfill, materialization, and offline-append jobs, including status and checkpoints.

Feature views have semantic identities such as `account_transaction_features@1.0.0`. Applying
different content under an existing identity causes a conflict, while reapplying identical
content is idempotent. Feature services provide pinned contracts composed of exact feature
references such as `account_transaction_features@1.0.0:txn_count_1h`.

The worker polls Postgres, claims pending work with row locking, and requeues interrupted jobs
when it starts. Backfills are checkpointed in daily chunks. See
[`registry.py`](../src/feature_store/registry.py), [`jobs.py`](../src/feature_store/jobs.py), and
[`worker.py`](../src/feature_store/worker.py).

## Offline and batch path

Batch sources and computed feature views are Delta Lake tables stored in MinIO:

1. A backfill job reads the source range, including the feature view's TTL lookback.
2. DuckDB executes the feature view's SQL over a PyArrow `source` relation.
3. The worker validates required output columns and declared types.
4. Results are partitioned by event date and written to the versioned view path.
5. Replayed backfills replace affected partitions rather than duplicating their rows.

The offline storage adapter supports Delta reads, appends, partition replacement, and staging
cleanup. See [`offline.py`](../src/feature_store/offline.py).

## Historical retrieval

Historical queries resolve either explicit pinned features or a named feature service. DuckDB
joins observations to offline feature rows by:

- Matching entity keys.
- Requiring the feature event time to be at or before the observation time.
- Selecting the newest eligible event.
- Using event ID as the deterministic tie-breaker.
- Returning null feature values with `missing` or `expired` status when appropriate.

Timestamps must include a timezone and are normalized to UTC at system boundaries. Inline API
queries are limited to 10,000 observations. Retrieval currently runs synchronously inside the
API rather than through a distributed batch engine. See
[`pit.py`](../src/feature_store/pit.py).

## Online serving

Redis stores one hash per feature-view and entity-key combination. Each hash contains the latest
feature values together with the source event timestamp and event ID.

Updates use a Lua script so comparison and mutation are atomic. An event is ignored if Redis
already contains a newer timestamp, or an equal timestamp with a greater or equal event ID.
This provides replay safety and protects the online store from out-of-order delivery.

Online reads resolve the requested feature contract through the registry and retrieve the
latest hashes from Redis. See [`online.py`](../src/feature_store/online.py).

## Streaming path

The stream consumer subscribes to registry-defined Redpanda topics and:

1. Parses and validates each event against its pinned feature view.
2. Sends malformed or schema-invalid events to a dead-letter topic.
3. Atomically updates Redis.
4. Buffers accepted events by feature view.
5. Writes each batch to a unique staging Delta table in MinIO.
6. Creates a durable `offline_append` job in Postgres.
7. Commits Kafka offsets after staging and job creation.
8. Lets the worker append staging data to the permanent offline view and clean up staging.

This path keeps online features fresh while eventually adding the same events to historical
storage. See [`streaming.py`](../src/feature_store/streaming.py).

## Materialization

A materialization job bridges the offline and online stores. The worker reads a requested time
range from a feature view's Delta table, selects the latest row for each entity using event time
and event ID ordering, and upserts those rows into Redis through the same replay-safe online
store interface used by streaming.

## Local deployment

Docker Compose runs:

- The FastAPI service.
- One background worker.
- One streaming consumer.
- Postgres.
- Redis.
- MinIO and its bucket initializer.
- Redpanda.

The project therefore demonstrates distributed-system boundaries, durable handoffs, consistent
batch and streaming representations, and separate online and offline retrieval paths. The
current deployment remains an intentionally single-tenant, single-machine MVP rather than a
horizontally scaled production cluster. See [`compose.yaml`](../compose.yaml).
