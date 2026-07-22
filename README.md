# Distributed Feature Store

A local-first feature store that keeps historical training data and low-latency serving data
consistent across batch and streaming workflows. It uses Delta Lake on MinIO, DuckDB for
point-in-time joins, Redis for online reads, Redpanda for streaming updates, and Postgres for
the registry and durable jobs.

## What it demonstrates

- Immutable, semantic feature versions and pinned feature services.
- Point-in-time correct historical joins with TTL handling and deterministic ties.
- Resumable daily backfills and explicit online materialization.
- Idempotent, out-of-order-safe online updates and durable offline stream ingestion.
- One typed contract across the Python SDK, CLI, and REST API.

## Quickstart

Requirements: Docker with Compose and `uv` for local development.

```bash
cp .env.example .env
docker compose up --build -d
uv sync --extra dev
uv run feature-store demo
uv run feature-store jobs
# After the backfill succeeds:
uv run feature-store demo-stream
```

The API documentation is available at <http://localhost:8000/docs>, MinIO at
<http://localhost:9001>, and metrics at <http://localhost:8000/metrics>.

## Data flow

```text
batch source ──> job worker ──> Delta/Parquet on MinIO ──> DuckDB PIT join
                                      │
                                      └──> materialize ──> Redis

feature producer ──> Redpanda ──> stream consumer ──> Redis
                                         │
                                         └──> durable staging job ──> Delta

SDK / CLI ──> FastAPI ──> Postgres registry + jobs
```

Feature views have exact `name@major.minor.patch` identities. Historical queries and feature
services must pin exact feature references such as
`account_transaction_features@1.0.0:txn_count_1h`. Applying a changed definition under an
existing identity returns a conflict.

## Point-in-time semantics

For every observation and feature view, retrieval selects the row with the greatest feature
event timestamp less than or equal to the observation timestamp. Equal timestamps are ordered
by stable event ID. A row older than the view TTL is reported as `expired`; no prior row is
reported as `missing`. Feature values in either case are null.

All timestamps at system boundaries must include a timezone and are normalized to UTC. Backfill
ranges are start-inclusive and end-exclusive.

## Development

```bash
uv run ruff check .
uv run mypy src
uv run pytest
uv run feature-store-benchmark --iterations 200
```

The Docker integration test profile requires a Docker daemon. Core correctness tests use local
SQLite and Delta tables and do not require infrastructure.

## Deliberate boundaries

This portfolio MVP is single-tenant and has no authentication or web dashboard. DuckDB is the
only batch engine. Batch transformations are SQL queries over a provided `source` relation;
streaming producers send already-computed feature rows. The storage interfaces are intentionally
small so Spark and cloud object-store adapters can be added later.
