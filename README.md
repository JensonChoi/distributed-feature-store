# Distributed Feature Store

A local-first feature store that keeps historical training data and low-latency serving data
consistent across batch and streaming workflows. It uses Delta Lake on MinIO, DuckDB for
point-in-time joins, Redis for online reads, Redpanda for streaming updates, and Postgres for
the registry and durable jobs.

## What it demonstrates

- Immutable, semantic feature versions and pinned feature services.
- Point-in-time correct historical joins with TTL handling and deterministic ties.
- Leased, resumable jobs with heartbeats, fenced completion, and bounded retries.
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

## Benchmark results

This snapshot was recorded on 2026-07-27 using a MacBook Air 9,1 with a 1.1 GHz quad-core
Intel Core i5 and 16 GB RAM, running macOS 15.7.7 and Docker Compose v5.3.1. Five sequential
runs each measured 200 online requests and one 1,000-row historical query.

| Run | Online p50 (ms) | Online p95 (ms) | Historical (s) | Historical (rows/s) |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 19.442 | 32.913 | 0.332 | 3,013.8 |
| 2 | 18.047 | 33.977 | 0.299 | 3,347.1 |
| 3 | 18.784 | 23.589 | 0.282 | 3,548.4 |
| 4 | 19.315 | 37.299 | 0.356 | 2,809.4 |
| 5 | 19.637 | 37.673 | 0.273 | 3,656.7 |

| Metric | Median | Min–max range |
| --- | ---: | ---: |
| Online p50 (ms) | 19.315 | 18.047–19.637 |
| Online p95 (ms) | 33.977 | 23.589–37.673 |
| Historical (s) | 0.299 | 0.273–0.356 |
| Historical (rows/s) | 3,347.1 | 2,809.4–3,656.7 |

Run the same benchmark with:

```bash
uv run feature-store-benchmark --iterations 200
```

These results are a machine-dependent snapshot, not enforced performance thresholds.

## Deliberate boundaries

This portfolio MVP is single-tenant and has no authentication or web dashboard. DuckDB is the
only batch engine. Batch transformations are SQL queries over a provided `source` relation;
streaming producers send already-computed feature rows. The storage interfaces are intentionally
small so Spark and cloud object-store adapters can be added later.
