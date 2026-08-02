# Distributed Feature Store

A local-first feature store that keeps historical training data and low-latency serving data
consistent across batch and streaming workflows. It uses Delta Lake on MinIO, DuckDB for
point-in-time joins, Redis for online reads, Redpanda for streaming updates, and Postgres for
the registry and durable jobs.

## What it demonstrates

- Immutable, semantic feature versions and pinned feature services.
- Point-in-time correct historical joins with TTL handling and deterministic ties.
- Inline and durable historical retrieval with streamed Parquet results.
- Leased, resumable jobs with heartbeats, fenced completion, and bounded retries.
- Watermark-driven incremental materialization with late-arrival lookback and coalescing.
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
<http://localhost:9001>, Prometheus at <http://localhost:9090>, and the provisioned Grafana
dashboard at <http://localhost:3000> (local admin credentials: `admin`/`admin`). Metrics are
served by the API at `:8000/metrics`, the worker at `:9101/metrics`, and the stream consumer at
`:9102/metrics`.

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

Historical requests containing at most `FS_INLINE_QUERY_LIMIT` observations return rows inline.
Larger requests return a `historical_query` job after their feature selectors and entity keys
have been validated. Poll the job and download its Parquet result when it succeeds:

```bash
uv run feature-store historical-read observations.json \
  -f account_transaction_features@1.0.0:txn_count_1h
uv run feature-store job JOB_ID
uv run feature-store job-result JOB_ID training-data.parquet
```

The download is streamed through the API; raw MinIO locations are not exposed. Query inputs and
results are retained for `FS_HISTORICAL_RESULT_TTL_SECONDS` (24 hours by default). The worker
deletes expired artifact prefixes every `FS_ARTIFACT_CLEANUP_INTERVAL_SECONDS`; durable job and
result metadata remain available after cleanup. Failed or exhausted jobs may be retried only
before their artifacts expire.

## Point-in-time semantics

For every observation and feature view, retrieval selects the row with the greatest feature
event timestamp less than or equal to the observation timestamp. Equal timestamps are ordered
by stable event ID. A row older than the view TTL is reported as `expired`; no prior row is
reported as `missing`. Feature values in either case are null.

All timestamps at system boundaries must include a timezone and are normalized to UTC. Backfill
ranges are start-inclusive and end-exclusive.

## Incremental materialization

An external scheduler can refresh a pinned feature view without calculating a start range:

```bash
uv run feature-store materialize-incremental account_transaction_features@1.0.0
```

The first successful run scans all offline history before its fixed submission-time cutoff.
Later runs scan from the last successful watermark minus the configured lookback through the
new cutoff. `FS_MATERIALIZATION_LOOKBACK_SECONDS` defaults to `3600`; use
`--lookback-seconds` for a per-run override and `--end` for a fixed UTC cutoff. Overlapping
submissions for the same pinned version coalesce onto one active job. For example, cron can
invoke the command every five minutes:

```cron
*/5 * * * * cd /srv/feature-store && uv run feature-store materialize-incremental account_transaction_features@1.0.0
```

The service does not run an internal recurring scheduler. Explicit range materialization
remains available through `feature-store materialize FEATURE_VIEW START END`.

## Freshness and serving metrics

Prometheus measures online reads/upserts, historical inline/async queries, jobs, materialization
freshness, and stream processing. Served-age histograms use the persisted feature event time:
online age is measured against wall-clock read time, while historical age is measured between
the observation and selected point-in-time row. Materialization exports both watermark age and
freshest-source-event age. Queue and ledger gauges include depth and oldest-record age.

Labels are intentionally bounded: exact pinned `feature_view` references, job kind/status,
operation, and fixed outcomes/reasons. Entity/event/job IDs, source paths, offsets, and exception
text are never metric labels. Useful local queries include:

```promql
sum(rate(feature_store_online_entity_results_total{result="missing"}[5m]))
  / sum(rate(feature_store_online_entity_results_total[5m]))
histogram_quantile(0.95,
  sum by (le, feature_view) (rate(feature_store_online_served_value_age_seconds_bucket[5m])))
max by (status) (feature_store_job_queue_oldest_age_seconds)
```

The alert rules in `monitoring/prometheus/alerts.yml` are local examples only. Their thresholds
must be tuned to production traffic, feature SLAs, and retry behavior. Alertmanager is not
configured and these rules send no notifications.

## Development

```bash
uv run ruff check .
uv run mypy src
uv run pytest
uv run feature-store-benchmark --list-scenarios
```

The Docker integration test profile requires a Docker daemon. Core correctness tests use local
SQLite and Delta tables and do not require infrastructure.

## Representative load testing

The built-in benchmark suite targets the checked-in fraud example. Prepare it before running:

```bash
uv run feature-store demo
# Wait until the backfill job reports "succeeded", then populate Redis for all four accounts:
uv run feature-store materialize-incremental account_transaction_features@1.0.0 \
  --end 2025-01-04T00:00:00Z
# Wait until the materialization job reports "succeeded", then run the suite:
uv run feature-store-benchmark
```

Keep `FS_INLINE_QUERY_LIMIT` at least `1000`. The suite expects the seeded 72-hour fraud
dataset, its four accounts, the four pinned `account_transaction_features@1.0.0` features, a
completed backfill, and materialized online values. Readiness and argument failures stop the run
immediately.

Six scenarios exercise distinct payload and concurrency shapes:

| Scenario | Request shape | Features | Concurrency |
| --- | ---: | ---: | ---: |
| `online-small` | 1 entity | 1 | 1 |
| `online-batch` | 4 entities | 4 | 1 |
| `online-concurrent` | 4 entities | 4 | 8 |
| `historical-small` | 100 observations | 1 | 1 |
| `historical-wide` | 1,000 observations | 4 | 1 |
| `historical-concurrent` | 250 observations | 4 | 4 |

### Benchmark snapshot

This snapshot was recorded on 2026-08-02 using a MacBook Air 9,1 with a 1.1 GHz quad-core
Intel Core i5 and 16 GB RAM, running macOS 15.7.7 and Docker Compose v5.3.1. One default suite
used three client-cold samples and a 10-second warm phase per scenario. All measured requests
succeeded.

| Scenario | Cold p50 (ms) | Warm succeeded/attempted | Warm requests/s | Warm entities or rows/s | Warm p50 (ms) | Warm p95 (ms) | Warm p99 (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `online-small` | 63.745 | 143/143 | 14.279 | 14.279 | 54.923 | 152.475 | 228.528 |
| `online-batch` | 75.261 | 89/89 | 8.837 | 35.347 | 86.912 | 293.181 | 334.580 |
| `online-concurrent` | 91.827 | 52/52 | 4.818 | 19.270 | 1,575.589 | 2,317.115 | 2,485.617 |
| `historical-small` | 809.548 | 12/12 | 1.108 | 110.764 | 812.389 | 1,988.768 | 1,988.768 |
| `historical-wide` | 1,126.398 | 14/14 | 1.354 | 1,353.956 | 693.827 | 1,320.508 | 1,320.508 |
| `historical-concurrent` | 260.472 | 33/33 | 3.049 | 762.345 | 1,224.183 | 1,705.353 | 1,720.126 |

Each scenario takes three client-cold samples, then runs warm for 10 seconds by default. Cold
means a new HTTP client and connection for every request. It does **not** flush Redis, restart a
service, clear a server-side cache, or change stored data. Every warm worker owns one persistent
async client, primes it once outside the measurements, and sends requests until the shared
deadline.

Use repeatable `--scenario` options to run a subset. `--iterations` remains available as an
optional cap on measured warm requests per scenario; it is useful for bounded tests and does
not replace the duration deadline. Other controls are `--duration-seconds`,
`--cold-iterations`, `--list-scenarios`, and `--output`:

```bash
uv run feature-store-benchmark --list-scenarios
uv run feature-store-benchmark --scenario online-concurrent \
  --scenario historical-concurrent --duration-seconds 20 --output benchmark.json
uv run feature-store-benchmark --iterations 200
```

Versioned JSON is always printed to stdout and is also written to `--output` when supplied. It
contains run and platform metadata, dataset assumptions, each scenario's explicit shape, and
separate cold/warm measurements. Measurements report attempted, succeeded, and failed request
counts; bounded error categories; error rate; elapsed time; successful requests per second;
successful entities or rows per second; and nearest-rank p50, p95, and p99 latency. HTTP 200 is
the only success. In particular, a historical HTTP 202 is an unexpected response because the
suite measures inline retrieval.

Individual request failures are recorded without aborting the report. The command exits nonzero
after writing the report only when a selected warm scenario has no successful requests; it does
not enforce latency, throughput, or error-rate thresholds. Results are informational and vary
with the machine, container resources, and competing load.

## Deliberate boundaries

This portfolio MVP is single-tenant and has no authentication or web dashboard. DuckDB is the
only batch engine. Batch transformations are SQL queries over a provided `source` relation;
streaming producers send already-computed feature rows. The storage interfaces are intentionally
small so Spark and cloud object-store adapters can be added later.
