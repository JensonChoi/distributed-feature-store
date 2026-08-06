# Roadmap

This roadmap is a set of candidate investments, not a release commitment. The order favors
correctness and operability before horizontal scale or a broader product surface.

## Guiding principles

- Preserve point-in-time correctness and batch/stream consistency.
- Make retries, replays, and recovery safe before increasing throughput.
- Measure end-to-end behavior under representative workloads.
- Keep service entry points thin and storage or execution engines replaceable.
- Add operational complexity only when its failure modes can be tested locally.

## Delivered

### End-to-end streaming deduplication

Streaming events now have a durable identity scoped to `(feature_view, event_id)`. Postgres
retains the canonical payload and its pending, staged, or applied lifecycle. Exact Kafka
replays do not repeat Redis or Delta mutations, while conflicting reuse of an identity is sent
to the dead-letter topic. Pending events recover on consumer startup, staging and job creation
share one database transaction, and offline appends compare event IDs before writing so worker
retries remain idempotent. Equal-timestamp events with distinct IDs remain separate historical
rows and continue to use the larger event ID as the serving tie-breaker.

Ledger retention and compaction remain future operational work.

### Job leases, heartbeats, and bounded retries

Workers now claim jobs through expiring ownership leases and renew them from a separate
database session. Lease tokens fence checkpoints, completion, failure, cancellation, and
stream-ledger finalization so an expired worker cannot overwrite a new owner. Retryable
failures use bounded exponential backoff and a snapshotted three-attempt budget, while
deterministic failures terminate immediately and expired final attempts become `exhausted`.
Operators can cancel queued, retrying, or running work and manually reset either failed or
exhausted jobs.

### Asynchronous historical retrieval

Historical requests at or below the configured inline limit still return rows immediately.
Larger validated requests are staged in object storage and run as durable jobs. Each attempt
writes a unique Parquet object, and the lease token fences publication so a stale worker cannot
replace the downloadable result. The API streams completed Parquet files without buffering the
whole result.

Inputs and results expire together after the configured retention period. Workers delete the
complete job prefix during periodic, idempotent cleanup sweeps while retaining job metadata.
Failed, exhausted, and cancelled inputs remain retryable until they expire.

### Incremental materialization

Pinned feature-view versions now keep independent successful cutoff watermarks and source
freshness. External schedulers can submit a cutoff-only command; overlapping submissions
coalesce, first runs bootstrap all history, and later runs push a watermark/lookback range into
Delta. Lease-fenced completion advances state, while replay-safe Redis writes produce typed
updated, skipped, scan, entity, and freshness summaries. Persisted recurring schedules remain
part of the later scheduled-jobs work.

### Feature freshness and serving metrics

Prometheus instrumentation now covers online and historical serving latency, outcomes,
missing/expired results, and the age of values actually served. Worker metrics expose job
attempts, queue state, materialization watermark/source freshness, and stream-ledger backlog;
the stream consumer reports processing, staging, online writes, ingestion lag, and categorized
dead letters. Compose includes a provisioned Grafana operator dashboard and local example alert
rules. Labels are restricted to pinned registry references and bounded enumerations.

### Representative load testing

The fraud example now has six reproducible online and historical scenarios spanning small and
wide payloads, concurrent clients, and sustained warm phases. Each worker primes and reuses its
own HTTP client, while separate client-cold samples create a new connection per request without
mutating backend state. Versioned JSON declares dataset and payload assumptions and reports
nearest-rank p50/p95/p99 latency, successful request and entity-or-row throughput, error rates,
and bounded failure categories. Results remain informational and machine-dependent.

### Registry plan, diff, and validation

Registry manifests can now be validated and planned against stored objects without writes.
Reports deterministically classify proposed objects as created, unchanged, or rejected,
aggregate missing-reference and immutable-conflict issues, and include recursive field-level
diffs. The API and typed SDK expose the workflow, while nested registry CLI commands print JSON
and return a failing exit status for rejected plans. Apply uses the same preflight before its
atomic commit, and the original top-level apply and list commands remain compatibility aliases.

## Next: developer and operator experience

### Registry lifecycle metadata

Support owners, tags, documentation links, creation provenance, deprecation status, and
replacement references without making versioned definitions mutable. Allow deprecated features
to remain readable while warning new consumers.

### Data quality contracts

Let feature definitions declare nullability, numeric ranges, accepted categories, uniqueness,
and freshness expectations. Validate batch outputs and streaming events consistently, with a
configurable choice to reject, quarantine, or report invalid data.

### Scheduled jobs

Add persisted schedules for backfills, materializations, compaction, retention, and staging
cleanup. Scheduling should create ordinary durable jobs so execution and recovery continue to
use one mechanism.

### Better job operations

Expose paginated filtering by status, kind, feature view, and creation time. Add structured
progress, per-partition timing, retry history, cancellation reasons, and links to relevant logs
or metrics.

### Feature discovery UI

Build a small read-only web interface for browsing entities, sources, feature views, versions,
feature services, lineage, freshness, and recent jobs. Mutation can remain API- and CLI-first
until authentication and audit behavior are defined.

### Generated typed clients

Generate or maintain strongly typed clients from the API contract and add async Python support.
Keep feature references and response-column naming consistent across the CLI, SDK, and HTTP API.

## Later: scale and extensibility

### Pluggable batch execution

Retain DuckDB for local and interactive work while adding an execution interface for engines
such as Spark. A backfill plan should choose an engine without changing registry or retrieval
contracts.

Questions to resolve:

- Which transformations form the portable subset?
- How are engine-specific SQL differences represented?
- Where do distributed job IDs, logs, cancellation, and retries live?

### Additional object-store adapters

Support production S3-compatible storage first, followed by explicitly tested cloud adapters.
Cover credentials, server-side encryption, region and endpoint configuration, multipart I/O,
and safe Delta commit behavior.

### Additional online stores

Extract a formal online-store interface and add at least one alternative backend. The contract
must preserve atomic timestamp and event-ID ordering, batched reads, serialization rules, and
missing-value behavior.

### Batched online retrieval

Use pipelined or multi-key reads for requests containing many entities or feature views. Add
request limits and payload-size accounting, then measure the tradeoff between batching,
serialization cost, and tail latency.

### Horizontally scalable services

Validate multiple API replicas, workers, and stream consumers. Partition work by feature view or
source where useful, remove single-process assumptions, and test failover during active
backfills and stream ingestion.

### Offline table maintenance

Add compaction, retention, vacuum, and checkpoint policies for Delta tables. Maintenance must
respect active readers, historical reproducibility, staged writes, and recovery windows.

### Streaming transformations

Allow selected transformations to run inside the streaming path instead of requiring producers
to send fully computed feature rows. Define how stateful windows, late data, watermarks, and
batch/stream transformation parity are represented and tested.

## Product and governance candidates

### Authentication, authorization, and tenancy

Introduce service identities, tenant or project boundaries, and role-based permissions for
registry mutation, retrieval, job control, and administration. Include audit records and
credential rotation before describing the service as multi-tenant.

### Lineage and impact analysis

Record dependencies from sources through feature views and feature services. Provide queries
such as "which models use this feature?" and "which services are affected by deprecating this
version?"

### Feature quality monitoring

Compare online and offline values, detect freshness violations, and track distribution changes,
null rates, and schema drift. Monitoring should distinguish pipeline defects from genuine data
changes and avoid silently changing feature values.

### Backup and disaster recovery

Document and test coordinated backup and restore of Postgres, Redis, Delta tables, and stream
positions. Define recovery point and recovery time objectives before adding automated disaster
recovery workflows.

### Secrets and deployment hardening

Replace local development credentials with secret-provider integrations, add TLS between
services, run containers as non-root users, pin production image versions, and publish
deployment guidance for resource limits and network policies.

## Explicit non-goals for now

- Claiming production readiness from local benchmark results.
- Adding a complex orchestration layer before durable job recovery is mature.
- Supporting unpinned feature references in retrieval contracts.
- Making existing registry identities mutable.
- Hiding batch and streaming consistency behavior behind best-effort updates.
