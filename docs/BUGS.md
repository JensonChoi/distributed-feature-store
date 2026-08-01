# Known Bugs

This document records confirmed, unresolved defects supported by current source or test
evidence. Entries describe behavior that violates the intended reliability or API contract;
roadmap enhancements and unimplemented feature requests belong in [ROADMAP.md](ROADMAP.md),
not here.

## Service startup does not upgrade existing databases

| Field | Value |
| --- | --- |
| Severity | High |
| Status | Open |
| Affected components | API, worker, stream consumer, database schema management |

**Observed behavior.** Every service calls `init_db()` at startup, and `init_db()` uses
SQLAlchemy's `Base.metadata.create_all()`. `create_all()` creates missing tables but does not
apply changes to tables that already exist. The repository has Alembic migrations, but service
startup does not run them.

**Impact.** A deployment that reuses a database created by an older release can start with a
stale schema. Later queries can then fail when application models reference columns introduced
by a migration, such as the lease and retry columns added in migration `0003`.

**Reproduction and evidence.** [`init_db()`](../src/feature_store/db.py#L102-L103) calls
`create_all()`, and the [API](../src/feature_store/api.py#L46-L50),
[worker](../src/feature_store/worker.py#L40-L44), and
[stream consumer](../src/feature_store/streaming.py#L267-L271) call it during startup. The
[migration tests](../tests/test_migrations.py) invoke Alembic explicitly, which proves the
migrations themselves work but does not cover service-driven upgrades.

**Recommended fix direction.** Make deployment or service startup run `alembic upgrade head`
before accepting work, and reserve `create_all()` for isolated tests or brand-new disposable
databases. Add an integration test that starts a service against a database at an older
revision and verifies that it reaches the current revision without losing data.

## Some invalid stream messages can terminate the consumer

| Field | Value |
| --- | --- |
| Severity | High |
| Status | Open |
| Affected components | Kafka stream consumer, registry validation, dead-letter handling |

**Observed behavior.** Stream handling catches `ValueError`, Pydantic `ValidationError`, and
`JSONDecodeError`, but an unknown feature view raises `RegistryNotFoundError`, a `KeyError`, so
it escapes the handler and terminates the consumer loop. Invalid UTF-8 is decoded before
Pydantic schema validation. Its `UnicodeDecodeError` is a `ValueError` and enters the
dead-letter branch, but that branch is not guarded; a producer or DLQ lookup failure can escape
and terminate the consumer while processing the malformed payload.

**Impact.** A poison message can stop a partition's processing until the consumer restarts. On
restart, the same uncommitted unknown-view message can cause a crash loop. Invalid UTF-8 also
depends on the separate DLQ durability weakness below instead of a fully contained validation
path.

**Reproduction and evidence.** [`StreamConsumer._handle()`](../src/feature_store/streaming.py#L81-L149)
looks up the view inside its narrow exception boundary, while
[`RegistryNotFoundError`](../src/feature_store/registry.py#L23-L28) derives from `KeyError`.
`json.loads()` receives the raw message bytes before model validation, and `_dead_letter()` is
called from the exception handler without its own failure boundary. Existing
[streaming tests](../tests/test_streaming.py) cover valid, duplicate, replayed, and conflicting
events, but not unknown views, invalid UTF-8, or failures while dead-lettering them.

**Recommended fix direction.** Treat registry lookup failures and byte-decoding failures as
explicit validation outcomes, make poison-message handling non-fatal, and add regression tests
for unknown views and invalid UTF-8 with both successful and failed DLQ delivery.

## Source offsets can be committed before DLQ delivery is confirmed

| Field | Value |
| --- | --- |
| Severity | High |
| Status | Open |
| Affected components | Kafka stream consumer, dead-letter producer, offset management |

**Observed behavior.** `_dead_letter()` queues a record and calls `Producer.flush(10)`, but it
ignores the return value and has no delivery callback. `_handle()` then synchronously commits
the source message offset. A timed-out or failed delivery can therefore be followed by a
successful source commit.

**Impact.** The source event will not be replayed and may never reach the DLQ, causing silent,
irrecoverable message loss and removing the evidence needed to diagnose the rejected event.

**Reproduction and evidence.** The [DLQ implementation](../src/feature_store/streaming.py#L174-L188)
does not inspect the `flush()` result; the conflict and validation branches
[commit immediately afterward](../src/feature_store/streaming.py#L120-L149). The test
[`test_conflicting_duplicate_is_dead_lettered_after_earlier_message_is_flushed`](../tests/test_streaming.py#L181)
uses a fake producer whose `flush()` cannot report a delivery failure.

**Recommended fix direction.** Wait for and verify a delivery report before committing the
source offset. On timeout or delivery error, leave the source offset uncommitted and surface a
retryable failure. Add tests for callback errors and nonzero `flush()` results.

## SQLite-backed job responses contain naïve timestamps

| Field | Value |
| --- | --- |
| Severity | Medium |
| Status | Open |
| Affected components | SQLite persistence, job service, HTTP job endpoints |

**Observed behavior.** Job columns request timezone-aware SQLAlchemy `DateTime` values, but
SQLite does not preserve timezone information. Loaded job records therefore contain naïve
`datetime` objects, and `serialize_job()` returns them unchanged to FastAPI.

**Impact.** Job response timestamps can omit a UTC offset even though the project contract
requires timezone-aware timestamps normalized to UTC. Clients may interpret them in local
time, reject them, or compare them incorrectly with offset-aware values.

**Reproduction and evidence.** The job [timestamp columns](../src/feature_store/db.py#L46-L63)
use `DateTime(timezone=True)`, and [`serialize_job()`](../src/feature_store/jobs.py#L41-L59)
passes loaded values through without restoring UTC. Existing job tests themselves sometimes
need to call [`replace(tzinfo=UTC)`](../tests/test_jobs.py#L398-L403) on SQLite-loaded retry
timestamps, demonstrating the lost timezone metadata without asserting the HTTP representation.

**Recommended fix direction.** Normalize persisted datetimes at the database boundary with a
UTC-aware type decorator or normalize every serialized job timestamp to an aware UTC value.
Add an API regression test using SQLite that requires an explicit UTC offset in every non-null
job timestamp.

## Invalid lease, heartbeat, and retry timings are accepted

| Field | Value |
| --- | --- |
| Severity | Medium |
| Status | Open |
| Affected components | Configuration, worker heartbeats, job leases, retry scheduling |

**Observed behavior.** `Settings` declares lease, heartbeat, maximum-attempt, and retry timing
fields as unconstrained numeric types. It accepts zero or negative durations and inconsistent
relationships such as a heartbeat interval greater than or equal to the lease duration or a
retry base greater than its maximum.

**Impact.** Invalid values can create immediately expired leases, tight heartbeat loops,
disabled or pathological retry behavior, and unnecessary duplicate execution. These failures
appear at runtime instead of producing a clear startup configuration error.

**Reproduction and evidence.** The fields in [`Settings`](../src/feature_store/config.py#L19-L27)
have no bounds or cross-field validator. The worker passes the heartbeat value directly to
[`Event.wait()`](../src/feature_store/worker.py#L18-L24), lease expiry is calculated directly
from `job_lease_seconds`, and retry delay is calculated directly from the base and maximum in
[`JobExecutor`](../src/feature_store/jobs.py#L455-L463). Tests exercise valid retry values but
do not reject invalid configurations.

**Recommended fix direction.** Add positive numeric constraints and cross-field validation:
the heartbeat must be shorter than the lease, retry base must not exceed retry maximum, and the
attempt count must be positive. Fail settings construction with an actionable validation error
and add boundary tests for environment-provided values.

## Verification baseline

The baseline suite succeeds despite these gaps: **41 tests pass, Ruff passes, and strict mypy
passes**. The findings above are therefore known coverage and lifecycle gaps rather than
currently failing baseline checks. This documentation change does not alter their **Open**
status or implement any fix.
