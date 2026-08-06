from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, text

from feature_store.config import get_settings
from feature_store.db import make_engine


def test_stream_event_migration_upgrades_and_downgrades_sqlite(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    database_path = tmp_path / "migration.db"
    database_url = f"sqlite:///{database_path}"
    monkeypatch.setenv("FS_DATABASE_URL", database_url)
    get_settings.cache_clear()
    config = Config("alembic.ini")

    command.upgrade(config, "head")
    engine = make_engine(database_url)
    assert "stream_events" in inspect(engine).get_table_names()

    command.downgrade(config, "0001")
    assert "stream_events" not in inspect(engine).get_table_names()
    assert {"jobs", "registry_records"} <= set(inspect(engine).get_table_names())
    engine.dispose()
    get_settings.cache_clear()


def test_job_lease_migration_preserves_existing_jobs_and_downgrades_sqlite(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    database_path = tmp_path / "job-leases.db"
    database_url = f"sqlite:///{database_path}"
    monkeypatch.setenv("FS_DATABASE_URL", database_url)
    get_settings.cache_clear()
    config = Config("alembic.ini")

    command.upgrade(config, "0002")
    engine = make_engine(database_url)
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO jobs "
                "(id, kind, status, payload, checkpoints, created_at) "
                "VALUES "
                "('existing', 'backfill', 'pending', '{}', '[]', "
                "'2025-01-01 00:00:00')"
            )
        )

    command.upgrade(config, "0003")
    columns = {column["name"] for column in inspect(engine).get_columns("jobs")}
    assert {
        "attempt_count",
        "max_attempts",
        "next_attempt_at",
        "worker_id",
        "lease_token",
        "lease_expires_at",
        "last_heartbeat_at",
        "failure_kind",
    } <= columns
    with engine.connect() as connection:
        row = connection.execute(
            text("SELECT attempt_count, max_attempts FROM jobs WHERE id = 'existing'")
        ).one()
    assert row == (0, 3)

    command.downgrade(config, "0002")
    columns = {column["name"] for column in inspect(engine).get_columns("jobs")}
    assert "attempt_count" not in columns
    assert "failure_kind" not in columns
    assert "stream_events" in inspect(engine).get_table_names()
    engine.dispose()
    get_settings.cache_clear()


def test_historical_artifact_migration_preserves_jobs_and_downgrades_sqlite(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    database_path = tmp_path / "historical-artifacts.db"
    database_url = f"sqlite:///{database_path}"
    monkeypatch.setenv("FS_DATABASE_URL", database_url)
    get_settings.cache_clear()
    config = Config("alembic.ini")
    command.upgrade(config, "0003")
    engine = make_engine(database_url)
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO jobs (id, kind, status, payload, checkpoints, created_at, "
                "attempt_count, max_attempts) VALUES "
                "('existing', 'backfill', 'pending', '{}', '[]', "
                "'2025-01-01 00:00:00', 0, 3)"
            )
        )

    command.upgrade(config, "0004")
    columns = {column["name"] for column in inspect(engine).get_columns("jobs")}
    assert {
        "artifact_uri",
        "result_uri",
        "result_metadata",
        "artifact_expires_at",
        "artifacts_cleaned_at",
    } <= columns
    with engine.connect() as connection:
        existing = connection.execute(text("SELECT id FROM jobs WHERE id = 'existing'")).scalar()
    assert existing == "existing"

    command.downgrade(config, "0003")
    columns = {column["name"] for column in inspect(engine).get_columns("jobs")}
    assert "artifact_uri" not in columns
    assert "attempt_count" in columns
    engine.dispose()
    get_settings.cache_clear()


def test_incremental_materialization_migration_upgrades_and_downgrades_sqlite(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    database_path = tmp_path / "incremental-materialization.db"
    database_url = f"sqlite:///{database_path}"
    monkeypatch.setenv("FS_DATABASE_URL", database_url)
    get_settings.cache_clear()
    config = Config("alembic.ini")
    command.upgrade(config, "0004")
    engine = make_engine(database_url)
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO jobs (id, kind, status, payload, checkpoints, created_at, "
                "attempt_count, max_attempts) VALUES "
                "('existing', 'materialize', 'succeeded', '{}', '[]', "
                "'2025-01-01 00:00:00', 1, 3)"
            )
        )

    command.upgrade(config, "0005")
    assert "materialization_states" in inspect(engine).get_table_names()
    with engine.connect() as connection:
        assert connection.execute(text("SELECT id FROM jobs WHERE id='existing'")).scalar() == (
            "existing"
        )
    command.downgrade(config, "0004")
    assert "materialization_states" not in inspect(engine).get_table_names()
    assert "jobs" in inspect(engine).get_table_names()
    engine.dispose()
    get_settings.cache_clear()


def test_registry_lifecycle_migration_preserves_records_and_downgrades_sqlite(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    database_path = tmp_path / "registry-lifecycle.db"
    database_url = f"sqlite:///{database_path}"
    monkeypatch.setenv("FS_DATABASE_URL", database_url)
    get_settings.cache_clear()
    config = Config("alembic.ini")
    command.upgrade(config, "0005")
    engine = make_engine(database_url)
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO registry_records "
                "(id, kind, name, version, fingerprint, spec, created_at) VALUES "
                "('record-1', 'entity', 'account', '', 'abc', "
                '\'{"name": "account", "join_keys": {"account_id": "string"}}\', '
                "'2025-01-01 00:00:00')"
            )
        )

    command.upgrade(config, "0006")
    columns = {column["name"] for column in inspect(engine).get_columns("registry_lifecycle")}
    assert {
        "registry_record_id",
        "feature_name",
        "owners",
        "tags",
        "documentation_links",
        "status",
        "deprecated_at",
        "deprecation_message",
        "replacement",
        "updated_at",
    } <= columns
    with engine.connect() as connection:
        assert (
            connection.execute(text("SELECT id FROM registry_records WHERE id='record-1'")).scalar()
            == "record-1"
        )

    command.downgrade(config, "0005")
    assert "registry_lifecycle" not in inspect(engine).get_table_names()
    assert "registry_records" in inspect(engine).get_table_names()
    engine.dispose()
    get_settings.cache_clear()
