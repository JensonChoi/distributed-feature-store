from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import inspect

from feature_store.config import get_settings
from feature_store.db import make_engine


def test_stream_event_migration_upgrades_and_downgrades_sqlite(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
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
