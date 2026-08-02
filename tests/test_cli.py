from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType

from typer.testing import CliRunner

from feature_store.cli import app


def test_job_result_command_streams_to_requested_output(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    output = tmp_path / "result.parquet"

    class FakeClient:
        def __enter__(self) -> FakeClient:
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            traceback: TracebackType | None,
        ) -> None:
            return None

        def download_job_result(self, job_id: str, path: Path) -> Path:
            assert job_id == "job-1"
            path.write_bytes(b"parquet")
            return path

    monkeypatch.setattr("feature_store.cli.FeatureStoreClient", FakeClient)
    result = CliRunner().invoke(app, ["job-result", "job-1", str(output)])
    assert result.exit_code == 0, result.output
    assert output.read_bytes() == b"parquet"
    assert json.loads(result.output) == {"job_id": "job-1", "output": str(output)}


def test_materialize_incremental_command_forwards_optional_window(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    captured: dict[str, object] = {}

    class FakeClient:
        def __enter__(self) -> FakeClient:
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def materialize_incremental(
            self,
            feature_view: str,
            *,
            end: datetime | None = None,
            lookback_seconds: int | None = None,
        ) -> dict[str, str]:
            captured.update(
                feature_view=feature_view,
                end=end,
                lookback_seconds=lookback_seconds,
            )
            return {"id": "job-1"}

    monkeypatch.setattr("feature_store.cli.FeatureStoreClient", FakeClient)
    result = CliRunner().invoke(
        app,
        [
            "materialize-incremental",
            "account_stats@1.0.0",
            "--end",
            "2025-01-02T00:00:00Z",
            "--lookback-seconds",
            "120",
        ],
    )
    assert result.exit_code == 0, result.output
    assert captured == {
        "feature_view": "account_stats@1.0.0",
        "end": datetime(2025, 1, 2, tzinfo=UTC),
        "lookback_seconds": 120,
    }
