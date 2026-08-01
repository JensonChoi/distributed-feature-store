from __future__ import annotations

import json
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
