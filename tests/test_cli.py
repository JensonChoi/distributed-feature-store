from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType

from typer.testing import CliRunner

from feature_store.cli import app
from feature_store.models import RegistryPlan, RegistryPlanSummary


def test_job_result_command_streams_to_requested_output(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
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


def test_nested_registry_commands_and_legacy_aliases(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    calls: list[str] = []
    path = tmp_path / "registry.yaml"
    path.write_text("entities: []\n")

    class FakeResult:
        summary = RegistryPlanSummary(created=0, unchanged=0, rejected=0)

        def model_dump(self, *, mode: str) -> dict[str, object]:
            assert mode == "json"
            return {"operation": calls[-1]}

    class FakeClient:
        def __enter__(self) -> FakeClient:
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def validate_file(self, _: Path) -> FakeResult:
            calls.append("validate")
            return FakeResult()

        def plan_file(self, _: Path) -> FakeResult:
            calls.append("plan")
            return FakeResult()

        def apply_file(self, _: Path) -> FakeResult:
            calls.append("apply")
            return FakeResult()

        def list_registry(self, kind: str | None) -> list[dict[str, str | None]]:
            calls.append(f"list:{kind}")
            return [{"kind": kind}]

    monkeypatch.setattr("feature_store.cli.FeatureStoreClient", FakeClient)
    runner = CliRunner()
    for arguments in (
        ["registry", "validate", str(path)],
        ["registry", "plan", str(path)],
        ["registry", "apply", str(path)],
        ["apply", str(path)],
        ["registry", "list", "--kind", "feature_view"],
        ["list", "--kind", "entity"],
    ):
        result = runner.invoke(app, arguments)
        assert result.exit_code == 0, result.output
        json.loads(result.output)
    assert calls == [
        "validate",
        "plan",
        "apply",
        "apply",
        "list:feature_view",
        "list:entity",
    ]


def test_registry_plan_exits_nonzero_for_rejections(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "registry.yaml"
    path.write_text("entities: []\n")

    class FakeClient:
        def __enter__(self) -> FakeClient:
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def plan_file(self, _: Path) -> RegistryPlan:
            return RegistryPlan(
                fingerprint="abc",
                summary=RegistryPlanSummary(created=0, unchanged=0, rejected=1),
                objects=[],
            )

    monkeypatch.setattr("feature_store.cli.FeatureStoreClient", FakeClient)
    result = CliRunner().invoke(app, ["registry", "plan", str(path)])
    assert result.exit_code == 1
    assert json.loads(result.output)["summary"]["rejected"] == 1


def test_registry_command_reports_invalid_manifest(tmp_path: Path) -> None:
    path = tmp_path / "registry.yaml"
    path.write_text("entities: [")
    result = CliRunner().invoke(app, ["registry", "validate", str(path)])
    assert result.exit_code == 2
    assert "error" in json.loads(result.output)


def test_registry_lifecycle_commands_forward_typed_targets_and_clear_flags(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    calls: list[tuple[str, object]] = []

    class Result:
        def model_dump(self, *, mode: str) -> dict[str, object]:
            assert mode == "json"
            return {"ok": True}

    class FakeClient:
        def __enter__(self) -> FakeClient:
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def describe_registry_object(self, target: object) -> Result:
            calls.append(("describe", target))
            return Result()

        def patch_registry_metadata(self, target: object, patch: object) -> Result:
            calls.append(("metadata", (target, patch)))
            return Result()

        def deprecate_registry_object(self, target: object, **kwargs: object) -> Result:
            calls.append(("deprecate", (target, kwargs)))
            return Result()

        def activate_registry_object(self, target: object) -> Result:
            calls.append(("activate", target))
            return Result()

    monkeypatch.setattr("feature_store.cli.FeatureStoreClient", FakeClient)
    runner = CliRunner()
    commands = [
        ["registry", "describe", "feature_view", "account_stats", "--version", "1.0.0"],
        [
            "registry",
            "metadata",
            "entity",
            "account",
            "--owner",
            "fraud-team",
            "--tag",
            "tier=critical",
            "--clear-documentation",
        ],
        ["registry", "deprecate", "entity", "account", "--message", "retiring"],
        ["registry", "activate", "entity", "account"],
    ]
    for command in commands:
        result = runner.invoke(app, command)
        assert result.exit_code == 0, result.output
        assert json.loads(result.output) == {"ok": True}
    assert [name for name, _ in calls] == ["describe", "metadata", "deprecate", "activate"]
    patch = calls[1][1][1]  # type: ignore[index]
    assert patch.owners == ["fraud-team"]
    assert patch.tags == {"tier": "critical"}
    assert patch.documentation_links == []


def test_registry_metadata_command_rejects_ambiguous_or_empty_updates() -> None:
    runner = CliRunner()
    empty = runner.invoke(app, ["registry", "metadata", "entity", "account"])
    assert empty.exit_code == 2
    ambiguous = runner.invoke(
        app,
        ["registry", "metadata", "entity", "account", "--owner", "team", "--clear-owners"],
    )
    assert ambiguous.exit_code == 2
