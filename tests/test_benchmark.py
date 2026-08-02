from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
import typer
from typer.testing import CliRunner

from feature_store import benchmark


def client_factory(
    handler: httpx.AsyncBaseTransport,
    created: list[httpx.AsyncClient] | None = None,
) -> benchmark.ClientFactory:
    def factory() -> httpx.AsyncClient:
        client = httpx.AsyncClient(transport=handler, base_url="http://benchmark.test")
        if created is not None:
            created.append(client)
        return client

    return factory


def test_nearest_rank_percentiles_and_empty_results() -> None:
    assert benchmark._nearest_rank([], 0.50) is None
    assert benchmark._nearest_rank([0.04, 0.01, 0.03, 0.02], 0.50) == 0.02
    assert benchmark._nearest_rank([float(value) for value in range(1, 101)], 0.95) == 95
    assert benchmark._nearest_rank([float(value) for value in range(1, 101)], 0.99) == 99

    empty = benchmark._measurement([], 2.0, 4, "entities")
    assert empty["attempted"] == 0
    assert empty["error_rate"] == 0.0
    assert empty["requests_per_second"] == 0.0
    assert empty["units_per_second"] == 0.0
    assert empty["latency_ms"] == {"p50": None, "p95": None, "p99": None}


def test_measurement_reports_throughput_units_and_errors() -> None:
    result = benchmark._measurement(
        [
            benchmark.Sample(0.01, None),
            benchmark.Sample(0.02, None),
            benchmark.Sample(0.03, "http_5xx"),
            benchmark.Sample(0.04, "timeout"),
        ],
        2.0,
        100,
        "rows",
    )

    assert result == {
        "attempted": 4,
        "succeeded": 2,
        "errors": 2,
        "error_counts": {
            "http_4xx": 0,
            "http_5xx": 1,
            "timeout": 1,
            "transport": 0,
            "unexpected_response": 0,
        },
        "error_rate": 0.5,
        "elapsed_seconds": 2.0,
        "requests_per_second": 1.0,
        "units": "rows",
        "units_per_second": 100.0,
        "latency_ms": {"p50": 10.0, "p95": 20.0, "p99": 20.0},
    }


@pytest.mark.parametrize(
    ("name", "payload_key", "unit_count", "feature_count"),
    [
        ("online-small", "entities", 1, 1),
        ("online-batch", "entities", 4, 4),
        ("online-concurrent", "entities", 4, 4),
        ("historical-small", "observations", 100, 1),
        ("historical-wide", "observations", 1000, 4),
        ("historical-concurrent", "observations", 250, 4),
    ],
)
def test_scenario_payload_matches_declared_shape(
    name: str, payload_key: str, unit_count: int, feature_count: int
) -> None:
    scenario = benchmark.SCENARIOS[name]
    payload = scenario.payload()

    assert len(payload[payload_key]) == unit_count
    assert len(payload["features"]) == feature_count
    count_key = "entity_count" if scenario.kind == "online" else "observation_count"
    assert scenario.shape()[count_key] == unit_count
    assert scenario.shape()["feature_count"] == feature_count


def test_cold_phase_creates_a_client_for_every_request() -> None:
    created: list[httpx.AsyncClient] = []
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json={"rows": []}))

    result = asyncio.run(
        benchmark._cold_phase(
            benchmark.SCENARIOS["online-small"],
            3,
            client_factory(transport, created),
        )
    )

    assert result["attempted"] == 3
    assert result["succeeded"] == 3
    assert len(created) == 3
    assert all(client.is_closed for client in created)


def test_warm_phase_uses_one_persistent_client_per_worker_and_request_cap() -> None:
    created: list[httpx.AsyncClient] = []
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"rows": []})

    scenario = benchmark.SCENARIOS["online-concurrent"]
    result = asyncio.run(
        benchmark._warm_phase(
            scenario,
            duration_seconds=10,
            iterations=11,
            client_factory=client_factory(httpx.MockTransport(handler), created),
        )
    )

    assert result["attempted"] == 11
    assert result["succeeded"] == 11
    assert calls == scenario.concurrency + 11  # one unmeasured prime per worker
    assert len(created) == scenario.concurrency
    assert all(client.is_closed for client in created)


@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        (httpx.Response(404), "http_4xx"),
        (httpx.Response(503), "http_5xx"),
        (httpx.Response(202), "unexpected_response"),
        (httpx.ReadTimeout("slow"), "timeout"),
        (httpx.ConnectError("offline"), "transport"),
    ],
)
def test_request_categorizes_failures(outcome: object, expected: str) -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        if isinstance(outcome, Exception):
            raise outcome
        assert isinstance(outcome, httpx.Response)
        return outcome

    async def request() -> benchmark.Sample:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url="http://benchmark.test"
        ) as client:
            return await benchmark._request(
                client,
                benchmark.SCENARIOS["historical-small"],
                benchmark.SCENARIOS["historical-small"].payload(),
            )

    assert asyncio.run(request()).error == expected


def test_run_suite_includes_metadata_and_continues_after_request_failures() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health/ready":
            return httpx.Response(200, json={"status": "ready"})
        return httpx.Response(500)

    report = asyncio.run(
        benchmark.run_suite(
            [benchmark.SCENARIOS["online-small"]],
            api_url="http://benchmark.test",
            duration_seconds=1,
            cold_iterations=2,
            iterations=2,
            client_factory=client_factory(httpx.MockTransport(handler)),
        )
    )

    assert report["schema_version"] == "1.0"
    assert report["dataset"]["feature_count"] == 4
    result = report["scenarios"][0]
    assert result["shape"]["entity_count"] == 1
    assert result["cold"]["error_counts"]["http_5xx"] == 2
    assert result["warm"]["attempted"] == 2
    assert result["warm"]["succeeded"] == 0


def test_run_suite_fails_fast_when_api_is_not_ready() -> None:
    transport = httpx.MockTransport(lambda _: httpx.Response(503))

    with pytest.raises(RuntimeError, match="readiness check returned HTTP 503"):
        asyncio.run(
            benchmark.run_suite(
                [benchmark.SCENARIOS["online-small"]],
                api_url="http://benchmark.test",
                duration_seconds=1,
                cold_iterations=1,
                iterations=1,
                client_factory=client_factory(transport),
            )
        )


def benchmark_app() -> typer.Typer:
    app = typer.Typer()
    app.command()(benchmark.run_benchmark)
    return app


def test_cli_lists_and_validates_scenarios_without_readiness_check(monkeypatch: Any) -> None:
    async def should_not_run(*_: object, **__: object) -> dict[str, Any]:
        raise AssertionError("listing must not run the suite")

    monkeypatch.setattr(benchmark, "run_suite", should_not_run)
    listed = CliRunner().invoke(benchmark_app(), ["--list-scenarios"])
    unknown = CliRunner().invoke(benchmark_app(), ["--scenario", "missing"])
    invalid_duration = CliRunner().invoke(benchmark_app(), ["--duration-seconds", "0"])
    invalid_iterations = CliRunner().invoke(benchmark_app(), ["--iterations", "0"])
    invalid_output = CliRunner().invoke(
        benchmark_app(), ["--output", "/directory/that/does/not/exist/result.json"]
    )

    assert listed.exit_code == 0
    assert "online-small" in listed.output
    assert "historical-concurrent" in listed.output
    assert unknown.exit_code != 0
    assert "unknown scenario" in unknown.output
    assert invalid_duration.exit_code != 0
    assert invalid_iterations.exit_code != 0
    assert invalid_output.exit_code != 0


def test_cli_filters_scenarios_prints_json_writes_output_and_supports_iterations(
    tmp_path: Path, monkeypatch: Any
) -> None:
    captured: dict[str, Any] = {}

    async def fake_run_suite(
        selected: list[benchmark.Scenario], **kwargs: Any
    ) -> dict[str, Any]:
        captured["selected"] = [item.name for item in selected]
        captured.update(kwargs)
        return {
            "schema_version": "1.0",
            "scenarios": [{"name": item.name, "warm": {"succeeded": 1}} for item in selected],
        }

    monkeypatch.setattr(benchmark, "run_suite", fake_run_suite)
    monkeypatch.setattr(benchmark, "get_settings", lambda: SimpleNamespace(api_url="http://api"))
    output = tmp_path / "benchmark.json"
    result = CliRunner().invoke(
        benchmark_app(),
        [
            "--scenario",
            "online-small",
            "--scenario",
            "historical-small",
            "--iterations",
            "7",
            "--cold-iterations",
            "2",
            "--duration-seconds",
            "0.5",
            "--output",
            str(output),
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == json.loads(output.read_text())
    assert captured["selected"] == ["online-small", "historical-small"]
    assert captured["iterations"] == 7
    assert captured["cold_iterations"] == 2
    assert captured["duration_seconds"] == 0.5


def test_cli_exits_nonzero_when_a_warm_scenario_has_no_successes(monkeypatch: Any) -> None:
    async def fake_run_suite(*_: object, **__: object) -> dict[str, Any]:
        return {
            "schema_version": "1.0",
            "scenarios": [{"name": "online-small", "warm": {"succeeded": 0}}],
        }

    monkeypatch.setattr(benchmark, "run_suite", fake_run_suite)
    monkeypatch.setattr(benchmark, "get_settings", lambda: SimpleNamespace(api_url="http://api"))
    result = CliRunner().invoke(
        benchmark_app(), ["--scenario", "online-small", "--iterations", "1"]
    )

    assert result.exit_code == 1
    assert json.loads(result.output)["scenarios"][0]["warm"]["succeeded"] == 0
