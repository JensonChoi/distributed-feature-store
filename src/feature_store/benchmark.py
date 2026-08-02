from __future__ import annotations

import asyncio
import json
import math
import platform
import sys
import time
from collections import Counter
from collections.abc import Callable
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

import httpx
import typer

from feature_store.config import get_settings

SCHEMA_VERSION = "1.0"
FEATURES = (
    "account_transaction_features@1.0.0:txn_count_1h",
    "account_transaction_features@1.0.0:amount_sum_24h",
    "account_transaction_features@1.0.0:country_mismatch",
    "account_transaction_features@1.0.0:account_age_days",
)
ACCOUNTS = ("acct_001", "acct_002", "acct_003", "acct_004")
OBSERVATION_TIMESTAMP = "2025-01-03T23:59:00+00:00"
ERROR_KINDS = ("http_4xx", "http_5xx", "timeout", "transport", "unexpected_response")


@dataclass(frozen=True)
class Scenario:
    name: str
    kind: str
    unit_count: int
    feature_count: int
    concurrency: int

    @property
    def endpoint(self) -> str:
        if self.kind == "online":
            return "/v1/online-features:read"
        return "/v1/historical-features:query"

    @property
    def unit_name(self) -> str:
        return "entities" if self.kind == "online" else "rows"

    def payload(self) -> dict[str, Any]:
        features = list(FEATURES[: self.feature_count])
        if self.kind == "online":
            return {
                "entities": [
                    {"account_id": ACCOUNTS[index % len(ACCOUNTS)]}
                    for index in range(self.unit_count)
                ],
                "features": features,
            }
        return {
            "observations": [
                {
                    "entity_values": {"account_id": ACCOUNTS[index % len(ACCOUNTS)]},
                    "event_timestamp": OBSERVATION_TIMESTAMP,
                }
                for index in range(self.unit_count)
            ],
            "features": features,
        }

    def shape(self) -> dict[str, Any]:
        count_key = "entity_count" if self.kind == "online" else "observation_count"
        return {
            "kind": self.kind,
            "endpoint": self.endpoint,
            count_key: self.unit_count,
            "feature_count": self.feature_count,
            "feature_refs": list(FEATURES[: self.feature_count]),
            "concurrency": self.concurrency,
        }


SCENARIOS = {
    scenario.name: scenario
    for scenario in (
        Scenario("online-small", "online", 1, 1, 1),
        Scenario("online-batch", "online", 4, 4, 1),
        Scenario("online-concurrent", "online", 4, 4, 8),
        Scenario("historical-small", "historical", 100, 1, 1),
        Scenario("historical-wide", "historical", 1000, 4, 1),
        Scenario("historical-concurrent", "historical", 250, 4, 4),
    )
}


@dataclass
class Sample:
    latency_seconds: float
    error: str | None


def _nearest_rank(samples: list[float], percentile: float) -> float | None:
    if not samples:
        return None
    ordered = sorted(samples)
    return ordered[max(0, math.ceil(percentile * len(ordered)) - 1)]


def _measurement(
    samples: list[Sample], elapsed_seconds: float, unit_count: int, unit_name: str
) -> dict[str, Any]:
    succeeded = [sample.latency_seconds for sample in samples if sample.error is None]
    errors = Counter(sample.error for sample in samples if sample.error is not None)
    attempted = len(samples)
    succeeded_count = len(succeeded)
    safe_elapsed = max(elapsed_seconds, sys.float_info.epsilon)

    def milliseconds(percentile: float) -> float | None:
        value = _nearest_rank(succeeded, percentile)
        return None if value is None else round(value * 1000, 3)

    return {
        "attempted": attempted,
        "succeeded": succeeded_count,
        "errors": attempted - succeeded_count,
        "error_counts": {kind: errors[kind] for kind in ERROR_KINDS},
        "error_rate": round((attempted - succeeded_count) / attempted, 6) if attempted else 0.0,
        "elapsed_seconds": round(elapsed_seconds, 6),
        "requests_per_second": round(succeeded_count / safe_elapsed, 3),
        "units": unit_name,
        "units_per_second": round(succeeded_count * unit_count / safe_elapsed, 3),
        "latency_ms": {
            "p50": milliseconds(0.50),
            "p95": milliseconds(0.95),
            "p99": milliseconds(0.99),
        },
    }


async def _request(
    client: httpx.AsyncClient, scenario: Scenario, payload: dict[str, Any]
) -> Sample:
    started = time.perf_counter()
    try:
        response = await client.post(scenario.endpoint, json=payload)
        latency = time.perf_counter() - started
        if response.status_code == 200:
            return Sample(latency, None)
        if 400 <= response.status_code < 500:
            return Sample(latency, "http_4xx")
        if 500 <= response.status_code < 600:
            return Sample(latency, "http_5xx")
        return Sample(latency, "unexpected_response")
    except httpx.TimeoutException:
        return Sample(time.perf_counter() - started, "timeout")
    except httpx.TransportError:
        return Sample(time.perf_counter() - started, "transport")
    except Exception:
        return Sample(time.perf_counter() - started, "unexpected_response")


ClientFactory = Callable[[], httpx.AsyncClient]


async def _cold_phase(
    scenario: Scenario, iterations: int, client_factory: ClientFactory
) -> dict[str, Any]:
    payload = scenario.payload()
    samples: list[Sample] = []
    started = time.perf_counter()
    for _ in range(iterations):
        async with client_factory() as client:
            samples.append(await _request(client, scenario, payload))
    return _measurement(
        samples, time.perf_counter() - started, scenario.unit_count, scenario.unit_name
    )


async def _warm_phase(
    scenario: Scenario,
    duration_seconds: float,
    iterations: int | None,
    client_factory: ClientFactory,
) -> dict[str, Any]:
    payload = scenario.payload()
    samples: list[Sample] = []
    next_request = 0

    async with AsyncExitStack() as stack:
        clients = [
            await stack.enter_async_context(client_factory()) for _ in range(scenario.concurrency)
        ]
        await asyncio.gather(*(_request(client, scenario, payload) for client in clients))
        started = time.perf_counter()
        deadline = started + duration_seconds

        async def worker(client: httpx.AsyncClient) -> None:
            nonlocal next_request
            while time.perf_counter() < deadline:
                if iterations is not None and next_request >= iterations:
                    return
                next_request += 1
                samples.append(await _request(client, scenario, payload))

        await asyncio.gather(*(worker(client) for client in clients))
        elapsed = time.perf_counter() - started
    return _measurement(samples, elapsed, scenario.unit_count, scenario.unit_name)


async def run_suite(
    selected: list[Scenario],
    *,
    api_url: str,
    duration_seconds: float,
    cold_iterations: int,
    iterations: int | None,
    client_factory: ClientFactory | None = None,
) -> dict[str, Any]:
    if client_factory is None:
        def default_client_factory() -> httpx.AsyncClient:
            return httpx.AsyncClient(base_url=api_url, timeout=10.0)

        client_factory = default_client_factory

    async with client_factory() as client:
        try:
            readiness = await client.get("/health/ready")
        except httpx.HTTPError as exc:
            raise RuntimeError(f"API readiness check failed: {exc}") from exc
        if readiness.status_code != 200:
            raise RuntimeError(f"API readiness check returned HTTP {readiness.status_code}")

    started_at = datetime.now(UTC)
    results: list[dict[str, Any]] = []
    for scenario in selected:
        cold = await _cold_phase(scenario, cold_iterations, client_factory)
        warm = await _warm_phase(scenario, duration_seconds, iterations, client_factory)
        results.append(
            {"name": scenario.name, "shape": scenario.shape(), "cold": cold, "warm": warm}
        )

    return {
        "schema_version": SCHEMA_VERSION,
        "run": {
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "api_url": api_url,
            "duration_seconds": duration_seconds,
            "cold_iterations": cold_iterations,
            "cold_semantics": "new HTTP client and connection per measured request",
            "request_cap": iterations,
            "python_version": platform.python_version(),
            "platform": platform.platform(),
        },
        "dataset": {
            "name": "fraud-demo",
            "accounts": list(ACCOUNTS),
            "feature_view": "account_transaction_features@1.0.0",
            "feature_count": len(FEATURES),
            "seeded_transaction_count": 576,
            "event_time_range": {
                "start_inclusive": "2025-01-01T00:00:00+00:00",
                "end_exclusive": "2025-01-04T00:00:00+00:00",
            },
            "observation_timestamp": OBSERVATION_TIMESTAMP,
            "assumptions": [
                "fraud demo source and registry are seeded",
                "the fraud backfill completed successfully",
                "all four example accounts are materialized online",
                "FS_INLINE_QUERY_LIMIT is at least 1000",
            ],
        },
        "scenarios": results,
    }


def _selected_scenarios(names: list[str] | None) -> list[Scenario]:
    if not names:
        return list(SCENARIOS.values())
    unknown = sorted(set(names) - SCENARIOS.keys())
    if unknown:
        choices = ", ".join(SCENARIOS)
        message = f"unknown scenario(s): {', '.join(unknown)}; choose from {choices}"
        raise typer.BadParameter(message)
    return [SCENARIOS[name] for name in names]


def run_benchmark(
    iterations: Annotated[
        int | None,
        typer.Option(help="Optional maximum warm requests per scenario."),
    ] = None,
    scenario: Annotated[
        list[str] | None,
        typer.Option("--scenario", help="Built-in scenario to run; repeat to select multiple."),
    ] = None,
    duration_seconds: Annotated[
        float, typer.Option(min=0.001, help="Warm phase duration per scenario.")
    ] = 10.0,
    cold_iterations: Annotated[
        int, typer.Option(min=0, help="Client-cold samples per scenario.")
    ] = 3,
    list_scenarios: Annotated[
        bool, typer.Option("--list-scenarios", help="List built-in scenarios and exit.")
    ] = False,
    output: Annotated[
        Path | None,
        typer.Option(
            dir_okay=False,
            writable=True,
            resolve_path=True,
            help="Also write the JSON report to this file.",
        ),
    ] = None,
) -> None:
    """Run reproducible, informational load scenarios against the local fraud example."""
    if list_scenarios:
        for item in SCENARIOS.values():
            typer.echo(
                f"{item.name}: {item.kind}, {item.unit_count} {item.unit_name}, "
                f"{item.feature_count} feature(s), concurrency {item.concurrency}"
            )
        return
    if iterations is not None and iterations < 1:
        raise typer.BadParameter("iterations must be at least 1")
    if output is not None and not output.parent.is_dir():
        raise typer.BadParameter(f"output parent directory does not exist: {output.parent}")

    selected = _selected_scenarios(scenario)
    try:
        report = asyncio.run(
            run_suite(
                selected,
                api_url=get_settings().api_url,
                duration_seconds=duration_seconds,
                cold_iterations=cold_iterations,
                iterations=iterations,
            )
        )
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(2) from exc

    rendered = json.dumps(report, indent=2)
    typer.echo(rendered)
    if output is not None:
        output.write_text(f"{rendered}\n")
    if any(result["warm"]["succeeded"] == 0 for result in report["scenarios"]):
        raise typer.Exit(1)


def main() -> None:
    typer.run(run_benchmark)


if __name__ == "__main__":
    main()
