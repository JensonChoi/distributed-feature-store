from __future__ import annotations

import json
import statistics
import time
from datetime import UTC, datetime

import httpx
import typer

from feature_store.config import get_settings


def _summary(samples: list[float]) -> dict[str, float]:
    ordered = sorted(samples)
    p95_index = min(len(ordered) - 1, int(len(ordered) * 0.95))
    return {
        "requests": float(len(samples)),
        "p50_ms": round(statistics.median(ordered) * 1000, 3),
        "p95_ms": round(ordered[p95_index] * 1000, 3),
    }


def main(iterations: int = 200) -> None:
    """Report local API latency without enforcing machine-dependent thresholds."""
    if iterations < 2:
        raise typer.BadParameter("iterations must be at least 2")
    payload = {
        "entities": [{"account_id": "acct_004"}],
        "feature_service": "fraud_model_v1",
    }
    samples: list[float] = []
    with httpx.Client(base_url=get_settings().api_url, timeout=10) as client:
        client.post("/v1/online-features:read", json=payload).raise_for_status()
        for _ in range(iterations):
            started = time.perf_counter()
            client.post("/v1/online-features:read", json=payload).raise_for_status()
            samples.append(time.perf_counter() - started)
        historical_started = time.perf_counter()
        historical = client.post(
            "/v1/historical-features:query",
            json={
                "observations": [
                    {
                        "entity_values": {"account_id": "acct_004"},
                        "event_timestamp": datetime(2025, 1, 3, tzinfo=UTC).isoformat(),
                    }
                    for _ in range(1000)
                ],
                "feature_service": "fraud_model_v1",
            },
        )
        historical.raise_for_status()
        historical_seconds = time.perf_counter() - historical_started
    result = {
        "online": _summary(samples),
        "historical": {
            "rows": 1000,
            "seconds": round(historical_seconds, 3),
            "rows_per_second": round(1000 / historical_seconds, 1),
        },
    }
    typer.echo(json.dumps(result, indent=2))


if __name__ == "__main__":
    typer.run(main)
