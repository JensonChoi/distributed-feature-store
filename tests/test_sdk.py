from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import httpx

from feature_store.sdk import FeatureStoreClient


def test_sdk_uses_versioned_online_contract() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "resolved_features": ["account_stats@1.0.0:amount"],
                "rows": [
                    {
                        "account_id": "a",
                        "account_stats__status": "present",
                        "account_stats__amount": 10.0,
                    }
                ],
            },
        )

    with FeatureStoreClient(
        "http://feature-store", transport=httpx.MockTransport(handler)
    ) as client:
        result = client.get_online_features(
            [{"account_id": "a"}], features=["account_stats@1.0.0:amount"]
        )
    assert captured["path"] == "/v1/online-features:read"
    assert captured["body"] == {
        "entities": [{"account_id": "a"}],
        "features": ["account_stats@1.0.0:amount"],
        "feature_service": None,
    }
    assert result.rows[0]["account_stats__amount"] == 10.0


def test_sdk_parses_async_historical_job() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            202,
            json={
                "id": "job-1",
                "kind": "historical_query",
                "status": "pending",
                "payload": {
                    "observation_count": 10001,
                    "resolved_features": ["account_stats@1.0.0:amount"],
                },
                "checkpoints": [],
                "error": None,
                "failure_kind": None,
                "attempt_count": 0,
                "max_attempts": 3,
                "next_attempt_at": None,
                "worker_id": None,
                "lease_expires_at": None,
                "last_heartbeat_at": None,
                "created_at": datetime(2025, 1, 1, tzinfo=UTC).isoformat(),
                "started_at": None,
                "finished_at": None,
                "artifact_expires_at": None,
                "artifacts_cleaned_at": None,
                "result": None,
            },
        )

    with FeatureStoreClient(
        "http://feature-store", transport=httpx.MockTransport(handler)
    ) as client:
        result = client.get_historical_features(
            [
                {
                    "entity_values": {"account_id": "a"},
                    "event_timestamp": "2025-01-01T00:00:00Z",
                }
            ],
            features=["account_stats@1.0.0:amount"],
        )
    assert result.kind == "historical_query"
    assert result.id == "job-1"


def test_sdk_streams_job_result_to_file(tmp_path: Path) -> None:
    content = b"parquet-content"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/jobs/job-1/result"
        return httpx.Response(200, content=content)

    output = tmp_path / "result.parquet"
    with FeatureStoreClient(
        "http://feature-store", transport=httpx.MockTransport(handler)
    ) as client:
        returned = client.download_job_result("job-1", output, chunk_size=3)
    assert returned == output
    assert output.read_bytes() == content
