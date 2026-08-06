from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import httpx

from feature_store.models import RegistryManifest, RegistryMetadataPatch, RegistryTarget
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


def test_sdk_incremental_materialization_contract() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(202, json={"id": "job-1"})

    with FeatureStoreClient(
        "http://feature-store", transport=httpx.MockTransport(handler)
    ) as client:
        result = client.materialize_incremental(
            "account_stats@1.0.0",
            end=datetime(2025, 1, 2, tzinfo=UTC),
            lookback_seconds=120,
        )
    assert result == {"id": "job-1"}
    assert captured == {
        "path": "/v1/jobs/materializations:incremental",
        "body": {
            "feature_view": "account_stats@1.0.0",
            "end": "2025-01-02T00:00:00Z",
            "lookback_seconds": 120,
        },
    }


def test_sdk_registry_plan_methods_parse_typed_responses(
    tmp_path: Path, manifest: RegistryManifest
) -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        return httpx.Response(
            200,
            json={
                "fingerprint": manifest.fingerprint(),
                "summary": {"created": 0, "unchanged": 0, "rejected": 0},
                "objects": [],
            },
        )

    path = tmp_path / "registry.yaml"
    path.write_text("entities: []\n")
    with FeatureStoreClient(
        "http://feature-store", transport=httpx.MockTransport(handler)
    ) as client:
        assert client.validate(manifest).summary.rejected == 0
        assert client.validate_file(path).objects == []
        assert client.plan(manifest).summary.created == 0
        assert client.plan_file(path).objects == []
    assert paths == [
        "/v1/registry/validate",
        "/v1/registry/validate",
        "/v1/registry/plan",
        "/v1/registry/plan",
    ]


def test_sdk_registry_lifecycle_methods_use_typed_contracts() -> None:
    calls: list[tuple[str, str, object]] = []
    target = RegistryTarget(kind="entity", name="account")
    descriptor = {
        "target": target.model_dump(mode="json"),
        "fingerprint": "abc",
        "spec": {"name": "account", "join_keys": {"account_id": "string"}},
        "provenance": {
            "created_at": "2025-01-01T00:00:00Z",
            "manifest_fingerprint": "abc",
        },
        "metadata": {"owners": [], "tags": {}, "documentation_links": []},
        "deprecation": {
            "status": "active",
            "deprecated_at": None,
            "message": None,
            "replacement": None,
        },
        "updated_at": None,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        calls.append((request.method, request.url.path, body))
        return httpx.Response(200, json=descriptor)

    with FeatureStoreClient(
        "http://feature-store", transport=httpx.MockTransport(handler)
    ) as client:
        assert client.describe_registry_object(target).target == target
        client.patch_registry_metadata(target, RegistryMetadataPatch(owners=["fraud-team"]))
        client.deprecate_registry_object(target, message="retiring")
        client.activate_registry_object(target)

    assert [(method, path) for method, path, _ in calls] == [
        ("GET", "/v1/registry/object"),
        ("PATCH", "/v1/registry/object/metadata"),
        ("POST", "/v1/registry/object:deprecate"),
        ("POST", "/v1/registry/object:activate"),
    ]
    assert calls[1][2] == {
        "target": {"kind": "entity", "name": "account", "version": None, "feature": None},
        "patch": {
            "owners": ["fraud-team"],
            "tags": None,
            "documentation_links": None,
        },
    }
