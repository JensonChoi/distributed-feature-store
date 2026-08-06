from __future__ import annotations

from collections.abc import Iterator

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from feature_store.api import app, get_session
from feature_store.models import RegistryManifest
from feature_store.registry import Registry


def test_registry_api_is_idempotent_and_returns_request_id(
    session: Session, manifest: RegistryManifest
) -> None:
    def override_session() -> Iterator[Session]:
        yield session

    app.dependency_overrides[get_session] = override_session
    client = TestClient(app, raise_server_exceptions=False)
    try:
        response = client.post(
            "/v1/registry/apply",
            json=manifest.model_dump(mode="json"),
            headers={"x-request-id": "test-request"},
        )
        assert response.status_code == 200
        assert response.headers["x-request-id"] == "test-request"
        assert response.json()["created"] == 5

        repeated = client.post("/v1/registry/apply", json=manifest.model_dump(mode="json"))
        assert repeated.status_code == 200
        assert repeated.json()["unchanged"] == 5

        listed = client.get("/v1/registry", params={"kind": "feature_view"})
        assert listed.status_code == 200
        assert listed.json()[0]["name"] == "account_stats"
    finally:
        client.close()
        app.dependency_overrides.clear()


def test_registry_plan_and_validation_return_structured_rejections(
    session: Session, manifest: RegistryManifest
) -> None:
    def override_session() -> Iterator[Session]:
        yield session

    app.dependency_overrides[get_session] = override_session
    client = TestClient(app, raise_server_exceptions=False)
    try:
        valid = client.post("/v1/registry/plan", json=manifest.model_dump(mode="json"))
        assert valid.status_code == 200
        assert valid.json()["summary"] == {"created": 5, "unchanged": 0, "rejected": 0}

        rejected_payload = manifest.model_dump(mode="json")
        rejected_payload["feature_views"][0]["entity"] = "missing_entity"
        rejected = client.post("/v1/registry/validate", json=rejected_payload)
        assert rejected.status_code == 200
        assert rejected.json()["summary"]["rejected"] == 2
        assert rejected.json()["objects"][3]["issues"][0]["code"] == "missing_entity"

        malformed = client.post("/v1/registry/plan", json={"entities": [{"name": "BAD"}]})
        assert malformed.status_code == 422
        assert Registry(session).list_records() == []
    finally:
        client.close()
        app.dependency_overrides.clear()


def test_incremental_materialization_api_coalesces_active_submissions(
    session: Session, manifest: RegistryManifest
) -> None:
    def override_session() -> Iterator[Session]:
        yield session

    app.dependency_overrides[get_session] = override_session
    client = TestClient(app, raise_server_exceptions=False)
    try:
        client.post("/v1/registry/apply", json=manifest.model_dump(mode="json"))
        payload = {
            "feature_view": "account_stats@1.0.0",
            "end": "2025-01-02T00:00:00Z",
            "lookback_seconds": 120,
        }
        first = client.post("/v1/jobs/materializations:incremental", json=payload)
        second = client.post("/v1/jobs/materializations:incremental", json=payload)
        assert first.status_code == second.status_code == 202
        assert first.json()["id"] == second.json()["id"]
        assert first.json()["payload"] == {
            "mode": "incremental",
            "feature_view": "account_stats@1.0.0",
            "end": "2025-01-02T00:00:00+00:00",
            "lookback_seconds": 120,
        }
    finally:
        client.close()
        app.dependency_overrides.clear()
