from __future__ import annotations

from collections.abc import Iterator

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from feature_store.api import app, get_session
from feature_store.models import RegistryManifest


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
