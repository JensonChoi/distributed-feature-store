from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, cast

import httpx
import yaml

from feature_store.config import get_settings
from feature_store.models import (
    ApplyResult,
    HistoricalQuery,
    JobRequest,
    JobResponse,
    Observation,
    OnlineQuery,
    QueryResponse,
    RegistryManifest,
)


class FeatureStoreClient:
    def __init__(
        self,
        base_url: str | None = None,
        *,
        timeout: float = 30.0,
        transport: httpx.BaseTransport | None = None,
    ):
        self._client = httpx.Client(
            base_url=base_url or get_settings().api_url,
            timeout=timeout,
            transport=transport,
        )

    def __enter__(self) -> FeatureStoreClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def apply(self, manifest: RegistryManifest) -> ApplyResult:
        response = self._client.post("/v1/registry/apply", json=manifest.model_dump(mode="json"))
        response.raise_for_status()
        return ApplyResult.model_validate(response.json())

    def apply_file(self, path: str | Path) -> ApplyResult:
        content = yaml.safe_load(Path(path).read_text())
        return self.apply(RegistryManifest.model_validate(content))

    def list_registry(self, kind: str | None = None) -> list[dict[str, Any]]:
        response = self._client.get("/v1/registry", params={"kind": kind} if kind else None)
        response.raise_for_status()
        return cast(list[dict[str, Any]], response.json())

    def get_online_features(
        self,
        entities: list[dict[str, Any]],
        *,
        features: list[str] | None = None,
        feature_service: str | None = None,
    ) -> QueryResponse:
        query = OnlineQuery(
            entities=entities, features=features or [], feature_service=feature_service
        )
        response = self._client.post("/v1/online-features:read", json=query.model_dump(mode="json"))
        response.raise_for_status()
        return QueryResponse.model_validate(response.json())

    def get_historical_features(
        self,
        observations: list[Observation | dict[str, Any]],
        *,
        features: list[str] | None = None,
        feature_service: str | None = None,
    ) -> QueryResponse | JobResponse:
        parsed = [Observation.model_validate(item) for item in observations]
        query = HistoricalQuery(
            observations=parsed, features=features or [], feature_service=feature_service
        )
        response = self._client.post(
            "/v1/historical-features:query", json=query.model_dump(mode="json")
        )
        response.raise_for_status()
        if response.status_code == 202:
            return JobResponse.model_validate(response.json())
        return QueryResponse.model_validate(response.json())

    def download_job_result(
        self, job_id: str, output: str | Path, *, chunk_size: int = 1024 * 1024
    ) -> Path:
        path = Path(output)
        with self._client.stream("GET", f"/v1/jobs/{job_id}/result") as response:
            response.raise_for_status()
            with path.open("wb") as destination:
                for chunk in response.iter_bytes(chunk_size=chunk_size):
                    destination.write(chunk)
        return path

    def backfill(self, feature_view: str, start: datetime, end: datetime) -> dict[str, Any]:
        return self._create_job("backfills", feature_view, start, end)

    def materialize(self, feature_view: str, start: datetime, end: datetime) -> dict[str, Any]:
        return self._create_job("materializations", feature_view, start, end)

    def job(self, job_id: str) -> dict[str, Any]:
        response = self._client.get(f"/v1/jobs/{job_id}")
        response.raise_for_status()
        return cast(dict[str, Any], response.json())

    def jobs(self) -> list[dict[str, Any]]:
        response = self._client.get("/v1/jobs")
        response.raise_for_status()
        return cast(list[dict[str, Any]], response.json())

    def retry_job(self, job_id: str) -> dict[str, Any]:
        response = self._client.post(f"/v1/jobs/{job_id}:retry")
        response.raise_for_status()
        return cast(dict[str, Any], response.json())

    def _create_job(
        self, kind: str, feature_view: str, start: datetime, end: datetime
    ) -> dict[str, Any]:
        request = JobRequest(feature_view=feature_view, start=start, end=end)
        response = self._client.post(f"/v1/jobs/{kind}", json=request.model_dump(mode="json"))
        response.raise_for_status()
        return cast(dict[str, Any], response.json())
