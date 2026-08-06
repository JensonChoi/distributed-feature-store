from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from feature_store.api import app, get_artifact_storage, get_session
from feature_store.artifacts import ArtifactStorage
from feature_store.config import Settings, get_settings
from feature_store.db import JobRecord
from feature_store.jobs import JobExecutor, JobService, serialize_job
from feature_store.models import (
    HistoricalQuery,
    JobStatus,
    Observation,
    RegistryManifest,
    RegistryTarget,
)
from feature_store.offline import OfflineStore
from feature_store.pit import HistoricalRetriever
from feature_store.registry import Registry


class LocalOfflineStore(OfflineStore):
    def __init__(self, root: Path):
        super().__init__()
        self.root = root

    def view_uri(self, view_ref: str) -> str:
        return str(self.root / "views" / view_ref.replace("@", "_"))


def query(timestamp: datetime, *, rows: int = 2) -> HistoricalQuery:
    return HistoricalQuery(
        observations=[
            Observation(entity_values={"account_id": "a"}, event_timestamp=timestamp)
            for _ in range(rows)
        ],
        features=["account_stats@1.0.0:amount"],
    )


def test_historical_job_writes_and_publishes_versioned_parquet(
    tmp_path: Path, session: Session, manifest: RegistryManifest
) -> None:
    registry = Registry(session)
    registry.apply(manifest)
    offline = LocalOfflineStore(tmp_path)
    artifacts = ArtifactStorage(root_uri=str(tmp_path / "artifacts"))
    timestamp = datetime(2025, 1, 1, 12, tzinfo=UTC)
    offline.append(
        offline.view_uri("account_stats@1.0.0"),
        pa.Table.from_pylist(
            [
                {
                    "account_id": "a",
                    "event_timestamp": timestamp - timedelta(minutes=1),
                    "event_id": "event-1",
                    "amount": 12.5,
                    "event_date": "2025-01-01",
                }
            ]
        ),
        "event_date",
    )
    request = query(timestamp)
    resolved = HistoricalRetriever(registry, offline).validate(
        request.observations, request.features, request.feature_service
    )
    feature_target = RegistryTarget(
        kind="feature_view",
        name="account_stats",
        version="1.0.0",
        feature="amount",
    )
    registry.deprecate(feature_target, "use a newer feature")
    warning_snapshot = registry.warnings_for_query(request.features)
    job = JobService(session).create_historical_query(
        request, resolved, artifacts, warnings=warning_snapshot
    )
    registry.reactivate(feature_target)
    assert artifacts.exists(artifacts.input_uri(job.id))
    assert "artifact_uri" not in serialize_job(job)
    assert job.payload["warnings"] == [
        warning.model_dump(mode="json") for warning in warning_snapshot
    ]

    settings = Settings(historical_result_ttl_seconds=60)
    executor = JobExecutor(session, offline=offline, artifacts=artifacts, settings=settings)
    claimed = executor.claim_next()
    assert claimed is not None and claimed.lease_token is not None
    lease_token = claimed.lease_token
    executor.execute(claimed)

    assert claimed.status == JobStatus.SUCCEEDED, claimed.error
    assert claimed.result_uri is not None
    assert f"attempt-1-{lease_token}.parquet" in claimed.result_uri
    result = pq.read_table(claimed.result_uri)
    assert result.num_rows == 2
    assert result["account_stats__amount"].to_pylist() == [12.5, 12.5]
    response = serialize_job(claimed)
    assert response["result"]["row_count"] == 2
    assert response["payload"]["warnings"] == response["result"]["warnings"]
    assert response["result"]["download_url"] == f"/v1/jobs/{job.id}/result"
    assert "result_uri" not in response


def test_async_api_validates_before_staging_and_streams_result(
    tmp_path: Path,
    session: Session,
    manifest: RegistryManifest,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    Registry(session).apply(manifest)
    artifacts = ArtifactStorage(root_uri=str(tmp_path / "artifacts"))

    def override_session() -> Iterator[Session]:
        yield session

    app.dependency_overrides[get_session] = override_session
    app.dependency_overrides[get_artifact_storage] = lambda: artifacts
    monkeypatch.setenv("FS_INLINE_QUERY_LIMIT", "1")
    get_settings.cache_clear()
    client = TestClient(app, raise_server_exceptions=False)
    timestamp = datetime(2025, 1, 1, 12, tzinfo=UTC)
    try:
        invalid = query(timestamp).model_dump(mode="json")
        invalid["observations"][0]["entity_values"] = {}
        response = client.post("/v1/historical-features:query", json=invalid)
        assert response.status_code == 422
        assert session.scalar(select(JobRecord)) is None
        assert not (tmp_path / "artifacts").exists()

        response = client.post(
            "/v1/historical-features:query", json=query(timestamp).model_dump(mode="json")
        )
        assert response.status_code == 202
        body = response.json()
        assert body["kind"] == "historical_query"
        assert body["payload"]["observation_count"] == 2
        assert "artifact_uri" not in body

        job = JobService(session).get(body["id"])
        result_uri = artifacts.result_uri(job.id, 1, "published")
        size = artifacts.write_parquet(result_uri, pa.table({"value": [1, 2]}))
        job.status = JobStatus.SUCCEEDED
        job.result_uri = result_uri
        job.result_metadata = {
            "format": "parquet",
            "content_type": "application/vnd.apache.parquet",
            "row_count": 2,
            "byte_size": size,
            "resolved_features": job.payload["resolved_features"],
        }
        job.artifact_expires_at = datetime.now(UTC) + timedelta(minutes=1)
        session.commit()
        download = client.get(f"/v1/jobs/{job.id}/result")
        assert download.status_code == 200
        assert pq.read_table(pa.BufferReader(download.content)).to_pydict() == {"value": [1, 2]}
    finally:
        client.close()
        app.dependency_overrides.clear()
        get_settings.cache_clear()


def test_expired_artifacts_are_gone_and_cannot_be_retried(tmp_path: Path, session: Session) -> None:
    artifacts = ArtifactStorage(root_uri=str(tmp_path / "artifacts"))
    now = datetime(2025, 1, 2, tzinfo=UTC)
    job = JobRecord(
        kind="historical_query",
        status=JobStatus.FAILED,
        payload={"observation_count": 1, "resolved_features": []},
        checkpoints=[],
        artifact_uri=artifacts.input_uri("expired"),
        artifact_expires_at=now - timedelta(seconds=1),
    )
    job.id = "expired"
    artifacts.write_json(job.artifact_uri, {"observations": []})
    session.add(job)
    session.commit()

    executor = JobExecutor(session, artifacts=artifacts, now=lambda: now)
    assert executor.cleanup_expired_artifacts() == 1
    session.refresh(job)
    assert job.artifacts_cleaned_at is not None
    assert job.artifacts_cleaned_at.replace(tzinfo=UTC) == now
    assert not Path(artifacts.job_prefix(job.id)).exists()
    assert executor.cleanup_expired_artifacts() == 0
    try:
        JobService(session).retry(job.id)
    except ValueError as exc:
        assert "expired" in str(exc)
    else:
        raise AssertionError("expired historical jobs must not be retried")


def test_cancelled_historical_job_can_retry_before_expiry(tmp_path: Path, session: Session) -> None:
    artifacts = ArtifactStorage(root_uri=str(tmp_path / "artifacts"))
    timestamp = datetime.now(UTC)
    request = query(timestamp)
    job = JobService(session).create_historical_query(request, request.features, artifacts)
    service = JobService(session, Settings(historical_result_ttl_seconds=60))
    cancelled = service.cancel(job.id)
    assert cancelled.artifact_expires_at is not None
    retried = service.retry(job.id)
    assert retried.status == JobStatus.PENDING
    assert retried.artifact_expires_at is None
    assert artifacts.exists(artifacts.input_uri(job.id))
