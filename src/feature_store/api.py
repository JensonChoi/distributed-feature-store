from __future__ import annotations

import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast

import redis
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from sqlalchemy import text
from sqlalchemy.orm import Session

from feature_store.config import get_settings
from feature_store.db import SessionLocal, init_db
from feature_store.jobs import JobService, serialize_job
from feature_store.models import (
    ApplyResult,
    HistoricalQuery,
    JobKind,
    JobRequest,
    OnlineQuery,
    QueryResponse,
    RegistryManifest,
)
from feature_store.observability import configure_logging
from feature_store.offline import OfflineStore
from feature_store.online import OnlineStore
from feature_store.pit import HistoricalRetriever
from feature_store.registry import Registry, RegistryConflictError, RegistryNotFoundError

REQUESTS = Counter(
    "feature_store_http_requests_total", "HTTP requests", ["method", "path", "status"]
)
LATENCY = Histogram("feature_store_http_request_seconds", "HTTP request duration", ["path"])


def get_session() -> Any:
    with SessionLocal() as session:
        yield session


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    configure_logging(get_settings().log_level)
    init_db()
    yield


app = FastAPI(
    title="Distributed Feature Store",
    version="0.1.0",
    description="Versioned offline and online features with point-in-time retrieval.",
    lifespan=lifespan,
)


@app.middleware("http")
async def observe_requests(request: Request, call_next: Any) -> Response:
    started = time.perf_counter()
    request_id = request.headers.get("x-request-id", str(uuid.uuid4()))
    response = await call_next(request)
    route = request.scope.get("route")
    path = getattr(route, "path", request.url.path)
    REQUESTS.labels(request.method, path, response.status_code).inc()
    LATENCY.labels(path).observe(time.perf_counter() - started)
    response.headers["x-request-id"] = request_id
    return cast(Response, response)


@app.exception_handler(RegistryNotFoundError)
async def not_found(_: Request, exc: RegistryNotFoundError) -> JSONResponse:
    return JSONResponse(status_code=404, content={"detail": str(exc).strip("'")})


@app.exception_handler(RegistryConflictError)
async def conflict(_: Request, exc: RegistryConflictError) -> JSONResponse:
    return JSONResponse(status_code=409, content={"detail": str(exc)})


@app.exception_handler(ValueError)
async def invalid_request(_: Request, exc: ValueError) -> JSONResponse:
    return JSONResponse(status_code=422, content={"detail": str(exc)})


@app.get("/health/live")
def live() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/ready")
def ready(session: Session = Depends(get_session)) -> dict[str, str]:
    try:
        session.execute(text("SELECT 1"))
        redis.Redis.from_url(get_settings().redis_url).ping()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"dependency unavailable: {exc}") from exc
    return {"status": "ready"}


@app.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/v1/registry/apply", response_model=ApplyResult)
def apply_registry(
    manifest: RegistryManifest, session: Session = Depends(get_session)
) -> ApplyResult:
    return Registry(session).apply(manifest)


@app.get("/v1/registry")
def list_registry(
    kind: str | None = None, session: Session = Depends(get_session)
) -> list[dict[str, Any]]:
    return Registry(session).list_records(kind)


@app.post("/v1/online-features:read", response_model=QueryResponse)
def online_read(query: OnlineQuery, session: Session = Depends(get_session)) -> QueryResponse:
    return OnlineStore().read(
        Registry(session), query.entities, query.features, query.feature_service
    )


@app.post("/v1/historical-features:query", response_model=QueryResponse)
def historical_query(
    query: HistoricalQuery, session: Session = Depends(get_session)
) -> QueryResponse:
    if len(query.observations) > get_settings().inline_query_limit:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"inline query limit is {get_settings().inline_query_limit} rows",
        )
    return HistoricalRetriever(Registry(session), OfflineStore()).query(
        query.observations, query.features, query.feature_service
    )


@app.post("/v1/jobs/backfills", status_code=202)
def create_backfill(request: JobRequest, session: Session = Depends(get_session)) -> dict[str, Any]:
    Registry(session).feature_view(request.feature_view)
    return serialize_job(JobService(session).create(JobKind.BACKFILL, request))


@app.post("/v1/jobs/materializations", status_code=202)
def create_materialization(
    request: JobRequest, session: Session = Depends(get_session)
) -> dict[str, Any]:
    Registry(session).feature_view(request.feature_view)
    return serialize_job(JobService(session).create(JobKind.MATERIALIZE, request))


@app.get("/v1/jobs")
def list_jobs(limit: int = 100, session: Session = Depends(get_session)) -> list[dict[str, Any]]:
    return [serialize_job(job) for job in JobService(session).list(min(limit, 500))]


@app.get("/v1/jobs/{job_id}")
def get_job(job_id: str, session: Session = Depends(get_session)) -> dict[str, Any]:
    try:
        return serialize_job(JobService(session).get(job_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc).strip("'")) from exc


@app.post("/v1/jobs/{job_id}:cancel")
def cancel_job(job_id: str, session: Session = Depends(get_session)) -> dict[str, Any]:
    try:
        return serialize_job(JobService(session).cancel(job_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc).strip("'")) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/v1/jobs/{job_id}:retry")
def retry_job(job_id: str, session: Session = Depends(get_session)) -> dict[str, Any]:
    try:
        return serialize_job(JobService(session).retry(job_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc).strip("'")) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


def run() -> None:
    uvicorn.run("feature_store.api:app", host="0.0.0.0", port=8000)


if __name__ == "__main__":
    run()
