from __future__ import annotations

import logging
import os
import socket
import threading
import time
import uuid

from feature_store.config import get_settings
from feature_store.db import SessionLocal, init_db
from feature_store.jobs import JobExecutor
from feature_store.observability import configure_logging

logger = logging.getLogger(__name__)


def _heartbeat(job_id: str, lease_token: str, worker_id: str, stopped: threading.Event) -> None:
    settings = get_settings()
    while not stopped.wait(settings.job_heartbeat_seconds):
        try:
            with SessionLocal() as session:
                active = JobExecutor(session, worker_id=worker_id, settings=settings).heartbeat(
                    job_id, lease_token
                )
        except Exception:
            logger.exception(
                "job heartbeat failed",
                extra={"job_id": job_id, "worker_id": worker_id},
            )
            continue
        if not active:
            logger.warning(
                "job lease lost",
                extra={"job_id": job_id, "worker_id": worker_id},
            )
            return


def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    init_db()
    worker_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
    logger.info("worker started", extra={"worker_id": worker_id})
    while True:
        with SessionLocal() as session:
            executor = JobExecutor(session, worker_id=worker_id, settings=settings)
            job = executor.claim_next()
            if job:
                logger.info(
                    "executing job",
                    extra={
                        "job_id": job.id,
                        "job_kind": job.kind,
                        "worker_id": worker_id,
                        "attempt_count": job.attempt_count,
                        "max_attempts": job.max_attempts,
                    },
                )
                assert job.lease_token is not None
                stopped = threading.Event()
                heartbeat = threading.Thread(
                    target=_heartbeat,
                    args=(job.id, job.lease_token, worker_id, stopped),
                    daemon=True,
                )
                heartbeat.start()
                try:
                    executor.execute(job)
                finally:
                    stopped.set()
                    heartbeat.join()
                logger.info(
                    "job finished",
                    extra={
                        "job_id": job.id,
                        "job_status": job.status,
                        "worker_id": worker_id,
                        "attempt_count": job.attempt_count,
                    },
                )
            else:
                time.sleep(settings.job_poll_seconds)


if __name__ == "__main__":
    run()
