from __future__ import annotations

import logging
import time

from feature_store.config import get_settings
from feature_store.db import SessionLocal, init_db
from feature_store.jobs import JobExecutor
from feature_store.observability import configure_logging

logger = logging.getLogger(__name__)


def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    init_db()
    with SessionLocal() as session:
        recovered = JobExecutor(session).recover_interrupted()
        if recovered:
            logger.warning("requeued interrupted jobs", extra={"recovered_jobs": recovered})
    while True:
        with SessionLocal() as session:
            executor = JobExecutor(session)
            job = executor.claim_next()
            if job:
                logger.info(
                    "executing job",
                    extra={"job_id": job.id, "job_kind": job.kind},
                )
                executor.execute(job)
                logger.info(
                    "job finished",
                    extra={"job_id": job.id, "job_status": job.status},
                )
            else:
                time.sleep(settings.job_poll_seconds)


if __name__ == "__main__":
    run()
