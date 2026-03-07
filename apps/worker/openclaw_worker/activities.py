from __future__ import annotations

import random
import time
from datetime import timedelta
from uuid import UUID

import httpx
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from temporalio import activity

from openclaw_shared.db_models import Job
from openclaw_shared.state_machine import validate_transition
from openclaw_shared.status import JobStatus
from openclaw_worker.config import settings

engine = create_engine(settings.database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


@activity.defn
async def emit_status(job_id: str, step: str, state: str, detail: str | None = None) -> None:
    payload = {
        "job_id": job_id,
        "step": step,
        "state": state,
        "detail": detail,
    }
    async with httpx.AsyncClient(timeout=5.0) as client:
        await client.post(settings.n8n_status_webhook_url, json=payload)


@activity.defn
async def emit_exception(job_id: str, error: str, detail: str | None = None) -> None:
    payload = {
        "job_id": job_id,
        "error": error,
        "detail": detail,
        "classification": "failed_after_retries",
    }
    async with httpx.AsyncClient(timeout=5.0) as client:
        await client.post(settings.n8n_exceptions_webhook_url, json=payload)


@activity.defn
def set_job_status(job_id: str, target_status: str) -> None:
    session = SessionLocal()
    try:
        job = session.get(Job, UUID(job_id))
        if job is None:
            return
        current = JobStatus(job.status)
        next_status = JobStatus(target_status)
        validate_transition(current, next_status)
        job.status = next_status.value
        session.commit()
    finally:
        session.close()


@activity.defn
def stub_step(job_id: str, name: str) -> None:
    del job_id
    if settings.fail_mode and name == "clip":
        raise RuntimeError("FAIL_MODE triggered clip step failure")
    time.sleep(random.uniform(1.0, 2.0))
