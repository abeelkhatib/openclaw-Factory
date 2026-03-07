from __future__ import annotations

import hashlib
import json
import uuid

from fastapi import Depends, FastAPI, Header, HTTPException
from sqlalchemy.orm import Session
from temporalio.client import Client

from openclaw_orchestrator_api.config import settings
from openclaw_orchestrator_api.db import get_db
from openclaw_shared.db_models import IdempotencyKey, Job
from openclaw_shared.models import JobRecordResponse, JobStartRequest, JobStartResponse
from openclaw_shared.status import JobStatus

app = FastAPI(title="orchestrator_api", version="0.1.0")


def _request_hash(payload: JobStartRequest) -> str:
    normalized = json.dumps(payload.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


async def _temporal_client() -> Client:
    return await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/jobs/start", response_model=JobStartResponse)
async def start_job(
    payload: JobStartRequest,
    db: Session = Depends(get_db),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> JobStartResponse:
    if not idempotency_key:
        raise HTTPException(status_code=400, detail="Idempotency-Key header is required")

    request_hash = _request_hash(payload)
    existing = db.get(IdempotencyKey, idempotency_key)
    if existing:
        if existing.request_hash != request_hash:
            raise HTTPException(status_code=409, detail="Idempotency-Key reuse with different request body")
        return JobStartResponse.model_validate(existing.response_json)

    job = Job(status=JobStatus.CREATED.value, config_json=payload.model_dump(mode="json"))
    db.add(job)
    db.flush()

    workflow_id = f"pipeline-{job.id}"
    workflow_run_id: str | None = None
    try:
        client = await _temporal_client()
        handle = await client.start_workflow(
            "Pipeline",
            str(job.id),
            id=workflow_id,
            task_queue=settings.temporal_task_queue,
        )
        workflow_run_id = handle.run_id
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=502, detail=f"Failed to start workflow: {exc}") from exc

    response = JobStartResponse(
        job_id=job.id,
        status=JobStatus(job.status),
        workflow_id=workflow_id,
        workflow_run_id=workflow_run_id,
    )

    idem = IdempotencyKey(
        key=idempotency_key,
        endpoint="POST /jobs/start",
        request_hash=request_hash,
        response_json=response.model_dump(mode="json"),
    )
    db.add(idem)
    db.commit()

    return response


@app.get("/jobs/{job_id}", response_model=JobRecordResponse)
def get_job(job_id: uuid.UUID, db: Session = Depends(get_db)) -> JobRecordResponse:
    job = db.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobRecordResponse(
        id=job.id,
        status=JobStatus(job.status),
        config_json=job.config_json,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )


def run() -> None:
    import uvicorn

    uvicorn.run(
        "openclaw_orchestrator_api.main:app",
        host=settings.orchestrator_host,
        port=settings.orchestrator_port,
        reload=False,
    )
