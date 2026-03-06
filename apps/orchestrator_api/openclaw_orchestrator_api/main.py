from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Header, Query
from sqlalchemy import select, and_
from sqlalchemy.orm import Session
from temporalio.client import Client, WorkflowIDReusePolicy

from openclaw_orchestrator_api.config import settings
from openclaw_orchestrator_api.db import get_db
from openclaw_shared.db_models import (
    BacklogStat,
    ChannelConfig,
    IdempotencyKey,
    Job,
    Script,
    Topic,
    VideoJob,
)
from openclaw_shared.models import (
    BacklogStatus,
    ChannelConfigResponse,
    GateBAlert,
    JobRecordResponse,
    JobStartRequest,
    JobStartResponse,
    TopicCandidate,
    TopicResponse,
    ValidationResult,
    VideoJobResponse,
    VideoProductionInput,
)
from openclaw_shared.status import JobStatus

app = FastAPI(title="orchestrator_api", version="0.2.0")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _request_hash(payload: Any) -> str:
    normalized = json.dumps(
        payload.model_dump(mode="json") if hasattr(payload, "model_dump") else payload,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


async def _temporal_client() -> Client:
    return await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace)


def _video_job_to_response(vj: VideoJob) -> VideoJobResponse:
    return VideoJobResponse(
        id=vj.id,
        topic_id=vj.topic_id,
        script_id=vj.script_id,
        channel_id=vj.channel_id,
        status=vj.status,
        stage=vj.stage,
        clip_urls=vj.clip_urls,
        vo_url=vj.vo_url,
        render_url=vj.render_url,
        publish_url=vj.publish_url,
        error=vj.error,
        gate_b_alerts=vj.gate_b_alerts,
        created_at=vj.created_at,
        updated_at=vj.updated_at,
    )


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health")
async def health(db: Session = Depends(get_db)) -> dict[str, str]:
    result: dict[str, str] = {}
    db_ok = False
    temporal_ok = False

    try:
        db.execute(select(1))  # type: ignore[arg-type]
        db_ok = True
    except Exception as exc:
        result["db_error"] = str(exc)

    try:
        client = await _temporal_client()
        await client.service_client.health_check()
        temporal_ok = True
    except Exception as exc:
        result["temporal_error"] = str(exc)

    result["db"] = "ok" if db_ok else "degraded"
    result["temporal"] = "ok" if temporal_ok else "degraded"

    if db_ok and temporal_ok:
        result["status"] = "ok"
    elif db_ok or temporal_ok:
        result["status"] = "degraded"
    else:
        result["status"] = "down"

    return result


# ---------------------------------------------------------------------------
# Legacy job endpoints (kept for backward compatibility)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Video jobs
# ---------------------------------------------------------------------------

class CreateVideoJobRequest(JobStartRequest.__bases__[0]):  # type: ignore[misc]
    """Create a VideoJob directly."""
    from pydantic import BaseModel as _B

    class _Inner(_B):
        topic_id: uuid.UUID
        channel_id: uuid.UUID

CreateVideoJobRequest = CreateVideoJobRequest._Inner  # type: ignore[assignment,misc]


from pydantic import BaseModel as _BaseModel  # noqa: E402


class CreateVideoJobRequest(_BaseModel):  # type: ignore[no-redef]
    topic_id: uuid.UUID
    channel_id: uuid.UUID


@app.post("/jobs", response_model=VideoJobResponse)
async def create_video_job(
    payload: CreateVideoJobRequest,
    db: Session = Depends(get_db),
) -> VideoJobResponse:
    topic = db.get(Topic, payload.topic_id)
    if not topic:
        raise HTTPException(status_code=404, detail="Topic not found")
    channel = db.get(ChannelConfig, payload.channel_id)
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")

    vj = VideoJob(
        topic_id=payload.topic_id,
        channel_id=payload.channel_id,
        status="CREATED",
    )
    db.add(vj)
    db.flush()

    workflow_id = f"video-{vj.id}"
    try:
        client = await _temporal_client()
        await client.start_workflow(
            "VideoProduction",
            VideoProductionInput(
                job_id=str(vj.id),
                topic_id=str(payload.topic_id),
                channel_id=str(payload.channel_id),
            ).model_dump(mode="json"),
            id=workflow_id,
            task_queue=settings.temporal_task_queue,
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
        )
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=502, detail=f"Failed to start VideoProduction workflow: {exc}") from exc

    db.commit()
    db.refresh(vj)
    return _video_job_to_response(vj)


@app.get("/jobs", response_model=list[VideoJobResponse])
def list_video_jobs(
    channel: uuid.UUID | None = Query(default=None),
    status: str | None = Query(default=None),
    date: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[VideoJobResponse]:
    q = select(VideoJob)
    if channel:
        q = q.where(VideoJob.channel_id == channel)
    if status:
        q = q.where(VideoJob.status == status)
    if date:
        try:
            day = datetime.fromisoformat(date).replace(tzinfo=timezone.utc)
            q = q.where(
                and_(
                    VideoJob.created_at >= day,
                    VideoJob.created_at < day + timedelta(days=1),
                )
            )
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format — use ISO 8601")

    rows = db.execute(q.order_by(VideoJob.created_at.desc()).limit(100)).scalars().all()
    return [_video_job_to_response(vj) for vj in rows]


# ---------------------------------------------------------------------------
# Topics
# ---------------------------------------------------------------------------

@app.post("/topics", response_model=TopicResponse)
def ingest_topic(payload: TopicCandidate, db: Session = Depends(get_db)) -> TopicResponse:
    topic = Topic(
        headline=payload.headline,
        entities=payload.entities,
        why_now=payload.why_now,
        priority_score=payload.priority_score,
        expiry_window_hours=payload.expiry_window_hours,
        source_links=payload.source_links,
        status="pending",
    )
    db.add(topic)
    db.commit()
    db.refresh(topic)
    return TopicResponse(
        id=topic.id,
        headline=topic.headline,
        entities=topic.entities,
        why_now=topic.why_now,
        priority_score=topic.priority_score,
        expiry_window_hours=topic.expiry_window_hours,
        source_links=topic.source_links,
        status=topic.status,
        cluster_id=topic.cluster_id,
        created_at=topic.created_at,
        updated_at=topic.updated_at,
    )


@app.get("/topics", response_model=list[TopicResponse])
def list_topics(
    status: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[TopicResponse]:
    q = select(Topic)
    if status:
        q = q.where(Topic.status == status)
    rows = db.execute(q.order_by(Topic.priority_score.desc()).limit(100)).scalars().all()
    return [
        TopicResponse(
            id=t.id,
            headline=t.headline,
            entities=t.entities,
            why_now=t.why_now,
            priority_score=t.priority_score,
            expiry_window_hours=t.expiry_window_hours,
            source_links=t.source_links,
            status=t.status,
            cluster_id=t.cluster_id,
            created_at=t.created_at,
            updated_at=t.updated_at,
        )
        for t in rows
    ]


# ---------------------------------------------------------------------------
# Backlog
# ---------------------------------------------------------------------------

@app.get("/backlog", response_model=list[BacklogStatus])
def get_backlog(db: Session = Depends(get_db)) -> list[BacklogStatus]:
    rows = db.execute(select(BacklogStat)).scalars().all()
    return [
        BacklogStatus(
            channel_id=stat.channel_id,
            backlog_count=stat.backlog_count,
            mode=stat.mode,
            updated_at=stat.updated_at,
        )
        for stat in rows
    ]


class BacklogModeRequest(_BaseModel):
    mode: str  # backlog | maintenance


@app.post("/backlog/{channel_id}/mode")
def set_backlog_mode_endpoint(
    channel_id: uuid.UUID,
    payload: BacklogModeRequest,
    db: Session = Depends(get_db),
) -> dict[str, str]:
    if payload.mode not in ("backlog", "maintenance"):
        raise HTTPException(status_code=400, detail="mode must be 'backlog' or 'maintenance'")
    stat = db.get(BacklogStat, channel_id)
    if not stat:
        raise HTTPException(status_code=404, detail="BacklogStat not found for channel")
    stat.mode = payload.mode
    db.commit()
    return {"channel_id": str(channel_id), "mode": payload.mode}


# ---------------------------------------------------------------------------
# Gate B acknowledge
# ---------------------------------------------------------------------------

class GateBAckRequest(_BaseModel):
    job_id: uuid.UUID
    alert_type: str


@app.post("/gate-b/acknowledge")
def acknowledge_gate_b(payload: GateBAckRequest, db: Session = Depends(get_db)) -> dict[str, Any]:
    vj = db.get(VideoJob, payload.job_id)
    if not vj:
        raise HTTPException(status_code=404, detail="VideoJob not found")

    alerts = list(vj.gate_b_alerts or [])
    matched = 0
    for alert in alerts:
        if alert.get("alert_type") == payload.alert_type and not alert.get("acknowledged"):
            alert["acknowledged"] = True
            matched += 1

    vj.gate_b_alerts = alerts
    db.commit()
    return {"acknowledged": matched, "job_id": str(payload.job_id), "alert_type": payload.alert_type}


# ---------------------------------------------------------------------------
# Manual workflow trigger
# ---------------------------------------------------------------------------

class TrendIngestRequest(_BaseModel):
    count: int = 20
    trigger_production: bool = True


@app.post("/workflows/trend-ingest")
async def trigger_trend_ingest(payload: TrendIngestRequest) -> dict[str, str]:
    wf_id = f"trend-ingest-manual-{int(datetime.utcnow().timestamp())}"
    try:
        client = await _temporal_client()
        handle = await client.start_workflow(
            "TrendIngestion",
            {"count": payload.count, "trigger_production": payload.trigger_production},
            id=wf_id,
            task_queue=settings.temporal_task_queue,
        )
        return {"workflow_id": wf_id, "run_id": handle.run_id}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to start TrendIngestion: {exc}") from exc


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run() -> None:
    import uvicorn

    uvicorn.run(
        "openclaw_orchestrator_api.main:app",
        host=settings.orchestrator_host,
        port=settings.orchestrator_port,
        reload=False,
    )
