from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from openclaw_shared.status import JobStatus


class JobStartRequest(BaseModel):
    topic_clusters: list[str] = Field(min_length=1)
    videos_per_channel: int = Field(gt=0)
    channels: list[str] = Field(min_length=1)


class JobStartResponse(BaseModel):
    job_id: UUID
    status: JobStatus
    workflow_id: str | None = None
    workflow_run_id: str | None = None


class JobRecordResponse(BaseModel):
    id: UUID
    status: JobStatus
    config_json: dict[str, Any]
    created_at: datetime
    updated_at: datetime
