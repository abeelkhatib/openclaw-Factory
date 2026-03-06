from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from openclaw_shared.status import JobStatus


# ---------------------------------------------------------------------------
# Existing models (unchanged)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Topic models
# ---------------------------------------------------------------------------

class TopicCandidate(BaseModel):
    """Input from MiniMax ranking — a single pop culture topic candidate."""
    headline: str
    entities: list[str] = Field(default_factory=list)
    why_now: str
    priority_score: float = Field(ge=0.0, le=1.0)
    expiry_window_hours: int = Field(gt=0, default=24)
    source_links: list[str] = Field(default_factory=list)


class TopicResponse(BaseModel):
    id: UUID
    headline: str
    entities: list[str]
    why_now: str
    priority_score: float
    expiry_window_hours: int
    source_links: list[str]
    status: str
    cluster_id: UUID | None = None
    created_at: datetime
    updated_at: datetime


# ---------------------------------------------------------------------------
# Script models
# ---------------------------------------------------------------------------

class ScriptDraft(BaseModel):
    """Output from MiniMax script generation."""
    hook: str
    body: str
    cta: str
    full_text: str | None = None
    duration_estimate_s: int = 50


class ScriptResponse(BaseModel):
    id: UUID
    topic_id: UUID
    hook: str
    body: str
    cta: str
    full_text: str
    duration_estimate_s: int
    qa_score: float | None = None
    variant_of: UUID | None = None
    created_at: datetime


# ---------------------------------------------------------------------------
# QA models
# ---------------------------------------------------------------------------

class QAResult(BaseModel):
    """Output from Ollama QA rubric."""
    script_id: str
    hook_score: float = Field(ge=0.0, le=1.0)
    pacing_score: float = Field(ge=0.0, le=1.0)
    duration_estimate_s: int
    overall_score: float = Field(ge=0.0, le=1.0)
    notes: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Clip models
# ---------------------------------------------------------------------------

class ClipSegment(BaseModel):
    start: float
    end: float
    reason: str


class ClipCandidate(BaseModel):
    """Output from Vugola source scan."""
    source_id: str
    title: str
    duration: float
    thumbnail_url: str
    relevance_score: float = Field(ge=0.0, le=1.0)
    suggested_segments: list[ClipSegment] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Gate B alert
# ---------------------------------------------------------------------------

class GateBAlert(BaseModel):
    job_id: str
    alert_type: str  # hard_failure | quality_risk | cost_risk | verification_failure
    details: dict[str, Any]
    created_at: datetime = Field(default_factory=datetime.utcnow)
    acknowledged: bool = False


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

class VerificationResult(BaseModel):
    passed: bool
    duration_s: float
    aspect_ok: bool
    has_audio: bool
    loudness_lufs: float | None = None
    failures: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Video production plan (for validator)
# ---------------------------------------------------------------------------

class VideoProductionPlan(BaseModel):
    job_id: str
    topic_id: str
    channel_id: str
    script: ScriptDraft | None = None
    clip_urls: list[str] = Field(default_factory=list)
    vo_url: str | None = None


class GateResult(BaseModel):
    passed: bool
    reason: str


class ValidationResult(BaseModel):
    passed: bool
    gates: dict[str, GateResult]
    warnings: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Backlog
# ---------------------------------------------------------------------------

class BacklogStatus(BaseModel):
    channel_id: UUID
    backlog_count: int
    mode: str  # backlog | maintenance
    updated_at: datetime


# ---------------------------------------------------------------------------
# Channel config
# ---------------------------------------------------------------------------

class ChannelConfigResponse(BaseModel):
    id: UUID
    name: str
    platform: str
    daily_target: int
    posting_windows: list[dict[str, str]]


# ---------------------------------------------------------------------------
# Workflow inputs
# ---------------------------------------------------------------------------

class VideoProductionInput(BaseModel):
    job_id: str
    topic_id: str
    channel_id: str


class TrendIngestionInput(BaseModel):
    count: int = 20
    trigger_production: bool = True


class BacklogManagerInput(BaseModel):
    channel_id: str


# ---------------------------------------------------------------------------
# Video job response
# ---------------------------------------------------------------------------

class VideoJobResponse(BaseModel):
    id: UUID
    topic_id: UUID
    script_id: UUID | None = None
    channel_id: UUID
    status: str
    stage: str | None = None
    clip_urls: list[str] | None = None
    vo_url: str | None = None
    render_url: str | None = None
    publish_url: str | None = None
    error: str | None = None
    gate_b_alerts: list[dict] | None = None
    created_at: datetime
    updated_at: datetime
