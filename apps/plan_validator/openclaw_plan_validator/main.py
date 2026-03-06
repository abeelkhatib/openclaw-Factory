from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import FastAPI
from sqlalchemy import create_engine, and_, select
from sqlalchemy.orm import sessionmaker

from openclaw_shared.db_models import ChannelConfig, VideoJob
from openclaw_shared.models import GateResult, ValidationResult, VideoProductionPlan

logger = logging.getLogger(__name__)
app = FastAPI(title="plan_validator", version="0.2.0")

# ---------------------------------------------------------------------------
# DB — uses DATABASE_URL env variable (same as other services)
# ---------------------------------------------------------------------------

import os

_DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://postgres:postgres@localhost:5432/openclaw",
)
_engine = create_engine(_DATABASE_URL, pool_pre_ping=True)
_Session = sessionmaker(bind=_engine, autoflush=False, autocommit=False)


def _get_session():
    return _Session()


# ---------------------------------------------------------------------------
# Gates
# ---------------------------------------------------------------------------

def _gate_script_present(plan: VideoProductionPlan) -> GateResult:
    if plan.script is None:
        return GateResult(passed=False, reason="script is missing")
    if not plan.script.hook or not plan.script.body or not plan.script.cta:
        return GateResult(passed=False, reason="script hook/body/cta has empty fields")
    return GateResult(passed=True, reason="script present with hook/body/cta")


def _gate_script_duration(plan: VideoProductionPlan) -> GateResult:
    if plan.script is None:
        return GateResult(passed=False, reason="script missing — cannot check duration")
    d = plan.script.duration_estimate_s
    if not (40 <= d <= 62):
        return GateResult(passed=False, reason=f"duration_estimate_s={d} not in [40, 62]")
    return GateResult(passed=True, reason=f"duration_estimate_s={d} is in range")


def _gate_clips_present(plan: VideoProductionPlan) -> GateResult:
    if not plan.clip_urls:
        return GateResult(passed=False, reason="no clip URLs in plan")
    return GateResult(passed=True, reason=f"{len(plan.clip_urls)} clip URL(s) present")


def _gate_vo_present(plan: VideoProductionPlan) -> GateResult:
    if not plan.vo_url:
        return GateResult(passed=False, reason="vo_url is missing")
    return GateResult(passed=True, reason="vo_url present")


def _gate_channel_valid(plan: VideoProductionPlan) -> tuple[GateResult, bool]:
    """Returns (GateResult, channel_exists)."""
    try:
        import uuid
        channel_uuid = uuid.UUID(plan.channel_id)
    except ValueError:
        return GateResult(passed=False, reason=f"channel_id '{plan.channel_id}' is not a valid UUID"), False

    try:
        session = _get_session()
        channel = session.get(ChannelConfig, channel_uuid)
        session.close()
        if channel is None:
            return GateResult(passed=False, reason=f"channel_id '{plan.channel_id}' not found in channel_configs"), False
        return GateResult(passed=True, reason=f"channel '{channel.name}' ({channel.platform}) found"), True
    except Exception as exc:
        logger.warning("[plan_validator] channel_valid gate DB error: %s", exc)
        return GateResult(passed=False, reason=f"DB error checking channel: {exc}"), False


def _gate_no_duplicate(plan: VideoProductionPlan) -> GateResult:
    """Check no VideoJob with same topic_id+channel_id in DONE status in last 7 days."""
    try:
        import uuid
        topic_uuid = uuid.UUID(plan.topic_id)
        channel_uuid = uuid.UUID(plan.channel_id)
    except ValueError:
        return GateResult(passed=True, reason="could not parse UUIDs — skipping duplicate check")

    try:
        session = _get_session()
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=7)
        existing = session.execute(
            select(VideoJob).where(
                and_(
                    VideoJob.topic_id == topic_uuid,
                    VideoJob.channel_id == channel_uuid,
                    VideoJob.status == "DONE",
                    VideoJob.created_at >= cutoff,
                )
            )
        ).scalars().first()
        session.close()

        if existing:
            return GateResult(
                passed=False,
                reason=f"Duplicate VideoJob {existing.id} (DONE) found for this topic+channel in the last 7 days",
            )
        return GateResult(passed=True, reason="No duplicate found in last 7 days")
    except Exception as exc:
        logger.warning("[plan_validator] no_duplicate gate DB error: %s", exc)
        return GateResult(passed=True, reason=f"DB error — skipping duplicate check: {exc}")


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/validate", response_model=ValidationResult)
def validate(plan: VideoProductionPlan) -> ValidationResult:
    gates: dict[str, GateResult] = {}
    warnings: list[str] = []

    # Gate: script_present
    gates["script_present"] = _gate_script_present(plan)

    # Gate: script_duration
    gates["script_duration"] = _gate_script_duration(plan)

    # Soft warning: qa_score
    if plan.script and plan.script.duration_estimate_s:
        # We don't have qa_score in VideoProductionPlan directly — emit a warning if script is borderline
        pass

    # Gate: clips_present
    gates["clips_present"] = _gate_clips_present(plan)

    # Gate: vo_present
    gates["vo_present"] = _gate_vo_present(plan)

    # Gate: channel_valid
    channel_gate, _channel_exists = _gate_channel_valid(plan)
    gates["channel_valid"] = channel_gate

    # Gate: no_duplicate
    gates["no_duplicate"] = _gate_no_duplicate(plan)

    # Soft warnings (non-blocking)
    if plan.script:
        word_count = len(f"{plan.script.hook} {plan.script.body} {plan.script.cta}".split())
        approx_duration = word_count / 2.5
        if approx_duration < 40:
            warnings.append(f"Estimated duration {approx_duration:.0f}s may be too short (word count: {word_count})")
        if len(plan.script.hook.split()) > 10:
            warnings.append(f"Hook is {len(plan.script.hook.split())} words — target ≤8 for best engagement")

    passed = all(g.passed for g in gates.values())
    return ValidationResult(passed=passed, gates=gates, warnings=warnings)


def run() -> None:
    import uvicorn

    uvicorn.run("openclaw_plan_validator.main:app", host="0.0.0.0", port=3001, reload=False)
