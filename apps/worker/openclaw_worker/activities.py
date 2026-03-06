from __future__ import annotations

import asyncio
import json
import logging
import random
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import httpx
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from temporalio import activity
from temporalio.common import RetryPolicy

from openclaw_shared.cache import get_cached, get_cached_vo, set_cached, set_cached_vo
from openclaw_shared.db_models import (
    BacklogStat,
    ChannelConfig,
    Job,
    Script,
    Topic,
    VideoJob,
)
from openclaw_shared.models import (
    ClipCandidate,
    ClipSegment,
    QAResult,
    ScriptDraft,
    TopicCandidate,
    VerificationResult,
)
from openclaw_shared.state_machine import validate_transition
from openclaw_shared.status import JobStatus
from openclaw_shared.storage import download_file, upload_bytes
from openclaw_worker.config import settings

logger = logging.getLogger(__name__)

engine = create_engine(settings.database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

# ---------------------------------------------------------------------------
# Retry policies
# ---------------------------------------------------------------------------

STANDARD_RETRY = RetryPolicy(
    maximum_attempts=3,
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
)

EXTERNAL_API_RETRY = RetryPolicy(
    maximum_attempts=5,
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=60),
)

NO_RETRY = RetryPolicy(maximum_attempts=1)


# ---------------------------------------------------------------------------
# DB sync helpers
# ---------------------------------------------------------------------------

def _get_topic_sync(topic_id: str) -> Topic:
    with SessionLocal() as session:
        topic = session.get(Topic, UUID(topic_id))
        if topic is None:
            raise ValueError(f"Topic not found: {topic_id}")
        session.expunge(topic)
        return topic


def _get_script_sync(script_id: str) -> Script:
    with SessionLocal() as session:
        script = session.get(Script, UUID(script_id))
        if script is None:
            raise ValueError(f"Script not found: {script_id}")
        session.expunge(script)
        return script


def _get_video_job_sync(job_id: str) -> VideoJob:
    with SessionLocal() as session:
        job = session.get(VideoJob, UUID(job_id))
        if job is None:
            raise ValueError(f"VideoJob not found: {job_id}")
        session.expunge(job)
        return job


def _get_channel_config_sync(channel_id: str) -> ChannelConfig:
    with SessionLocal() as session:
        channel = session.get(ChannelConfig, UUID(channel_id))
        if channel is None:
            raise ValueError(f"ChannelConfig not found: {channel_id}")
        session.expunge(channel)
        return channel


# ---------------------------------------------------------------------------
# Legacy activities (kept, signatures unchanged)
# ---------------------------------------------------------------------------

@activity.defn
async def emit_status(job_id: str, step: str, state: str, detail: str | None = None) -> None:
    payload = {"job_id": job_id, "step": step, "state": state, "detail": detail}
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            await client.post(settings.n8n_status_webhook_url, json=payload)
        except Exception:
            logger.warning("[activity=emit_status] [job_id=%s] webhook failed (non-fatal)", job_id)


@activity.defn
async def emit_exception(job_id: str, error: str, detail: str | None = None) -> None:
    payload = {
        "job_id": job_id,
        "error": error,
        "detail": detail,
        "classification": "failed_after_retries",
    }
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            await client.post(settings.n8n_exceptions_webhook_url, json=payload)
        except Exception:
            logger.warning("[activity=emit_exception] [job_id=%s] webhook failed (non-fatal)", job_id)


@activity.defn
async def set_job_status(job_id: str, target_status: str) -> None:
    def _update() -> None:
        with SessionLocal() as session:
            # Try VideoJob first
            vj = session.get(VideoJob, UUID(job_id))
            if vj is not None:
                vj.status = target_status
                session.commit()
                return
            # Fall back to legacy Job table
            legacy_job = session.get(Job, UUID(job_id))
            if legacy_job is None:
                return
            current = JobStatus(legacy_job.status)
            next_status = JobStatus(target_status)
            validate_transition(current, next_status)
            legacy_job.status = next_status.value
            session.commit()

    await asyncio.to_thread(_update)


@activity.defn
async def stub_step(job_id: str, name: str) -> None:
    del job_id
    if settings.fail_mode and name == "clip":
        raise RuntimeError("FAIL_MODE triggered clip step failure")
    await asyncio.sleep(random.uniform(1.0, 2.0))


# ---------------------------------------------------------------------------
# VideoJob updater (takes dict of updates, not **kwargs)
# ---------------------------------------------------------------------------

@activity.defn
async def update_video_job(job_id: str, updates: dict) -> None:
    """Update VideoJob fields. Pass a dict of column_name -> value."""
    def _update() -> None:
        with SessionLocal() as session:
            job = session.get(VideoJob, UUID(job_id))
            if job is None:
                logger.warning("[activity=update_video_job] [job_id=%s] VideoJob not found", job_id)
                return
            for k, v in updates.items():
                setattr(job, k, v)
            session.commit()

    await asyncio.to_thread(_update)


# ---------------------------------------------------------------------------
# Channel / Backlog helpers
# ---------------------------------------------------------------------------

@activity.defn
async def list_active_channels() -> list[str]:
    """Return list of active channel_config IDs (all channels for now)."""
    def _query() -> list[str]:
        with SessionLocal() as session:
            rows = session.execute(select(ChannelConfig.id)).scalars().all()
            return [str(r) for r in rows]

    return await asyncio.to_thread(_query)


@activity.defn
async def update_backlog_stats(channel_ids: list[str], delta: int) -> None:
    def _update() -> None:
        with SessionLocal() as session:
            for cid in channel_ids:
                stat = session.get(BacklogStat, UUID(cid))
                if stat:
                    stat.backlog_count = max(0, stat.backlog_count + delta)
                    session.commit()

    await asyncio.to_thread(_update)


@activity.defn
async def get_backlog_stat(channel_id: str) -> dict:
    def _query() -> dict:
        with SessionLocal() as session:
            stat = session.get(BacklogStat, UUID(channel_id))
            if stat is None:
                return {"backlog_count": 0, "mode": "maintenance", "channel_id": channel_id}
            return {
                "backlog_count": stat.backlog_count,
                "mode": stat.mode,
                "channel_id": channel_id,
            }

    return await asyncio.to_thread(_query)


@activity.defn
async def set_backlog_mode(channel_id: str, mode: str) -> None:
    def _update() -> None:
        with SessionLocal() as session:
            stat = session.get(BacklogStat, UUID(channel_id))
            if stat:
                stat.mode = mode
                session.commit()

    await asyncio.to_thread(_update)


# ---------------------------------------------------------------------------
# Ollama JSON repair helper
# ---------------------------------------------------------------------------

def _repair_json_with_ollama(raw: str) -> Any:
    """Use Ollama to repair malformed JSON. Returns parsed object."""
    import urllib.request

    prompt = (
        "The following text should be valid JSON but may be malformed. "
        "Return ONLY the corrected JSON, no explanation:\n\n" + raw
    )
    payload = json.dumps({"model": settings.ollama_model, "prompt": prompt, "stream": False}).encode()
    req = urllib.request.Request(
        f"{settings.ollama_base_url}/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
            return json.loads(data.get("response", "null"))
    except Exception:
        logger.exception("[ollama-repair] Failed to repair JSON via Ollama")
        return None


# ---------------------------------------------------------------------------
# Trend ingestion
# ---------------------------------------------------------------------------

@activity.defn
async def fetch_trending_topics(count: int = 20) -> list[dict]:
    """
    Call MiniMax M2.5 to generate ranked pop culture topics.
    Returns list of TopicCandidate dicts (JSON-serializable for Temporal).
    """
    logger.info("[activity=fetch_trending_topics] Fetching %d topics", count)

    if not settings.minimax_api_key:
        # STUB: MiniMax — set MINIMAX_API_KEY to enable
        logger.info("[activity=fetch_trending_topics] STUB: MiniMax — set MINIMAX_API_KEY to enable")
        sample = [
            ("Celebrity Feuds That Shocked Everyone This Week", ["celebrity", "drama"],
             "trending on social media", 0.95, 48),
            ("Top 10 Movie Moments That Broke the Internet", ["movies", "viral"],
             "anniversary of iconic scenes", 0.88, 72),
            ("Music Album Drop Rankings: Who Won This Month", ["music", "albums"],
             "monthly music review season", 0.82, 96),
            ("Sports GOAT Debates Heating Up Again", ["sports", "debate"],
             "playoff season incoming", 0.78, 48),
            ("TV Show Cancellations That Fans Are Still Angry About", ["TV", "cancellation"],
             "streaming wars peak", 0.72, 120),
        ]
        return [
            TopicCandidate(
                headline=h, entities=e, why_now=w,
                priority_score=s, expiry_window_hours=ex, source_links=[],
            ).model_dump(mode="json")
            for h, e, w, s, ex in sample[:count]
        ]

    system_prompt = (
        "You are a pop culture trend analyst for short-form video. "
        "Return ONLY a valid JSON array of topic objects, no markdown, no explanation. "
        "Each object: headline (string, ≤10 words), entities (string array), why_now (string), "
        "priority_score (float 0–1), expiry_window_hours (integer), source_links (string array). "
        "Topics must be real pop culture: celebrity drama, beefs, anniversaries, controversies, "
        "viral moments, ranking debates. Order by priority_score descending."
    )
    user_prompt = (
        f"Generate {count} pop culture short-form video topics. "
        "Return only the JSON array."
    )

    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(
            f"{settings.minimax_api_base}/text/chatcompletion_v2",
            headers={"Authorization": f"Bearer {settings.minimax_api_key}"},
            json={
                "model": settings.minimax_model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": 0.8,
                "max_tokens": 4000,
            },
        )
        resp.raise_for_status()
        data = resp.json()

    raw = data["choices"][0]["message"]["content"]
    try:
        topics_raw = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("[activity=fetch_trending_topics] JSON parse failed — trying Ollama repair")
        topics_raw = await asyncio.to_thread(_repair_json_with_ollama, raw) or []

    result = []
    for item in topics_raw[:count]:
        try:
            result.append(TopicCandidate(**item).model_dump(mode="json"))
        except Exception:
            logger.warning("[activity=fetch_trending_topics] Skipping malformed topic: %s", item)

    logger.info("[activity=fetch_trending_topics] Returning %d topics", len(result))
    return result


@activity.defn
async def ingest_topics(topics: list[dict]) -> list[str]:
    """Upsert TopicCandidates to topics table. Return list of topic UUIDs."""
    logger.info("[activity=ingest_topics] Ingesting %d topics", len(topics))

    def _upsert() -> list[str]:
        ids: list[str] = []
        with SessionLocal() as session:
            for item in topics:
                try:
                    candidate = TopicCandidate(**item)
                except Exception:
                    logger.warning("[activity=ingest_topics] Skipping malformed candidate")
                    continue
                normalized = candidate.headline.lower().strip()
                existing = (
                    session.query(Topic)
                    .filter(Topic.headline.ilike(normalized))
                    .first()
                )
                if existing:
                    existing.priority_score = candidate.priority_score
                    existing.why_now = candidate.why_now
                    existing.source_links = candidate.source_links
                    ids.append(str(existing.id))
                else:
                    topic = Topic(
                        headline=candidate.headline,
                        entities=candidate.entities,
                        why_now=candidate.why_now,
                        priority_score=candidate.priority_score,
                        expiry_window_hours=candidate.expiry_window_hours,
                        source_links=candidate.source_links,
                        status="pending",
                    )
                    session.add(topic)
                    session.flush()
                    ids.append(str(topic.id))
            session.commit()
        return ids

    return await asyncio.to_thread(_upsert)


# ---------------------------------------------------------------------------
# Script generation
# ---------------------------------------------------------------------------

@activity.defn
async def generate_script(topic_id: str) -> str:
    """Generate a pop culture script via MiniMax. Returns script_id as str."""
    logger.info("[activity=generate_script] [topic_id=%s] Generating script", topic_id)
    topic = await asyncio.to_thread(_get_topic_sync, topic_id)

    if not settings.minimax_api_key:
        # STUB: MiniMax — set MINIMAX_API_KEY to enable
        logger.info("[activity=generate_script] STUB: MiniMax — set MINIMAX_API_KEY to enable")
        draft = ScriptDraft(
            hook=f"You won't believe what just happened with {', '.join(topic.entities[:2]) or topic.headline[:30]}.",
            body=(
                "Beat 1: The story nobody saw coming started just days ago. "
                "Beat 2: The internet exploded with millions of hot takes overnight. "
                "Beat 3: Insiders are finally speaking out about what really went down. "
                "Beat 4: And the fallout is only just beginning."
            ),
            cta="Follow for the pop culture drops you actually need to know about.",
            duration_estimate_s=52,
        )
    else:
        system_prompt = (
            "You are a viral pop culture short-form video scriptwriter. "
            "Return ONLY a JSON object: hook, body, cta, full_text, duration_estimate_s. "
            "Rules: hook ≤8 words, curiosity gap. body = 3-4 beats, each ≤2 sentences. "
            "cta = 1 sentence, platform-native. full_text = hook+body+cta for TTS. "
            "duration_estimate_s = word_count/2.5, target 45-55s."
        )
        user_prompt = (
            f"Write a viral pop culture script about: {topic.headline}\n"
            f"Context: {topic.why_now}\nKey entities: {', '.join(topic.entities)}"
        )
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                f"{settings.minimax_api_base}/text/chatcompletion_v2",
                headers={"Authorization": f"Bearer {settings.minimax_api_key}"},
                json={
                    "model": settings.minimax_model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "temperature": 0.85,
                    "max_tokens": 1500,
                },
            )
            resp.raise_for_status()
            data = resp.json()

        raw = data["choices"][0]["message"]["content"]
        try:
            draft = ScriptDraft(**json.loads(raw))
        except Exception:
            logger.warning("[activity=generate_script] JSON parse failed — trying Ollama repair")
            repaired = await asyncio.to_thread(_repair_json_with_ollama, raw)
            if not repaired:
                raise ValueError("Script generation failed — Ollama repair returned empty result")
            draft = ScriptDraft(**(repaired[0] if isinstance(repaired, list) else repaired))

    if not draft.full_text:
        draft.full_text = f"{draft.hook} {draft.body} {draft.cta}"

    def _store() -> str:
        with SessionLocal() as session:
            script = Script(
                topic_id=UUID(topic_id),
                hook=draft.hook,
                body=draft.body,
                cta=draft.cta,
                full_text=draft.full_text,
                duration_estimate_s=draft.duration_estimate_s,
            )
            session.add(script)
            session.flush()
            sid = str(script.id)
            t = session.get(Topic, UUID(topic_id))
            if t:
                t.status = "scripted"
            session.commit()
        return sid

    script_id = await asyncio.to_thread(_store)
    logger.info("[activity=generate_script] [topic_id=%s] Script stored: %s", topic_id, script_id)
    return script_id


@activity.defn
async def qa_script(script_id: str) -> dict:
    """QA a script via Ollama. Returns QAResult dict."""
    logger.info("[activity=qa_script] [script_id=%s] Running QA", script_id)
    script = await asyncio.to_thread(_get_script_sync, script_id)

    word_count = len(script.full_text.split())
    duration_estimate = int(word_count / 2.5)

    prompt = (
        f"Evaluate this pop culture short-form video script. "
        f"Return ONLY JSON: hook_score (0-1), pacing_score (0-1), "
        f"duration_estimate_s (int at 150wpm), overall_score (weighted avg), notes (array).\n\n"
        f"HOOK: {script.hook}\nBODY: {script.body}\nCTA: {script.cta}\nWord count: {word_count}"
    )

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{settings.ollama_base_url}/api/generate",
                json={"model": settings.ollama_model, "prompt": prompt, "stream": False},
            )
            resp.raise_for_status()
            data = resp.json()
        qa_raw = json.loads(data.get("response", "{}"))
        result = QAResult(
            script_id=script_id,
            hook_score=float(qa_raw.get("hook_score", 0.7)),
            pacing_score=float(qa_raw.get("pacing_score", 0.7)),
            duration_estimate_s=int(qa_raw.get("duration_estimate_s", duration_estimate)),
            overall_score=float(qa_raw.get("overall_score", 0.7)),
            notes=qa_raw.get("notes", []),
        )
    except Exception:
        logger.warning("[activity=qa_script] [script_id=%s] Ollama QA failed — using fallback", script_id)
        result = QAResult(
            script_id=script_id,
            hook_score=0.7,
            pacing_score=0.7,
            duration_estimate_s=duration_estimate,
            overall_score=0.7,
            notes=["QA ran in fallback mode — Ollama unavailable"],
        )

    def _update_score() -> None:
        with SessionLocal() as session:
            s = session.get(Script, UUID(script_id))
            if s:
                s.qa_score = result.overall_score
                s.duration_estimate_s = result.duration_estimate_s
                session.commit()

    await asyncio.to_thread(_update_score)
    logger.info("[activity=qa_script] [script_id=%s] QA score: %.2f", script_id, result.overall_score)
    return result.model_dump(mode="json")


@activity.defn
async def generate_variants(script_id: str, count: int = 2) -> list[str]:
    """Generate script variants via Ollama. Returns list of new script_ids."""
    logger.info("[activity=generate_variants] [script_id=%s] Generating %d variants", script_id, count)
    script = await asyncio.to_thread(_get_script_sync, script_id)

    variant_ids: list[str] = []
    for i in range(count):
        prompt = (
            f"Rewrite this pop culture video script with a DIFFERENT hook angle, "
            f"same body facts, same CTA energy. Return ONLY JSON: hook, body, cta, full_text, duration_estimate_s.\n\n"
            f"Original hook: {script.hook}\nBody: {script.body}\nCTA: {script.cta}"
        )
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    f"{settings.ollama_base_url}/api/generate",
                    json={"model": settings.ollama_model, "prompt": prompt, "stream": False},
                )
                resp.raise_for_status()
                data = resp.json()
            draft = ScriptDraft(**json.loads(data.get("response", "{}")))
        except Exception:
            logger.warning("[activity=generate_variants] Variant %d generation failed — using permutation", i)
            draft = ScriptDraft(
                hook=f"Nobody is talking about this: {script.hook}",
                body=script.body,
                cta=script.cta,
                duration_estimate_s=script.duration_estimate_s,
            )

        if not draft.full_text:
            draft.full_text = f"{draft.hook} {draft.body} {draft.cta}"

        def _store(d: ScriptDraft = draft, topic_id: UUID = script.topic_id, parent: str = script_id) -> str:
            with SessionLocal() as session:
                v = Script(
                    topic_id=topic_id,
                    hook=d.hook,
                    body=d.body,
                    cta=d.cta,
                    full_text=d.full_text,
                    duration_estimate_s=d.duration_estimate_s,
                    variant_of=UUID(parent),
                )
                session.add(v)
                session.flush()
                vid = str(v.id)
                session.commit()
            return vid

        variant_ids.append(await asyncio.to_thread(_store))

    logger.info("[activity=generate_variants] [script_id=%s] Created: %s", script_id, variant_ids)
    return variant_ids


@activity.defn
async def embed_script(script_id: str) -> None:
    """Embed script.full_text via MiniMax and store in pgvector."""
    if not settings.minimax_api_key:
        logger.info("[activity=embed_script] STUB: MiniMax — set MINIMAX_API_KEY to enable")
        return

    logger.info("[activity=embed_script] [script_id=%s] Generating embedding", script_id)
    script = await asyncio.to_thread(_get_script_sync, script_id)

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{settings.minimax_api_base}/embeddings",
            headers={"Authorization": f"Bearer {settings.minimax_api_key}"},
            json={"model": settings.minimax_embedding_model, "input": [script.full_text]},
        )
        resp.raise_for_status()
        embedding = resp.json()["data"][0]["embedding"]

    def _store() -> None:
        with SessionLocal() as session:
            s = session.get(Script, UUID(script_id))
            if s:
                s.embedding = embedding
                session.commit()

    await asyncio.to_thread(_store)
    logger.info("[activity=embed_script] [script_id=%s] Embedding stored dim=%d", script_id, len(embedding))


# ---------------------------------------------------------------------------
# Clip selection
# ---------------------------------------------------------------------------

@activity.defn
async def scan_sources(topic_id: str) -> list[dict]:
    """Scan Vugola for clip sources matching topic. Returns list of ClipCandidate dicts."""
    logger.info("[activity=scan_sources] [topic_id=%s] Scanning sources", topic_id)
    topic = await asyncio.to_thread(_get_topic_sync, topic_id)

    if not settings.vugola_api_key:
        # STUB: Vugola — set VUGOLA_API_KEY to enable
        logger.info("[activity=scan_sources] STUB: Vugola — set VUGOLA_API_KEY to enable")
        # TODO(external): Vugola key needed
        return [
            ClipCandidate(
                source_id=f"stub-source-{i}",
                title=f"Stub clip {i}: {topic.headline[:40]}",
                duration=90.0,
                thumbnail_url=f"https://example.com/thumb_{i}.jpg",
                relevance_score=round(0.9 - i * 0.1, 2),
                suggested_segments=[ClipSegment(start=0.0, end=15.0, reason="opening context")],
            ).model_dump(mode="json")
            for i in range(3)
        ]

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{settings.vugola_api_base}/sources/search",
            headers={"Authorization": f"Bearer {settings.vugola_api_key}"},
            json={"query": topic.headline, "entities": topic.entities, "limit": 10},
        )
        resp.raise_for_status()
        data = resp.json()

    candidates = [ClipCandidate(**item) for item in data.get("results", [])]
    logger.info("[activity=scan_sources] [topic_id=%s] Found %d candidates", topic_id, len(candidates))
    return [c.model_dump(mode="json") for c in candidates]


@activity.defn
async def export_clip(source_id: str, start: float, end: float, job_id: str) -> str:
    """Export clip from Vugola, upload to R2, return R2 URL."""
    logger.info("[activity=export_clip] [job_id=%s] Exporting %s [%.1f-%.1f]", job_id, source_id, start, end)

    if not settings.vugola_api_key:
        # STUB: Vugola — set VUGOLA_API_KEY to enable
        logger.info("[activity=export_clip] STUB: Vugola — set VUGOLA_API_KEY to enable")
        # TODO(external): Vugola key needed
        return f"r2://openclaw-assets/clips/{job_id}/{source_id}_{start}_{end}.mp4"

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{settings.vugola_api_base}/clips/export",
            headers={"Authorization": f"Bearer {settings.vugola_api_key}"},
            json={"source_id": source_id, "start": start, "end": end},
        )
        resp.raise_for_status()
        export_id = resp.json()["export_id"]

        for _ in range(12):  # max 60s
            await asyncio.sleep(5)
            sr = await client.get(
                f"{settings.vugola_api_base}/clips/export/{export_id}",
                headers={"Authorization": f"Bearer {settings.vugola_api_key}"},
            )
            sr.raise_for_status()
            sd = sr.json()
            if sd["status"] == "done":
                dl_url = sd["download_url"]
                break
            if sd["status"] == "failed":
                raise RuntimeError(f"Vugola export failed: {sd.get('error')}")
        else:
            raise TimeoutError(f"Vugola export {export_id} timed out")

        clip_resp = await client.get(dl_url)
        clip_resp.raise_for_status()
        clip_bytes = clip_resp.content

    r2_key = f"clips/{job_id}/{source_id}_{start}_{end}.mp4"
    r2_url = await upload_bytes(clip_bytes, r2_key, content_type="video/mp4")
    logger.info("[activity=export_clip] [job_id=%s] Clip uploaded: %s", job_id, r2_url)
    return r2_url


# ---------------------------------------------------------------------------
# Voiceover
# ---------------------------------------------------------------------------

@activity.defn
async def generate_voiceover(script_id: str, job_id: str) -> str:
    """Generate TTS via MiniMax Speech, upload to R2, return R2 URL."""
    logger.info("[activity=generate_voiceover] [job_id=%s] [script_id=%s]", job_id, script_id)
    script = await asyncio.to_thread(_get_script_sync, script_id)

    cached = await get_cached_vo(script.full_text)
    if cached:
        logger.info("[activity=generate_voiceover] [job_id=%s] Cache hit", job_id)
        return cached

    if not settings.minimax_api_key:
        # STUB: MiniMax — set MINIMAX_API_KEY to enable
        logger.info("[activity=generate_voiceover] STUB: MiniMax — set MINIMAX_API_KEY to enable")
        # TODO(external): MiniMax key needed
        r2_url = f"r2://openclaw-assets/vo/{job_id}/{script_id}.mp3"
        await set_cached_vo(script.full_text, r2_url, ttl_days=settings.vo_cache_ttl_days)
        return r2_url

    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(
            f"{settings.minimax_api_base}/text_to_speech",
            headers={"Authorization": f"Bearer {settings.minimax_api_key}"},
            json={
                "model": settings.minimax_speech_model,
                "text": script.full_text,
                "voice_setting": {"voice_id": "energetic_narrator", "speed": 1.05, "vol": 1.0, "pitch": 0},
                "audio_setting": {"format": "mp3", "sample_rate": 32000, "bitrate": 128000},
            },
        )
        resp.raise_for_status()
        audio_data = resp.json()

    import base64
    audio_bytes = base64.b64decode(audio_data.get("audio", "")) or \
                  bytes.fromhex(audio_data.get("audio_hex", ""))

    r2_key = f"vo/{job_id}/{script_id}.mp3"
    r2_url = await upload_bytes(audio_bytes, r2_key, content_type="audio/mpeg")
    await set_cached_vo(script.full_text, r2_url, ttl_days=settings.vo_cache_ttl_days)
    logger.info("[activity=generate_voiceover] [job_id=%s] VO uploaded: %s", job_id, r2_url)
    return r2_url


# ---------------------------------------------------------------------------
# Assembly & Verification
# ---------------------------------------------------------------------------

@activity.defn
async def render_video(job_id: str) -> str:
    """POST to Remotion, poll, upload to R2. Returns R2 URL."""
    logger.info("[activity=render_video] [job_id=%s] Starting render", job_id)

    vj = await asyncio.to_thread(_get_video_job_sync, job_id)
    if not vj.script_id:
        raise ValueError(f"VideoJob {job_id} has no script_id")
    script = await asyncio.to_thread(_get_script_sync, str(vj.script_id))

    wps = 2.5
    hook_words = len(script.hook.split())
    body_words = len(script.body.split())
    cta_words = len(script.cta.split())
    hook_end_ms = int(hook_words / wps * 1000)
    body_end_ms = hook_end_ms + int(body_words / wps * 1000)
    cta_end_ms = body_end_ms + int(cta_words / wps * 1000)

    captions = [
        {"text": script.hook, "start_ms": 0, "end_ms": hook_end_ms},
        {"text": script.body, "start_ms": hook_end_ms, "end_ms": body_end_ms},
        {"text": script.cta, "start_ms": body_end_ms, "end_ms": cta_end_ms},
    ]

    render_payload = {
        "template": settings.remotion_template,
        "props": {
            "clips": vj.clip_urls or [],
            "voiceover": vj.vo_url or "",
            "captions": captions,
            "format": "9:16",
            "duration_s": script.duration_estimate_s,
        },
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.post(f"{settings.remotion_api_base}/render", json=render_payload)
            resp.raise_for_status()
        except httpx.ConnectError as exc:
            # TODO(external): Remotion server needed
            raise RuntimeError(
                f"Remotion server unreachable at {settings.remotion_api_base}"
            ) from exc

        render_id = resp.json()["render_id"]

        for _ in range(60):  # max 300s
            await asyncio.sleep(5)
            sr = await client.get(f"{settings.remotion_api_base}/render/{render_id}/status")
            sr.raise_for_status()
            sd = sr.json()
            if sd["status"] == "done":
                output_url = sd["output_url"]
                break
            if sd["status"] == "failed":
                raise RuntimeError(f"Remotion render failed: {sd.get('error')}")
        else:
            raise TimeoutError(f"Remotion render {render_id} timed out")

        video_resp = await client.get(output_url)
        video_resp.raise_for_status()
        video_bytes = video_resp.content

    r2_key = f"renders/{job_id}/final.mp4"
    r2_url = await upload_bytes(video_bytes, r2_key, content_type="video/mp4")

    def _update() -> None:
        with SessionLocal() as session:
            j = session.get(VideoJob, UUID(job_id))
            if j:
                j.render_url = r2_url
                session.commit()

    await asyncio.to_thread(_update)
    logger.info("[activity=render_video] [job_id=%s] Render uploaded: %s", job_id, r2_url)
    return r2_url


@activity.defn
async def verify_output(job_id: str) -> dict:
    """Run FFprobe on rendered video and verify specs. Returns VerificationResult dict."""
    logger.info("[activity=verify_output] [job_id=%s] Verifying output", job_id)
    vj = await asyncio.to_thread(_get_video_job_sync, job_id)
    if not vj.render_url:
        raise ValueError(f"VideoJob {job_id} has no render_url")

    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        if vj.render_url.startswith("r2://"):
            r2_key = "/".join(vj.render_url.split("/")[3:])
            await download_file(r2_key, tmp_path)
        else:
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.get(vj.render_url)
                resp.raise_for_status()
                with open(tmp_path, "wb") as f:
                    f.write(resp.content)

        # FFprobe
        ffprobe = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_streams", "-show_format", tmp_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(ffprobe.communicate(), timeout=30)
        probe = json.loads(stdout)

        duration_s = float(probe.get("format", {}).get("duration", 0))
        streams = probe.get("streams", [])
        v_stream = next((s for s in streams if s.get("codec_type") == "video"), None)
        a_stream = next((s for s in streams if s.get("codec_type") == "audio"), None)

        width = int(v_stream.get("width", 0)) if v_stream else 0
        height = int(v_stream.get("height", 0)) if v_stream else 0
        aspect_ok = (height > 0 and abs(width / height - 9 / 16) < 0.05) if (width and height) else False
        has_audio = a_stream is not None

        # Loudness
        loudness_lufs: float | None = None
        try:
            lnorm = await asyncio.create_subprocess_exec(
                "ffmpeg", "-i", tmp_path,
                "-filter:a", "loudnorm=print_format=json",
                "-f", "null", "-",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, lnorm_stderr = await asyncio.wait_for(lnorm.communicate(), timeout=60)
            stderr_txt = lnorm_stderr.decode("utf-8", errors="replace")
            js = stderr_txt.rfind("{")
            je = stderr_txt.rfind("}") + 1
            if js >= 0 and je > js:
                ldata = json.loads(stderr_txt[js:je])
                loudness_lufs = float(ldata.get("input_i", -99))
        except Exception:
            logger.warning("[activity=verify_output] [job_id=%s] Loudness analysis failed", job_id)

        failures: list[str] = []
        if not (43 <= duration_s <= 62):
            failures.append(f"Duration {duration_s:.1f}s not in [43, 62]")
        if not aspect_ok:
            failures.append(f"Aspect ratio not 9:16 (got {width}x{height})")
        if not has_audio:
            failures.append("No audio stream")
        if loudness_lufs is not None and not (-20 <= loudness_lufs <= -12):
            failures.append(f"Loudness {loudness_lufs:.1f} LUFS not in [-20, -12]")

        result = VerificationResult(
            passed=not failures,
            duration_s=duration_s,
            aspect_ok=aspect_ok,
            has_audio=has_audio,
            loudness_lufs=loudness_lufs,
            failures=failures,
        )

        if failures:
            raise ValueError(str(failures))

        logger.info("[activity=verify_output] [job_id=%s] Passed", job_id)
        return result.model_dump(mode="json")

    finally:
        import os
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Publishing
# ---------------------------------------------------------------------------

@activity.defn
async def schedule_post(job_id: str, channel_id: str) -> str:
    """Schedule a post via Vugola. Returns platform post URL."""
    logger.info("[activity=schedule_post] [job_id=%s] [channel_id=%s]", job_id, channel_id)

    vj = await asyncio.to_thread(_get_video_job_sync, job_id)
    channel = await asyncio.to_thread(_get_channel_config_sync, channel_id)
    topic = await asyncio.to_thread(_get_topic_sync, str(vj.topic_id))

    script = None
    if vj.script_id:
        script = await asyncio.to_thread(_get_script_sync, str(vj.script_id))

    today = datetime.now(tz=timezone.utc).date().isoformat()
    post_count_key = f"posts:{channel_id}:{today}"
    post_count = int(await get_cached(post_count_key) or 0)

    windows = channel.posting_windows or [{"start": "09:00", "end": "21:00"}]
    _window = windows[post_count % len(windows)]  # noqa: F841 (used for future scheduling)
    scheduled_at = datetime.now(tz=timezone.utc) + timedelta(hours=random.randint(1, 4))

    caption = f"{script.hook} {script.cta}" if script else topic.headline
    hashtags = [f"#{e.replace(' ', '')}" for e in (topic.entities or [])[:5]]

    if not settings.vugola_api_key:
        # STUB: Vugola — set VUGOLA_API_KEY to enable
        logger.info("[activity=schedule_post] STUB: Vugola — set VUGOLA_API_KEY to enable")
        # TODO(external): Vugola key needed
        publish_url = f"https://platform.example.com/posts/{job_id}"
    else:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{settings.vugola_api_base}/publish",
                headers={"Authorization": f"Bearer {settings.vugola_api_key}"},
                json={
                    "video_url": vj.render_url,
                    "channel_id": channel_id,
                    "scheduled_at": scheduled_at.isoformat(),
                    "caption": caption,
                    "hashtags": hashtags,
                },
            )
            resp.raise_for_status()
            publish_url = resp.json().get("post_url", f"https://platform.example.com/posts/{job_id}")

    def _update() -> None:
        with SessionLocal() as session:
            j = session.get(VideoJob, UUID(job_id))
            if j:
                j.publish_url = publish_url
                j.status = "DONE"
                session.commit()

    await asyncio.to_thread(_update)
    await set_cached(post_count_key, post_count + 1, ttl_s=48 * 3600)
    logger.info("[activity=schedule_post] [job_id=%s] Published: %s", job_id, publish_url)
    return publish_url


# ---------------------------------------------------------------------------
# Gate B
# ---------------------------------------------------------------------------

@activity.defn
async def emit_gate_b_alert(job_id: str, alert_type: str, details: dict) -> None:
    """Emit Gate B alert to n8n and append to VideoJob."""
    logger.warning(
        "[activity=emit_gate_b_alert] [job_id=%s] alert_type=%s", job_id, alert_type,
    )
    alert = {
        "job_id": job_id,
        "alert_type": alert_type,
        "details": details,
        "created_at": datetime.utcnow().isoformat(),
        "acknowledged": False,
    }
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            await client.post(
                settings.n8n_exceptions_webhook_url,
                json={**alert, "classification": alert_type},
            )
        except Exception:
            logger.warning("[activity=emit_gate_b_alert] [job_id=%s] n8n webhook failed (non-fatal)", job_id)

    def _store() -> None:
        with SessionLocal() as session:
            j = session.get(VideoJob, UUID(job_id))
            if j:
                existing = list(j.gate_b_alerts or [])
                existing.append(alert)
                j.gate_b_alerts = existing
                session.commit()

    await asyncio.to_thread(_store)
