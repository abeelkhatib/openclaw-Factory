"""Redis cache helpers."""
from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

import redis.asyncio as aioredis

from openclaw_shared.config_shared import shared_settings

logger = logging.getLogger(__name__)

_redis_client: aioredis.Redis | None = None


async def get_redis() -> aioredis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = aioredis.from_url(shared_settings.redis_url, decode_responses=True)
    return _redis_client


async def get_cached(key: str) -> Any | None:
    redis = await get_redis()
    try:
        value = await redis.get(key)
        if value is None:
            return None
        return json.loads(value)
    except Exception:
        logger.exception("[cache] get_cached failed for key=%s", key)
        return None


async def set_cached(key: str, value: Any, ttl_s: int) -> None:
    redis = await get_redis()
    try:
        await redis.set(key, json.dumps(value), ex=ttl_s)
    except Exception:
        logger.exception("[cache] set_cached failed for key=%s", key)


async def delete_cached(key: str) -> None:
    redis = await get_redis()
    try:
        await redis.delete(key)
    except Exception:
        logger.exception("[cache] delete_cached failed for key=%s", key)


# ---------------------------------------------------------------------------
# VO-specific helpers
# ---------------------------------------------------------------------------

def vo_cache_key(script_text: str) -> str:
    return "vo:" + hashlib.sha256(script_text.encode()).hexdigest()


async def get_cached_vo(script_text: str) -> str | None:
    """Return R2 URL if cached, else None."""
    key = vo_cache_key(script_text)
    result = await get_cached(key)
    if isinstance(result, str):
        return result
    return None


async def set_cached_vo(script_text: str, r2_url: str, ttl_days: int = 30) -> None:
    key = vo_cache_key(script_text)
    await set_cached(key, r2_url, ttl_s=ttl_days * 86400)


# ---------------------------------------------------------------------------
# Rate limiting (sliding window using Redis INCR + EXPIRE)
# ---------------------------------------------------------------------------

async def check_rate_limit(key: str, limit: int, window_s: int) -> bool:
    """Return True if request is allowed, False if rate limited."""
    redis = await get_redis()
    try:
        pipe = redis.pipeline()
        await pipe.incr(key)
        await pipe.expire(key, window_s)
        results = await pipe.execute()
        current_count = results[0]
        return current_count <= limit
    except Exception:
        logger.exception("[cache] check_rate_limit failed for key=%s", key)
        return True  # allow on error to avoid blocking the pipeline
