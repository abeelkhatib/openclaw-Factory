"""Cloudflare R2 / S3-compatible storage."""
from __future__ import annotations

import asyncio
import logging
from functools import lru_cache

import boto3
from botocore.config import Config

from openclaw_shared.config_shared import shared_settings

logger = logging.getLogger(__name__)

# R2 endpoint pattern
_R2_ENDPOINT = "https://{account_id}.r2.cloudflarestorage.com"


def _get_r2_settings() -> tuple[str, str, str, str, str]:
    """Read R2 settings from environment via a lazy import of the full settings."""
    try:
        # Try worker settings first, then orchestrator
        from openclaw_worker.config import settings  # type: ignore[import]
    except ImportError:
        try:
            from openclaw_orchestrator_api.config import settings  # type: ignore[import]
        except ImportError:
            # Fall back to shared settings — R2 fields not available
            return "", "", "", "openclaw-assets", ""
    return (
        settings.r2_account_id,
        settings.r2_access_key_id,
        settings.r2_secret_access_key,
        settings.r2_bucket,
        settings.r2_public_base,
    )


@lru_cache(maxsize=1)
def _s3_client():  # type: ignore[return]
    account_id, access_key, secret_key, bucket, _ = _get_r2_settings()
    if not account_id:
        logger.warning("[storage] R2_ACCOUNT_ID not set — storage calls will fail")
        return None
    endpoint = _R2_ENDPOINT.format(account_id=account_id)
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def _bucket() -> str:
    _, _, _, bucket, _ = _get_r2_settings()
    return bucket


def _public_url(key: str) -> str:
    _, _, _, bucket, public_base = _get_r2_settings()
    if public_base:
        return f"{public_base.rstrip('/')}/{key}"
    return f"r2://{bucket}/{key}"


async def upload_file(local_path: str, key: str) -> str:
    """Upload file, return public URL."""
    def _upload() -> str:
        client = _s3_client()
        if client is None:
            raise RuntimeError("R2 client not configured — set R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY")
        client.upload_file(local_path, _bucket(), key)
        return _public_url(key)

    return await asyncio.to_thread(_upload)


async def download_file(key: str, local_path: str) -> None:
    """Download a file from R2 to local_path."""
    def _download() -> None:
        client = _s3_client()
        if client is None:
            raise RuntimeError("R2 client not configured")
        client.download_file(_bucket(), key, local_path)

    await asyncio.to_thread(_download)


async def upload_bytes(data: bytes, key: str, content_type: str = "application/octet-stream") -> str:
    """Upload bytes directly, return URL."""
    import io

    def _upload() -> str:
        client = _s3_client()
        if client is None:
            raise RuntimeError("R2 client not configured")
        client.upload_fileobj(
            io.BytesIO(data),
            _bucket(),
            key,
            ExtraArgs={"ContentType": content_type},
        )
        return _public_url(key)

    return await asyncio.to_thread(_upload)


def presigned_url(key: str, expiry_s: int = 3600) -> str:
    client = _s3_client()
    if client is None:
        raise RuntimeError("R2 client not configured")
    return client.generate_presigned_url(
        "get_object",
        Params={"Bucket": _bucket(), "Key": key},
        ExpiresIn=expiry_s,
    )
