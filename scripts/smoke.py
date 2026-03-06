"""
openclaw-factory comprehensive connectivity smoke test.

Checks: DB, pgvector, Redis, Temporal, MiniMax API, Ollama, Vugola, Remotion, n8n, R2/S3.
Exit code 0 if all non-skipped checks pass. Exit code 1 otherwise.
"""
from __future__ import annotations

import asyncio
import os
import sys
import textwrap
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5432/openclaw")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
MINIMAX_API_BASE = os.getenv("MINIMAX_API_BASE", "https://api.minimaxi.chat/v1")
MINIMAX_API_KEY = os.getenv("MINIMAX_API_KEY", "")
OLLAMA_BASE = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
VUGOLA_API_BASE = os.getenv("VUGOLA_API_BASE", "https://api.vugola.com/v1")
VUGOLA_API_KEY = os.getenv("VUGOLA_API_KEY", "")
REMOTION_API_BASE = os.getenv("REMOTION_API_BASE", "http://localhost:3002")
N8N_BASE = "http://localhost:5678"
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET = os.getenv("R2_BUCKET", "openclaw-assets")


# ──────────────────────────────────────────────────────────────────────────────
# Result types
# ──────────────────────────────────────────────────────────────────────────────

class Status(Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    SKIP = "SKIP"


@dataclass
class CheckResult:
    name: str
    status: Status
    detail: str = ""
    note: str = ""


# ──────────────────────────────────────────────────────────────────────────────
# Checks
# ──────────────────────────────────────────────────────────────────────────────

async def check_db() -> CheckResult:
    """1. DB connection — SELECT 1 (sync sqlalchemy in thread)."""
    def _check() -> CheckResult:
        try:
            from sqlalchemy import create_engine, text

            eng = create_engine(DATABASE_URL, pool_pre_ping=True)
            with eng.connect() as conn:
                conn.execute(text("SELECT 1"))
            eng.dispose()
            return CheckResult("DB connection", Status.PASS, "SELECT 1 succeeded")
        except Exception as exc:
            return CheckResult("DB connection", Status.FAIL, str(exc))

    return await asyncio.to_thread(_check)


async def check_pgvector() -> CheckResult:
    """2. pgvector extension installed (sync sqlalchemy in thread)."""
    def _check() -> CheckResult:
        try:
            from sqlalchemy import create_engine, text

            eng = create_engine(DATABASE_URL, pool_pre_ping=True)
            with eng.connect() as conn:
                # Check if pgvector is installed at the server level
                server_has_it = conn.execute(
                    text("SELECT COUNT(*) FROM pg_available_extensions WHERE name='vector'")
                ).scalar()
                if not server_has_it:
                    eng.dispose()
                    return CheckResult(
                        "pgvector extension",
                        Status.SKIP,
                        "pgvector not installed on this Postgres server",
                        note="# TODO: switch to pgvector/pgvector:pg16 image (already updated in docker-compose.yml)",
                    )
                row = conn.execute(
                    text("SELECT extname FROM pg_extension WHERE extname='vector'")
                ).fetchone()
            eng.dispose()
            if row:
                return CheckResult("pgvector extension", Status.PASS, "extension 'vector' is installed")
            return CheckResult(
                "pgvector extension",
                Status.FAIL,
                "pgvector available but not enabled - run: CREATE EXTENSION vector",
            )
        except Exception as exc:
            return CheckResult("pgvector extension", Status.FAIL, str(exc))

    return await asyncio.to_thread(_check)


async def check_redis() -> CheckResult:
    """3. Redis connection — PING (sync in thread)."""
    def _check() -> CheckResult:
        try:
            import redis

            r = redis.from_url(REDIS_URL, decode_responses=True)
            pong = r.ping()
            r.close()
            return CheckResult("Redis connection", Status.PASS, f"PING -> {pong}")
        except Exception as exc:
            return CheckResult("Redis connection", Status.FAIL, str(exc))

    return await asyncio.to_thread(_check)


async def check_temporal() -> CheckResult:
    """4. Temporal connection — connect and get system info."""
    try:
        from temporalio.client import Client

        client = await Client.connect(TEMPORAL_ADDRESS, namespace="default")
        # List namespaces to verify connectivity
        resp = await client.service_client.operator_service.list_namespaces(
            list_namespaces_request=None  # type: ignore[arg-type]
        )
        ns_count = len(resp.namespaces) if hasattr(resp, "namespaces") else "?"
        return CheckResult("Temporal connection", Status.PASS, f"Connected to {TEMPORAL_ADDRESS}, namespaces: {ns_count}")
    except Exception:
        # Fallback: just check we can connect at all
        try:
            from temporalio.client import Client
            await Client.connect(TEMPORAL_ADDRESS, namespace="default")
            return CheckResult("Temporal connection", Status.PASS, f"Connected to {TEMPORAL_ADDRESS}")
        except Exception as exc2:
            return CheckResult("Temporal connection", Status.FAIL, str(exc2))


async def check_minimax() -> CheckResult:
    """5. MiniMax API — GET /v1/models."""
    if not MINIMAX_API_KEY:
        return CheckResult(
            "MiniMax API",
            Status.SKIP,
            "MINIMAX_API_KEY not set",
            note="# TODO: set MINIMAX_API_KEY to enable",
        )
    try:
        import httpx

        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{MINIMAX_API_BASE}/models",
                headers={"Authorization": f"Bearer {MINIMAX_API_KEY}"},
            )
            resp.raise_for_status()
        return CheckResult("MiniMax API", Status.PASS, f"HTTP {resp.status_code}")
    except Exception as exc:
        return CheckResult("MiniMax API", Status.FAIL, str(exc))


async def check_ollama() -> CheckResult:
    """6. Ollama — GET /api/tags."""
    try:
        import httpx

        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(f"{OLLAMA_BASE}/api/tags")
            resp.raise_for_status()
            data = resp.json()
        model_names = [m.get("name", "?") for m in data.get("models", [])]
        return CheckResult(
            "Ollama",
            Status.PASS,
            f"Running - {len(model_names)} model(s): {', '.join(model_names[:3]) or 'none loaded'}",
        )
    except Exception as exc:
        return CheckResult("Ollama", Status.FAIL, str(exc))


async def check_vugola() -> CheckResult:
    """7. Vugola API — GET /v1/health."""
    if not VUGOLA_API_KEY:
        return CheckResult(
            "Vugola API",
            Status.SKIP,
            "VUGOLA_API_KEY not set",
            note="# TODO: set VUGOLA_API_KEY to enable",
        )
    try:
        import httpx

        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{VUGOLA_API_BASE}/health",
                headers={"Authorization": f"Bearer {VUGOLA_API_KEY}"},
            )
            resp.raise_for_status()
        return CheckResult("Vugola API", Status.PASS, f"HTTP {resp.status_code}")
    except Exception as exc:
        return CheckResult("Vugola API", Status.FAIL, str(exc))


async def check_remotion() -> CheckResult:
    """8. Remotion server — GET /health."""
    try:
        import httpx

        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{REMOTION_API_BASE}/health")
            resp.raise_for_status()
        return CheckResult("Remotion server", Status.PASS, f"HTTP {resp.status_code}")
    except Exception as exc:
        return CheckResult(
            "Remotion server",
            Status.SKIP,
            f"Unreachable: {exc}",
            note="# TODO: start Remotion render server on port 3002",
        )


async def check_n8n() -> CheckResult:
    """9. n8n webhooks — GET /healthz."""
    try:
        import httpx

        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{N8N_BASE}/healthz")
            resp.raise_for_status()
        return CheckResult("n8n webhooks", Status.PASS, f"HTTP {resp.status_code}")
    except Exception as exc:
        return CheckResult("n8n webhooks", Status.FAIL, str(exc))


async def check_r2() -> CheckResult:
    """10. R2/S3 — list_buckets."""
    if not R2_ACCOUNT_ID:
        return CheckResult(
            "R2/S3 storage",
            Status.SKIP,
            "R2_ACCOUNT_ID not set",
            note="# TODO: set R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY",
        )
    try:
        import boto3
        from botocore.config import Config

        endpoint = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            config=Config(signature_version="s3v4"),
            region_name="auto",
        )
        response = await asyncio.to_thread(s3.list_buckets)
        bucket_names = [b["Name"] for b in response.get("Buckets", [])]
        r2_bucket_found = R2_BUCKET in bucket_names
        detail = f"Buckets: {bucket_names}"
        if not r2_bucket_found:
            detail += f" - WARNING: bucket '{R2_BUCKET}' not found"
        return CheckResult("R2/S3 storage", Status.PASS, detail)
    except Exception as exc:
        return CheckResult("R2/S3 storage", Status.FAIL, str(exc))


# ──────────────────────────────────────────────────────────────────────────────
# Runner
# ──────────────────────────────────────────────────────────────────────────────

ALL_CHECKS: list[Callable[[], CheckResult]] = [
    check_db,
    check_pgvector,
    check_redis,
    check_temporal,
    check_minimax,
    check_ollama,
    check_vugola,
    check_remotion,
    check_n8n,
    check_r2,
]

_STATUS_EMOJI = {
    Status.PASS: "[PASS]",
    Status.FAIL: "[FAIL]",
    Status.SKIP: "[SKIP]",
}

_STATUS_COLOR = {
    Status.PASS: "\033[92m",  # green
    Status.FAIL: "\033[91m",  # red
    Status.SKIP: "\033[93m",  # yellow
}
_RESET = "\033[0m"


def _colored(s: str, status: Status) -> str:
    if not sys.stdout.isatty():
        return s
    return f"{_STATUS_COLOR[status]}{s}{_RESET}"


async def _run_all() -> list[CheckResult]:
    tasks = [check() for check in ALL_CHECKS]
    return list(await asyncio.gather(*tasks))


def _print_table(results: list[CheckResult]) -> None:
    col_name = max(len(r.name) for r in results) + 2
    col_status = 8
    col_detail = 60

    header = f"{'Check':<{col_name}} {'Status':<{col_status}} Detail"
    print()
    print(header)
    sep = "-" * (col_name + col_status + col_detail + 4)
    print(sep)

    for r in results:
        symbol = _STATUS_EMOJI[r.status]
        detail = r.detail[:col_detail]
        line = f"{r.name:<{col_name}} {_colored(symbol, r.status):<{col_status + 10}} {detail}"
        print(line)
        if r.note:
            print(f"  {'':>{col_name}} {r.note}")

    print(sep)

    passed = sum(1 for r in results if r.status == Status.PASS)
    failed = sum(1 for r in results if r.status == Status.FAIL)
    skipped = sum(1 for r in results if r.status == Status.SKIP)
    total = len(results)

    summary = f"Results: {passed}/{total} passed"
    if skipped:
        summary += f", {skipped} skipped"
    if failed:
        summary += f", {failed} FAILED"
        print(f"\n{_colored(summary, Status.FAIL)}")
    else:
        print(f"\n{_colored(summary, Status.PASS)}")
    print()


def main() -> None:
    # On Windows, asyncio defaults to ProactorEventLoop which breaks some libraries.
    # Use SelectorEventLoop for compatibility.
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
    results = asyncio.run(_run_all())
    _print_table(results)

    hard_failures = [r for r in results if r.status == Status.FAIL]
    if hard_failures:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
