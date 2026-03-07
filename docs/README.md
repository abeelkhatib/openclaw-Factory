# OpenClaw Factory - Step 1 Foundation

This repository contains the FOUNDATION/BACKBONE implementation for an OpenClaw-style content factory.

## What is implemented

- Infra compose stack: Postgres, Redis, Temporal Server, Temporal UI, n8n
- Orchestrator API (`apps/orchestrator_api`)
  - `GET /health`
  - `POST /jobs/start` with `Idempotency-Key` support
  - `GET /jobs/{job_id}`
- Worker (`apps/worker`) with Temporal workflow + stub activities
  - Workflow: `Pipeline(job_id)`
  - Activities: `emit_status`, `stub_step`, `set_job_status`, `emit_exception`
- Plan validator stub (`apps/plan_validator`)
  - `GET /health`
  - `POST /validate`
- Shared package (`packages/shared`) for status enum, transition validator, and shared models
- Alembic migration for `jobs` and `idempotency_keys`
- n8n workflow export JSONs for `/webhook/status` and `/webhook/exceptions`
- Smoke test script to verify idempotent `POST /jobs/start`

## Prerequisites

- Python 3.11+
- `uv` installed (fallback is Poetry if needed)
- Docker + Docker Compose

## 1) Start infrastructure

From repository root:

```bash
docker compose -f infra/docker-compose.yml up -d
```

Service URLs:

- Temporal UI: <http://localhost:8080>
- n8n: <http://localhost:5678>
- Postgres: `localhost:5432`
- Redis: `localhost:6379`

## 2) Configure environment

```bash
cp .env.example .env
```

Defaults are already set for local docker-compose ports.

## 3) Install dependencies

```bash
uv sync
```

Fallback:

```bash
poetry install
```

## 4) Run DB migrations

```bash
uv run alembic -c apps/orchestrator_api/alembic.ini upgrade head
```

## 5) Import n8n workflows

In n8n UI:

1. Import `infra/n8n/status_workflow.json`
2. Import `infra/n8n/exceptions_workflow.json`
3. Activate both workflows

These create endpoints:

- `POST http://localhost:5678/webhook/status`
- `POST http://localhost:5678/webhook/exceptions`

## 6) Start application processes (separate terminals)

Orchestrator API:

```bash
uv run orchestrator-api
```

Worker:

```bash
uv run worker
```

Plan validator API:

```bash
uv run plan-validator-api
```

## 7) Smoke test

```bash
make smoke
```

Equivalent:

```bash
uv run smoke
```

Smoke does the following:

- Calls `POST /jobs/start` twice with the same `Idempotency-Key`
- Verifies the same `job_id` is returned
- Prints workflow ID/run ID if returned by the API

## Health endpoints

- Orchestrator API: `GET http://localhost:3000/health`
- Plan validator API: `GET http://localhost:3001/health`

## Gate B behavior

- Worker always posts pipeline status updates to `/webhook/status`
- Worker posts to `/webhook/exceptions` only when the workflow fails after retries and transitions to `FAILED`
