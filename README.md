# openclaw-factory

A fully automated short-form video pipeline that ingests source footage, transcribes dialogue with AI, assembles split-screen vertical videos, and publishes directly to TikTok and YouTube Shorts — with zero human intervention.

---

## What It Does

1. **Ingests** a source video (e.g. a TV episode) from Cloudflare R2
2. **Transcribes** the dialogue using OpenAI Whisper with word-level timestamps
3. **Assembles** a 9:16 vertical split-screen video using Remotion:
   - Top 60%: source footage with baked-in audio
   - Bottom 40%: looping gameplay footage
   - Center: captions synced to dialogue timestamps
4. **Publishes** the finished video to TikTok and YouTube Shorts via upload-post API

---

## Architecture

```
n8n (trigger)
    │
    ▼
FastAPI Orchestrator (port 3000)
    │
    ▼
Temporal Workflow Engine
    │
    ▼
OpenClaw (activity executor)
    ├── get_job_config      → reads job config from Supabase
    ├── transcribe_clip     → downloads episode, runs Whisper, saves transcript to R2
    ├── assemble_video      → sends render request to Remotion renderer (port 3002)
    └── publish_video       → uploads final video to TikTok / YouTube Shorts
            │
            ▼
    Remotion Renderer (Express + React + FFmpeg)
            │
            ▼
    Cloudflare R2 (asset storage)
            │
            ▼
    upload-post API (social publishing)
```

---

## Stack

| Layer | Technology |
|---|---|
| Orchestration | Temporal, n8n |
| Activity Executor | OpenClaw |
| Backend | Python, FastAPI, asyncio |
| Data Validation | Pydantic, SQLAlchemy, Alembic |
| AI / Transcription | OpenAI Whisper (base model, CPU) |
| Video Rendering | Remotion, React, TypeScript |
| Video Processing | FFmpeg |
| Cloud Storage | Cloudflare R2 (S3-compatible, boto3) |
| Database | PostgreSQL + pgvector (Supabase), Redis |
| Publishing | upload-post API (TikTok + YouTube Shorts) |
| Runtime | Node.js, uv (Python) |

---

## Project Structure

```
openclaw-factory/
├── apps/
│   ├── worker/                  # Python Temporal worker (OpenClaw)
│   │   └── openclaw_worker/
│   │       ├── activities.py    # Pipeline activity implementations
│   │       ├── workflows.py     # Temporal workflow definition
│   │       ├── worker.py        # Worker entry point
│   │       └── config.py        # Pydantic settings
│   └── remotion-renderer/       # Node.js render server
│       └── src/
│           ├── server.ts        # Express API server
│           ├── compositions/
│           │   └── VideoFactory.tsx  # Remotion composition
│           └── index.tsx        # Composition registry
├── packages/
│   └── shared/                  # Shared DB models
└── .env                         # Environment config
```

---

## Pipeline Workflow

```
get_job_config → transcribe_clip → assemble_video → publish_video
```

Each activity is independently retried by Temporal (3 attempts, 30s timeout per activity, 10min for long-running tasks like transcription and rendering).

---

## Environment Variables

```env
# Temporal
TEMPORAL_ADDRESS=localhost:7233
TEMPORAL_NAMESPACE=default
TEMPORAL_TASK_QUEUE=pipeline-task-queue

# Database
DATABASE_URL=postgresql://...

# Cloudflare R2
R2_ACCOUNT_ID=
R2_ACCESS_KEY_ID=
R2_SECRET_ACCESS_KEY=
R2_BUCKET=openclaw-factory
R2_ENDPOINT_URL=https://<account>.r2.cloudflarestorage.com

# Episode source
EPISODE_URL=https://pub-xxx.r2.dev/episodes/episode.mp4
WHISPER_MODEL=base

# Gameplay loop
GAMEPLAY_CLIP_URL=https://pub-xxx.r2.dev/gameplay_loop.mp4

# Publishing
UPLOAD_POST_API_KEY=
UPLOAD_POST_TIKTOK_USERNAME=
UPLOAD_POST_YOUTUBE_USERNAME=

# Webhooks
N8N_STATUS_WEBHOOK_URL=
N8N_EXCEPTIONS_WEBHOOK_URL=
```

---

## Running Locally

You'll need Temporal running locally (`temporal server start-dev`) and a Supabase project set up.

```bash
# Terminal 1 — API server
uv run orchestrator-api

# Terminal 2 — Temporal worker
uv run worker

# Terminal 3 — Remotion render server
cd apps/remotion-renderer
npm run dev
```

Trigger a job:

```powershell
Invoke-RestMethod -Method POST -Uri "http://localhost:3000/jobs/start" `
  -ContentType "application/json" `
  -Headers @{"Idempotency-Key" = "job-001"} `
  -Body '{"topic_clusters": ["house md"], "videos_per_channel": 1, "channels": ["shorts"]}'
```

---

## Status

> ⚠️ Active development. Core pipeline (transcription → render → publish) is functional. Episode source is currently configured via `EPISODE_URL` env var pointing to a single MP4 in R2.

---

## License

MIT
