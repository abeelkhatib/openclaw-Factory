from __future__ import annotations

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from openclaw_worker.activities import (
    embed_script,
    emit_exception,
    emit_gate_b_alert,
    emit_status,
    export_clip,
    fetch_trending_topics,
    generate_script,
    generate_variants,
    generate_voiceover,
    get_backlog_stat,
    ingest_topics,
    list_active_channels,
    qa_script,
    render_video,
    scan_sources,
    schedule_post,
    set_backlog_mode,
    set_job_status,
    stub_step,
    update_backlog_stats,
    update_video_job,
    verify_output,
)
from openclaw_worker.config import settings
from openclaw_worker.workflows import (
    BacklogManagerWorkflow,
    TrendIngestionWorkflow,
    VideoProductionWorkflow,
    _FireAndForgetEmbedWorkflow,
)


async def _run_worker() -> None:
    client = await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace)
    worker = Worker(
        client,
        task_queue=settings.temporal_task_queue,
        workflows=[
            VideoProductionWorkflow,
            TrendIngestionWorkflow,
            BacklogManagerWorkflow,
            _FireAndForgetEmbedWorkflow,
        ],
        activities=[
            # Legacy
            emit_status,
            emit_exception,
            set_job_status,
            stub_step,
            # New
            update_video_job,
            list_active_channels,
            update_backlog_stats,
            get_backlog_stat,
            set_backlog_mode,
            fetch_trending_topics,
            ingest_topics,
            generate_script,
            qa_script,
            generate_variants,
            embed_script,
            scan_sources,
            export_clip,
            generate_voiceover,
            render_video,
            verify_output,
            schedule_post,
            emit_gate_b_alert,
        ],
    )
    await worker.run()


def run() -> None:
    asyncio.run(_run_worker())


if __name__ == "__main__":
    run()
