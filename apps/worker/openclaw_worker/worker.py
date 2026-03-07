from __future__ import annotations

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from openclaw_worker.activities import emit_exception, emit_status, set_job_status, stub_step
from openclaw_worker.config import settings
from openclaw_worker.workflows import PipelineWorkflow


async def _run_worker() -> None:
    client = await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace)
    worker = Worker(
        client,
        task_queue=settings.temporal_task_queue,
        workflows=[PipelineWorkflow],
        activities=[emit_status, emit_exception, set_job_status, stub_step],
    )
    await worker.run()


def run() -> None:
    asyncio.run(_run_worker())


if __name__ == "__main__":
    run()
