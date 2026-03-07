from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from openclaw_shared.status import JobStatus
    from openclaw_worker.activities import emit_exception, emit_status, set_job_status, stub_step


ACTIVITY_TIMEOUT = timedelta(seconds=30)
ACTIVITY_RETRY = RetryPolicy(initial_interval=timedelta(seconds=1), maximum_attempts=3)


@workflow.defn(name="Pipeline")
class PipelineWorkflow:
    @workflow.run
    async def run(self, job_id: str) -> str:
        await workflow.execute_activity(
            set_job_status,
            job_id,
            JobStatus.RUNNING.value,
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=ACTIVITY_RETRY,
        )
        await workflow.execute_activity(
            emit_status,
            job_id,
            "pipeline",
            "started",
            "Workflow started",
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=ACTIVITY_RETRY,
        )

        try:
            for step in ["plan", "clip", "schedule"]:
                await workflow.execute_activity(
                    stub_step,
                    job_id,
                    step,
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    retry_policy=ACTIVITY_RETRY,
                )

            await workflow.execute_activity(
                set_job_status,
                job_id,
                JobStatus.DONE.value,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=ACTIVITY_RETRY,
            )
            await workflow.execute_activity(
                emit_status,
                job_id,
                "pipeline",
                "done",
                "Workflow finished successfully",
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=ACTIVITY_RETRY,
            )
            return "done"
        except Exception as exc:
            await workflow.execute_activity(
                set_job_status,
                job_id,
                JobStatus.FAILED.value,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=ACTIVITY_RETRY,
            )
            await workflow.execute_activity(
                emit_status,
                job_id,
                "pipeline",
                "failed",
                str(exc),
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=ACTIVITY_RETRY,
            )
            await workflow.execute_activity(
                emit_exception,
                job_id,
                str(exc),
                "Workflow failed after configured retries",
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=ACTIVITY_RETRY,
            )
            raise
