from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy, WorkflowIDReusePolicy

with workflow.unsafe.imports_passed_through():
    from openclaw_shared.models import (
        BacklogManagerInput,
        TrendIngestionInput,
        VideoProductionInput,
    )
    from openclaw_worker.activities import (
        EXTERNAL_API_RETRY,
        NO_RETRY,
        STANDARD_RETRY,
        embed_script,
        emit_gate_b_alert,
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
        update_backlog_stats,
        update_video_job,
        verify_output,
    )
    from openclaw_worker.config import settings

ACTIVITY_TIMEOUT = timedelta(minutes=5)
LONG_ACTIVITY_TIMEOUT = timedelta(minutes=10)
RENDER_TIMEOUT = timedelta(minutes=15)


@workflow.defn(name="VideoProduction")
class VideoProductionWorkflow:
    """Full single-video production pipeline."""

    @workflow.run
    async def run(self, inp: dict) -> str:
        data = VideoProductionInput(**inp)
        job_id = data.job_id
        topic_id = data.topic_id
        channel_id = data.channel_id

        workflow.logger.info("VideoProduction started job_id=%s", job_id)

        try:
            # ----------------------------------------------------------------
            # Stage 1: Script generation + QA
            # ----------------------------------------------------------------
            await workflow.execute_activity(
                update_video_job,
                job_id,
                {"stage": "scripting", "status": "SCRIPTING"},
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=STANDARD_RETRY,
            )

            script_id: str = await workflow.execute_activity(
                generate_script,
                topic_id,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=EXTERNAL_API_RETRY,
            )

            await workflow.execute_activity(
                update_video_job,
                job_id,
                {"script_id": script_id},
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=STANDARD_RETRY,
            )

            qa_result: dict = await workflow.execute_activity(
                qa_script,
                script_id,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=STANDARD_RETRY,
            )

            final_script_id = script_id
            if qa_result["overall_score"] < settings.qa_score_threshold:
                variant_ids: list[str] = await workflow.execute_activity(
                    generate_variants,
                    script_id,
                    settings.max_script_variants,
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    retry_policy=STANDARD_RETRY,
                )

                best_score = qa_result["overall_score"]
                for vid in variant_ids:
                    v_qa: dict = await workflow.execute_activity(
                        qa_script,
                        vid,
                        start_to_close_timeout=ACTIVITY_TIMEOUT,
                        retry_policy=STANDARD_RETRY,
                    )
                    if v_qa["overall_score"] > best_score:
                        best_score = v_qa["overall_score"]
                        final_script_id = vid

                if best_score < settings.qa_score_threshold:
                    await workflow.execute_activity(
                        emit_gate_b_alert,
                        job_id,
                        "quality_risk",
                        {"qa_score": best_score, "threshold": settings.qa_score_threshold},
                        start_to_close_timeout=ACTIVITY_TIMEOUT,
                        retry_policy=NO_RETRY,
                    )

                await workflow.execute_activity(
                    update_video_job,
                    job_id,
                    {"script_id": final_script_id},
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    retry_policy=STANDARD_RETRY,
                )

            # ----------------------------------------------------------------
            # Stage 2: Clip scanning + export (parallel)
            # ----------------------------------------------------------------
            await workflow.execute_activity(
                update_video_job,
                job_id,
                {"stage": "clipping", "status": "CLIPPING"},
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=STANDARD_RETRY,
            )

            clip_candidates: list[dict] = await workflow.execute_activity(
                scan_sources,
                topic_id,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=EXTERNAL_API_RETRY,
            )

            top_clips = sorted(
                clip_candidates,
                key=lambda c: c.get("relevance_score", 0),
                reverse=True,
            )[:3]

            clip_tasks = []
            for clip in top_clips:
                segs = clip.get("suggested_segments", [])
                seg = segs[0] if segs else {"start": 0.0, "end": 15.0}
                clip_tasks.append(
                    workflow.execute_activity(
                        export_clip,
                        clip["source_id"],
                        float(seg["start"]),
                        float(seg["end"]),
                        job_id,
                        start_to_close_timeout=LONG_ACTIVITY_TIMEOUT,
                        retry_policy=EXTERNAL_API_RETRY,
                    )
                )

            clip_urls: list[str] = list(await asyncio.gather(*clip_tasks))

            await workflow.execute_activity(
                update_video_job,
                job_id,
                {"clip_urls": clip_urls},
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=STANDARD_RETRY,
            )

            # ----------------------------------------------------------------
            # Stage 3: Voiceover
            # ----------------------------------------------------------------
            await workflow.execute_activity(
                update_video_job,
                job_id,
                {"stage": "vo", "status": "VO"},
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=STANDARD_RETRY,
            )

            vo_url: str = await workflow.execute_activity(
                generate_voiceover,
                final_script_id,
                job_id,
                start_to_close_timeout=LONG_ACTIVITY_TIMEOUT,
                retry_policy=EXTERNAL_API_RETRY,
            )

            await workflow.execute_activity(
                update_video_job,
                job_id,
                {"vo_url": vo_url},
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=STANDARD_RETRY,
            )

            # ----------------------------------------------------------------
            # Stage 4: Render
            # ----------------------------------------------------------------
            await workflow.execute_activity(
                update_video_job,
                job_id,
                {"stage": "rendering", "status": "RENDERING"},
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=STANDARD_RETRY,
            )

            _render_url: str = await workflow.execute_activity(
                render_video,
                job_id,
                start_to_close_timeout=RENDER_TIMEOUT,
                retry_policy=EXTERNAL_API_RETRY,
            )

            # ----------------------------------------------------------------
            # Stage 5: Verify
            # ----------------------------------------------------------------
            try:
                await workflow.execute_activity(
                    verify_output,
                    job_id,
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    retry_policy=STANDARD_RETRY,
                )
            except Exception as verify_exc:
                await workflow.execute_activity(
                    emit_gate_b_alert,
                    job_id,
                    "verification_failure",
                    {"error": str(verify_exc)},
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    retry_policy=NO_RETRY,
                )
                raise

            # ----------------------------------------------------------------
            # Stage 6: Publish
            # ----------------------------------------------------------------
            await workflow.execute_activity(
                update_video_job,
                job_id,
                {"stage": "publishing", "status": "PUBLISHING"},
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=STANDARD_RETRY,
            )

            publish_url: str = await workflow.execute_activity(
                schedule_post,
                job_id,
                channel_id,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=EXTERNAL_API_RETRY,
            )

            await workflow.execute_activity(
                update_video_job,
                job_id,
                {"stage": "done", "status": "DONE"},
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=STANDARD_RETRY,
            )

            # Stage 7: Embed (fire-and-forget — not on critical path)
            await workflow.start_child_workflow(
                "_FireAndForgetEmbed",
                {"script_id": final_script_id},
                id=f"embed-{final_script_id}",
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                parent_close_policy=workflow.ParentClosePolicy.ABANDON,
            )

            workflow.logger.info("VideoProduction done job_id=%s publish_url=%s", job_id, publish_url)
            return publish_url

        except Exception as exc:
            workflow.logger.error("VideoProduction failed job_id=%s error=%s", job_id, exc)
            try:
                await workflow.execute_activity(
                    emit_gate_b_alert,
                    job_id,
                    "hard_failure",
                    {"error": str(exc)},
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    retry_policy=NO_RETRY,
                )
                await workflow.execute_activity(
                    update_video_job,
                    job_id,
                    {"status": "DEAD_LETTER", "error": str(exc)},
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    retry_policy=NO_RETRY,
                )
            except Exception:
                pass
            return "failed"


@workflow.defn(name="_FireAndForgetEmbed")
class _FireAndForgetEmbedWorkflow:
    """Thin wrapper to run embed_script asynchronously."""

    @workflow.run
    async def run(self, inp: dict) -> None:
        await workflow.execute_activity(
            embed_script,
            inp["script_id"],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=EXTERNAL_API_RETRY,
        )


@workflow.defn(name="TrendIngestion")
class TrendIngestionWorkflow:
    """Triggered by n8n cron. Fetches topics, ingests, optionally fires production."""

    @workflow.run
    async def run(self, inp: dict) -> list[str]:
        data = TrendIngestionInput(**inp)
        workflow.logger.info("TrendIngestion started count=%d", data.count)

        topics: list[dict] = await workflow.execute_activity(
            fetch_trending_topics,
            data.count,
            start_to_close_timeout=timedelta(minutes=3),
            retry_policy=EXTERNAL_API_RETRY,
        )

        topic_ids: list[str] = await workflow.execute_activity(
            ingest_topics,
            topics,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=STANDARD_RETRY,
        )

        if data.trigger_production and topic_ids:
            active_channels: list[str] = await workflow.execute_activity(
                list_active_channels,
                start_to_close_timeout=timedelta(minutes=1),
                retry_policy=STANDARD_RETRY,
            )

            for topic_id in topic_ids:
                for channel_id in active_channels:
                    child_id = f"video-{topic_id}-{channel_id}"
                    try:
                        await workflow.start_child_workflow(
                            "VideoProduction",
                            {"job_id": child_id, "topic_id": topic_id, "channel_id": channel_id},
                            id=child_id,
                            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                            parent_close_policy=workflow.ParentClosePolicy.ABANDON,
                        )
                    except Exception as exc:
                        workflow.logger.warning(
                            "TrendIngestion: could not start VideoProduction child_id=%s err=%s",
                            child_id, exc,
                        )

            await workflow.execute_activity(
                update_backlog_stats,
                active_channels,
                len(topic_ids),
                start_to_close_timeout=timedelta(minutes=1),
                retry_policy=STANDARD_RETRY,
            )

        workflow.logger.info("TrendIngestion done: %d topics", len(topic_ids))
        return topic_ids


@workflow.defn(name="BacklogManager")
class BacklogManagerWorkflow:
    """Long-running workflow that manages backlog mode per channel."""

    @workflow.run
    async def run(self, inp: dict) -> None:
        data = BacklogManagerInput(**inp)
        channel_id = data.channel_id
        workflow.logger.info("BacklogManager started channel_id=%s", channel_id)

        while True:
            stat: dict = await workflow.execute_activity(
                get_backlog_stat,
                channel_id,
                start_to_close_timeout=timedelta(minutes=1),
                retry_policy=STANDARD_RETRY,
            )

            backlog_count: int = stat.get("backlog_count", 0)
            mode: str = stat.get("mode", "maintenance")

            if backlog_count < settings.backlog_low_threshold and mode == "maintenance":
                await workflow.execute_activity(
                    set_backlog_mode,
                    channel_id,
                    "backlog",
                    start_to_close_timeout=timedelta(minutes=1),
                    retry_policy=STANDARD_RETRY,
                )
                ingest_id = f"trend-ingest-backlog-{channel_id}-{int(workflow.now().timestamp())}"
                await workflow.start_child_workflow(
                    "TrendIngestion",
                    {"count": 50, "trigger_production": True},
                    id=ingest_id,
                    parent_close_policy=workflow.ParentClosePolicy.ABANDON,
                )
                workflow.logger.info("BacklogManager: channel %s → BACKLOG mode", channel_id)

            elif backlog_count >= settings.backlog_target and mode == "backlog":
                await workflow.execute_activity(
                    set_backlog_mode,
                    channel_id,
                    "maintenance",
                    start_to_close_timeout=timedelta(minutes=1),
                    retry_policy=STANDARD_RETRY,
                )
                workflow.logger.info("BacklogManager: channel %s → MAINTENANCE mode", channel_id)

            await workflow.sleep(timedelta(hours=1))
