"""video factory tables

Revision ID: 20260304_0002
Revises: 20260304_0001
Create Date: 2026-03-04 00:01:00
"""
from __future__ import annotations

import warnings

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260304_0002"
down_revision = "20260304_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Enable pgvector extension.
    # Requires pgvector to be installed on the Postgres server.
    # Production: use pgvector/pgvector:pg16 Docker image.
    # Standard postgres:16-alpine does NOT include it; embedding falls back to TEXT.
    conn = op.get_bind()
    pgvector_ok = False
    conn.execute(sa.text("SAVEPOINT before_vector"))
    try:
        conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS vector"))
        conn.execute(sa.text("RELEASE SAVEPOINT before_vector"))
        pgvector_ok = True
    except Exception:
        conn.execute(sa.text("ROLLBACK TO SAVEPOINT before_vector"))
        warnings.warn(
            "pgvector extension not available. "
            "Switch to pgvector/pgvector:pg16 Docker image. "
            "Script embedding column will be TEXT until then.",
            stacklevel=2,
        )

    op.create_table(
        "evergreen_clusters",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("cluster_name", sa.String(length=255), nullable=False),
        sa.Column("moment_tags", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("reusability_score", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("seasonality", sa.JSON(), nullable=True),
    )

    op.create_table(
        "topics",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("headline", sa.Text(), nullable=False),
        sa.Column("entities", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("why_now", sa.Text(), nullable=False),
        sa.Column("priority_score", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("expiry_window_hours", sa.Integer(), nullable=False, server_default="24"),
        sa.Column("source_links", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column(
            "cluster_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("evergreen_clusters.id"),
            nullable=True,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_table(
        "scripts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("topic_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("topics.id"), nullable=False),
        sa.Column("hook", sa.Text(), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("cta", sa.Text(), nullable=False),
        sa.Column("full_text", sa.Text(), nullable=False),
        sa.Column("duration_estimate_s", sa.Integer(), nullable=False, server_default="50"),
        sa.Column("qa_score", sa.Float(), nullable=True),
        sa.Column(
            "variant_of",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("scripts.id"),
            nullable=True,
        ),
        # embedding: vector(1536) when pgvector is available, TEXT otherwise
        sa.Column("embedding", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    # Promote embedding to vector(1536) if pgvector is available
    if pgvector_ok:
        conn.execute(sa.text("ALTER TABLE scripts ALTER COLUMN embedding TYPE vector(1536) USING NULL"))

    op.create_table(
        "channel_configs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("platform", sa.String(length=32), nullable=False),
        sa.Column("credentials_ref", sa.String(length=255), nullable=False),
        sa.Column("daily_target", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("posting_windows", sa.JSON(), nullable=False, server_default="[]"),
    )

    op.create_table(
        "backlog_stats",
        sa.Column(
            "channel_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("channel_configs.id"),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("backlog_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("mode", sa.String(length=32), nullable=False, server_default="maintenance"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_table(
        "video_jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("topic_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("topics.id"), nullable=False),
        sa.Column("script_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("scripts.id"), nullable=True),
        sa.Column(
            "channel_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("channel_configs.id"),
            nullable=False,
        ),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="CREATED"),
        sa.Column("stage", sa.String(length=64), nullable=True),
        sa.Column("clip_urls", sa.JSON(), nullable=True),
        sa.Column("vo_url", sa.Text(), nullable=True),
        sa.Column("render_url", sa.Text(), nullable=True),
        sa.Column("publish_url", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("gate_b_alerts", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )


def downgrade() -> None:
    op.drop_table("video_jobs")
    op.drop_table("backlog_stats")
    op.drop_table("channel_configs")
    op.drop_table("scripts")
    op.drop_table("topics")
    op.drop_table("evergreen_clusters")
    conn = op.get_bind()
    conn.execute(sa.text("DROP EXTENSION IF EXISTS vector"))
