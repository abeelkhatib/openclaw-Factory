"""video factory – enable pgvector and create video pipeline tables

Revision ID: 20260304_0002
Revises: 20260304_0001
Create Date: 2026-03-04 00:01:00
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260304_0002"
down_revision = "20260304_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Enable pgvector if the extension binary is available; skip silently otherwise.
    conn = op.get_bind()
    conn.execute(sa.text("SAVEPOINT before_vector"))
    try:
        conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS vector"))
    except Exception:
        conn.execute(sa.text("ROLLBACK TO SAVEPOINT before_vector"))
    else:
        conn.execute(sa.text("RELEASE SAVEPOINT before_vector"))

    op.create_table(
        "evergreen_clusters",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("embedding", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("name", name="uq_evergreen_clusters_name"),
    )

    op.create_table(
        "topics",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("cluster_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("title", sa.String(500), nullable=False),
        sa.Column("body", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["cluster_id"], ["evergreen_clusters.id"]),
    )

    op.create_table(
        "scripts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("topic_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("content", sa.Text, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"]),
        sa.ForeignKeyConstraint(["topic_id"], ["topics.id"]),
    )

    op.create_table(
        "channel_configs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("channel_name", sa.String(255), nullable=False),
        sa.Column("config_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("channel_name", name="uq_channel_configs_channel_name"),
    )

    op.create_table(
        "backlog_stats",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("channel_name", sa.String(255), nullable=False),
        sa.Column("pending_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("stats_json", sa.JSON(), nullable=False),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_table(
        "video_jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("channel_name", sa.String(255), nullable=False),
        sa.Column("script_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("config_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"]),
        sa.ForeignKeyConstraint(["script_id"], ["scripts.id"]),
    )


def downgrade() -> None:
    op.drop_table("video_jobs")
    op.drop_table("backlog_stats")
    op.drop_table("channel_configs")
    op.drop_table("scripts")
    op.drop_table("topics")
    op.drop_table("evergreen_clusters")

    conn = op.get_bind()
    conn.execute(sa.text("SAVEPOINT before_drop_vector"))
    try:
        conn.execute(sa.text("DROP EXTENSION IF EXISTS vector"))
    except Exception:
        conn.execute(sa.text("ROLLBACK TO SAVEPOINT before_drop_vector"))
    else:
        conn.execute(sa.text("RELEASE SAVEPOINT before_drop_vector"))
