"""video factory – enable pgvector

Revision ID: 20260304_0002
Revises: 20260304_0001
Create Date: 2026-03-04 00:01:00
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260304_0002"
down_revision = "20260304_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(sa.text("SAVEPOINT before_vector"))
    try:
        conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS vector"))
    except Exception:
        conn.execute(sa.text("ROLLBACK TO SAVEPOINT before_vector"))
    else:
        conn.execute(sa.text("RELEASE SAVEPOINT before_vector"))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(sa.text("SAVEPOINT before_drop_vector"))
    try:
        conn.execute(sa.text("DROP EXTENSION IF EXISTS vector"))
    except Exception:
        conn.execute(sa.text("ROLLBACK TO SAVEPOINT before_drop_vector"))
    else:
        conn.execute(sa.text("RELEASE SAVEPOINT before_drop_vector"))
