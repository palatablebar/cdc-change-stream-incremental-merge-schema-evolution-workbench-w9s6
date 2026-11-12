"""新增 catchup_sessions：连续增量追赶会话表。

Revision ID: 0002_catchup
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from src.config.settings import settings

revision = "0002_catchup"
down_revision = "0001_initial"
branch_labels = None
depends_on = None

CDC = settings.cdc_schema


def upgrade() -> None:
    op.create_table(
        "catchup_sessions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("stream_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="idle"),
        sa.Column("batch_size", sa.Integer(), nullable=False, server_default="20"),
        sa.Column("safe_lsn", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("gap_lsn", sa.BigInteger(), nullable=True),
        sa.Column("total_batches", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("succeeded_batches", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failed_batches", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("current_batch_no", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("current_run_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("last_run_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("pause_requested", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("last_recovery_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("last_error", sa.String(length=600), nullable=True),
        sa.Column("started_by", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("last_progress_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        schema=CDC,
    )
    op.create_index(
        "ix_catchup_sessions_stream_id",
        "catchup_sessions",
        ["stream_id"],
        schema=CDC,
    )
    op.create_index(
        "ix_catchup_sessions_status",
        "catchup_sessions",
        ["status"],
        schema=CDC,
    )
    # 同一条流只允许一个未终结会话
    op.create_index(
        "uq_catchup_open_per_stream",
        "catchup_sessions",
        ["stream_id"],
        unique=True,
        schema=CDC,
        postgresql_where=sa.text("status <> 'completed'"),
    )
    # MergeRun 增加归属会话列（普通 FK，不建强约束，避免批次失败/重试被外键绑死）
    op.add_column(
        "merge_runs",
        sa.Column("catchup_session_id", postgresql.UUID(as_uuid=True), nullable=True),
        schema=CDC,
    )
    op.create_index(
        "ix_merge_runs_catchup_session_id",
        "merge_runs",
        ["catchup_session_id"],
        schema=CDC,
    )


def downgrade() -> None:
    op.drop_index("ix_merge_runs_catchup_session_id", table_name="merge_runs", schema=CDC)
    op.drop_column("merge_runs", "catchup_session_id", schema=CDC)
    op.drop_index("uq_catchup_open_per_stream", table_name="catchup_sessions", schema=CDC)
    op.drop_index("ix_catchup_sessions_status", table_name="catchup_sessions", schema=CDC)
    op.drop_index("ix_catchup_sessions_stream_id", table_name="catchup_sessions", schema=CDC)
    op.drop_table("catchup_sessions", schema=CDC)
