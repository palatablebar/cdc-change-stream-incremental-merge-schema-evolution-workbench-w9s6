from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, Boolean, DateTime, Index, Integer, String, text
from sqlalchemy.dialects.postgresql import JSONB
from src.models.types import GUID
from sqlalchemy.orm import Mapped, mapped_column

from src.config.settings import settings
from src.models.base import Base, TimestampMixin, utcnow


class CatchupSession(Base, TimestampMixin):
    """一条流的连续增量追赶会话。

    核心约束：
      - 每条流同时至多有一个未终结（status != completed）的会话，靠部分唯一索引保证；
      - safe_lsn 是「最后确认位置」：只有当前批次归并成功并落账后才向前推进；
      - current_run_id 指向在飞的批次 MergeRun，批次终结后由 worker 回调清空并决定是否续下一批；
      - gap_lsn 非空表示位点空洞：会话停在空洞之前，补齐后才能继续。
    """

    __tablename__ = "catchup_sessions"
    __table_args__ = (
        # 同一条流只允许一个未终结会话；completed 的历史会话保留备查
        Index(
            "uq_catchup_open_per_stream",
            "stream_id",
            unique=True,
            postgresql_where=text("status <> 'completed'"),
            sqlite_where=text("status <> 'completed'"),
        ),
        {"schema": settings.cdc_schema},
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    stream_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="idle", index=True)
    batch_size: Mapped[int] = mapped_column(Integer, nullable=False, default=20)

    # 水位线语义：safe_lsn = 已安全接纳的最大 LSN（与 change_streams.watermark_lsn 对齐）
    safe_lsn: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    # 阻塞空洞的位置：safe_lsn 之后第一个未到达 LSN；无空洞为 None
    gap_lsn: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # 批次记账
    total_batches: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    succeeded_batches: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_batches: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    current_batch_no: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    current_run_id: Mapped[uuid.UUID | None] = mapped_column(GUID, nullable=True)
    last_run_id: Mapped[uuid.UUID | None] = mapped_column(GUID, nullable=True)

    # 暂停意图：批次在飞时请求暂停会置真，当前批次确认后由回调落到 paused 并清掉
    pause_requested: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # 最近一次恢复结果（续跑/批次成败/补空洞都写这里，值班同学据此判断能否放心暂停）
    last_recovery_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(600), nullable=True)

    started_by: Mapped[uuid.UUID | None] = mapped_column(GUID, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    last_progress_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
