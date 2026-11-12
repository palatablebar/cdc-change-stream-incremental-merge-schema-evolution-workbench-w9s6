from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, String
from sqlalchemy.dialects.postgresql import JSONB
from src.models.types import GUID
from sqlalchemy.orm import Mapped, mapped_column

from src.config.settings import settings
from src.models.base import Base, utcnow


class RejectedEvent(Base):
    """被拒绝的事件：原因枚举 + 原始载荷快照（重放要用）。"""

    __tablename__ = "rejected_events"
    __table_args__ = {"schema": settings.cdc_schema}

    id: Mapped[uuid.UUID] = mapped_column(GUID, primary_key=True, default=uuid.uuid4)
    event_id: Mapped[uuid.UUID | None] = mapped_column(GUID, nullable=True, index=True)
    run_id: Mapped[uuid.UUID | None] = mapped_column(GUID, nullable=True, index=True)
    stream_id: Mapped[uuid.UUID] = mapped_column(GUID, nullable=False, index=True)
    lsn: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    reason: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    detail: Mapped[str] = mapped_column(String(600), nullable=False, default="")
    payload_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending", index=True)
    replayed_event_id: Mapped[uuid.UUID | None] = mapped_column(GUID, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False, index=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
