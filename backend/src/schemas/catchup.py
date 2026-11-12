from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class CatchupStartRequest(BaseModel):
    stream_id: str
    batch_size: int = Field(default=20, ge=1, le=500)


class CatchupGapFillRequest(BaseModel):
    gap_lsn: int = Field(ge=1)
    op: str = Field(default="insert", pattern="^(insert|update|delete)$")
    pk_value: str | None = None
    after: dict[str, Any] | None = None
    before: dict[str, Any] | None = None
