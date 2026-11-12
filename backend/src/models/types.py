"""跨方言 UUID 类型：PostgreSQL 用原生 UUID，SQLite 等用 CHAR(32) 存规范十六进制。

生产数据库始终是 PostgreSQL（走原生 UUID，零开销）；本地无 PG 的测试环境用
SQLite 建表时自动回退为 CHAR(32)，并在绑定/取回时完成 uuid.UUID ↔ str 转换，
业务代码与模型声明仍然统一写 uuid.UUID。
"""
from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.types import CHAR, TypeDecorator


class GUID(TypeDecorator):
    """平台无关的 UUID 类型。"""

    impl = CHAR
    cache_ok = True

    def load_dialect_impl(self, dialect: Any) -> Any:
        if dialect.name == "postgresql":
            return dialect.type_descriptor(PG_UUID(as_uuid=True))
        return dialect.type_descriptor(CHAR(32))

    def process_bind_param(self, value: Any, dialect: Any) -> str | uuid.UUID | None:
        if value is None:
            return None
        if dialect.name == "postgresql":
            return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
        return value.hex if isinstance(value, uuid.UUID) else uuid.UUID(str(value)).hex

    def process_result_value(self, value: Any, dialect: Any) -> uuid.UUID | None:
        if value is None:
            return None
        return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
