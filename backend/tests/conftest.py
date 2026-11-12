"""本地无 PostgreSQL/Redis 环境下的 SQLite 测试夹具。

生产仍跑 PostgreSQL：模型里的 PG 专属类型（UUID/JSONB）在 SQLite 上由 SQLAlchemy
自动回退为 CHAR(32)/JSON，业务代码不感知。测试只验证追赶编排逻辑，不验证方言特性。
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))


@pytest.fixture()
def patch_dispatch(monkeypatch):
    """拦截 Celery 投递：只建 queued 批次，不依赖 broker；批次执行由测试显式驱动。"""
    from tests.catchup_helpers import install_fake_dispatch

    return install_fake_dispatch(monkeypatch)


@pytest.fixture()
def db_session():
    from sqlalchemy.dialects.postgresql import JSONB
    from sqlalchemy.ext.compiler import compiles

    import src.config.database as database
    from src.config.settings import settings
    from src.models import Base

    # PG JSONB 在 SQLite 上渲染为 JSON（UUID 已由跨方言 GUID 类型处理）
    @compiles(JSONB, "sqlite")
    def _render_jsonb_sqlite(element, compiler, **kw):  # noqa: ANN001
        return "JSON"

    engine = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
    )
    # SQLite 没有 schema 概念，把 cdc/dw schema 名清空，表直接建在 main 下
    with mock.patch.object(database, "engine", engine), mock.patch.object(
        settings, "cdc_schema", None
    ), mock.patch.object(settings, "target_schema", None):
        # 模型类上的 __table_args__ 在定义时已绑定 schema，需同步清掉已注册表的 schema
        for table in Base.metadata.tables.values():
            table.schema = None
        TestingSession = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)
        with mock.patch.object(database, "SessionLocal", TestingSession):
            Base.metadata.create_all(bind=engine)
            session = TestingSession()
            try:
                yield session
            finally:
                session.close()
            Base.metadata.drop_all(bind=engine)
            engine.dispose()
