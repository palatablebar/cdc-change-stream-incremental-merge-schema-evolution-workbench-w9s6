"""追赶会话 HTTP 链路测试：真实 FastAPI 路由 + JWT + RBAC，DB/Redis/Celery 用替身。"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest import mock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.config.database import get_db
from src.main import app
from src.models import Base
from src.models.cdc_event import CdcEvent
from src.models.change_stream import ChangeStream
from src.models.user import User
from src.services import run_service
from src.utils.jwt import create_access_token
from src.utils.password import hash_password


@compiles(JSONB, "sqlite")
def _render_jsonb_sqlite(element, compiler, **kw):  # noqa: ANN001
    return "JSON"


class _FakeRedis:
    def pipeline(self):
        class _Pipe:
            def zremrangebyscore(self, *a): ...
            def zadd(self, *a): ...
            def zcard(self, *a): return 1
            def expire(self, *a): ...
            def execute(self): return [0, 1, 1, True]
        return _Pipe()

    def setex(self, *a, **k): ...
    def get(self, *a, **k): return None


class _TaskResult:
    def __init__(self, i): self.id = f"task-{i}"


@pytest.fixture()
def client():
    # StaticPool 让内存 SQLite 的所有连接共享同一个底层库，TestClient 才能看到建表数据
    engine = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    for table in Base.metadata.tables.values():
        table.schema = None
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)
    db = Session()

    users = {
        "admin": User(email="a@x.io", password_hash=hash_password("pw"), display_name="A", role="admin", is_active=True),
        "viewer": User(email="v@x.io", password_hash=hash_password("pw"), display_name="V", role="viewer", is_active=True),
    }
    db.add_all(users.values())
    db.commit()
    for u in users.values():
        db.refresh(u)
    tokens = {role: create_access_token(str(u.id), u.role, u.email) for role, u in users.items()}

    stream = ChangeStream(
        name="orders", source_db="s", source_table="s.orders", pk_column="order_id",
        watermark_lsn=0, max_lsn=6, status="healthy",
    )
    db.add(stream)
    db.commit()
    db.refresh(stream)
    for lsn in range(1, 7):
        db.add(CdcEvent(
            stream_id=stream.id, lsn=lsn, received_seq=lsn, op="insert",
            pk_value=f"P{lsn}", after_json={"order_id": f"P{lsn}", "amount": lsn},
            received_at=datetime.now(timezone.utc),
        ))
    db.commit()

    sent: list[str] = []
    run_service.celery_app.send_task = lambda name, args=None, **k: (sent.append(args[0]), _TaskResult(len(sent)))[1]

    def _override_get_db():
        try:
            yield db
        finally:
            pass

    app.dependency_overrides[get_db] = _override_get_db
    with mock.patch("src.middlewares.rate_limit.get_redis", return_value=_FakeRedis()), mock.patch(
        "src.config.app.settings.seed_demo", False
    ):
        with TestClient(app) as c:
            c.headers_update = {}  # type: ignore[attr-defined]
            yield c, tokens, str(stream.id), db, sent

    app.dependency_overrides.clear()
    db.close()
    Base.metadata.drop_all(bind=engine)
    engine.dispose()


def test_viewer_cannot_start_catchup(client):
    c, tokens, stream_id, _, _ = client
    resp = c.post(
        "/api/catchup/sessions",
        json={"stream_id": stream_id, "batch_size": 3},
        headers={"Authorization": f"Bearer {tokens['viewer']}"},
    )
    assert resp.status_code == 403
    assert resp.json()["code"] == "FORBIDDEN"


def test_anonymous_rejected(client):
    c, _, stream_id, _, _ = client
    resp = c.get("/api/catchup/sessions")
    assert resp.status_code == 401


def test_full_catchup_lifecycle_over_http(client):
    c, tokens, stream_id, db, sent = client
    h = {"Authorization": f"Bearer {tokens['admin']}"}

    # 发起追赶，第一批在飞
    resp = c.post("/api/catchup/sessions", json={"stream_id": stream_id, "batch_size": 3}, headers=h)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    session_id = body["id"]
    assert body["status"] == "active"
    assert body["safe_lsn"] == 0
    assert body["current_run"] is not None
    assert body["current_run"]["batch_no"] == 1
    assert body["backlog_events"] == 6

    # 重复发起不新增会话/在飞批次
    again = c.post("/api/catchup/sessions", json={"stream_id": stream_id, "batch_size": 3}, headers=h)
    assert again.json()["id"] == session_id
    assert len(sent) == 1

    # 流维度查询能拿到同一个开放会话
    open_resp = c.get(f"/api/catchup/streams/{stream_id}/session", headers=h)
    assert open_resp.json()["open"] is True
    assert open_resp.json()["session"]["id"] == session_id

    # 列表接口
    lst = c.get("/api/catchup/sessions", headers=h)
    assert lst.status_code == 200
    assert lst.json()["total"] >= 1

    # 暂停
    paused = c.post(f"/api/catchup/sessions/{session_id}/pause", headers=h)
    assert paused.status_code == 200
    assert paused.json()["last_recovery"]["kind"] == "pause_requested"


def test_batch_size_validation(client):
    c, tokens, stream_id, _, _ = client
    h = {"Authorization": f"Bearer {tokens['admin']}"}
    resp = c.post("/api/catchup/sessions", json={"stream_id": stream_id, "batch_size": 0}, headers=h)
    assert resp.status_code == 422
