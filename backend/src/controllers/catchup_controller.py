"""连续增量追赶 controller。"""
from __future__ import annotations

from fastapi import Request
from sqlalchemy.orm import Session

from src.models.user import User
from src.schemas.catchup import CatchupGapFillRequest, CatchupStartRequest
from src.services import audit_service, catchup_service


def start_session(request: Request, payload: CatchupStartRequest, user: User, db: Session) -> dict:
    session = catchup_service.start_session(db, payload.stream_id, payload.batch_size, str(user.id))
    audit_service.record(
        db,
        action="catchup.start",
        entity_type="CatchupSession",
        entity_id=str(session.id),
        actor_id=str(user.id),
        payload={
            "stream_id": payload.stream_id,
            "batch_size": payload.batch_size,
            "status": session.status,
            "safe_lsn": session.safe_lsn,
        },
        ip=getattr(request.state, "client_ip", None),
        trace_id=getattr(request.state, "trace_id", None),
    )
    return catchup_service.serialize_session(db, session)


def list_sessions(limit: int, db: Session) -> dict:
    rows = catchup_service.list_sessions(db, limit=limit)
    return {"items": [catchup_service.serialize_session(db, row) for row in rows], "total": len(rows)}


def get_session(session_id: str, db: Session) -> dict:
    session = catchup_service.get_session_or_404(db, session_id)
    return catchup_service.serialize_session(db, session)


def get_stream_session(stream_id: str, db: Session) -> dict:
    session = catchup_service.get_open_session(db, stream_id)
    if session is None:
        return {"open": False, "session": None}
    return {"open": True, "session": catchup_service.serialize_session(db, session)}


def resume_session(request: Request, session_id: str, user: User, db: Session) -> dict:
    session = catchup_service.resume_session(db, session_id, str(user.id))
    audit_service.record(
        db,
        action="catchup.resume",
        entity_type="CatchupSession",
        entity_id=str(session.id),
        actor_id=str(user.id),
        payload={"status": session.status, "safe_lsn": session.safe_lsn},
        ip=getattr(request.state, "client_ip", None),
        trace_id=getattr(request.state, "trace_id", None),
    )
    return catchup_service.serialize_session(db, session)


def pause_session(request: Request, session_id: str, user: User, db: Session) -> dict:
    session = catchup_service.pause_session(db, session_id, str(user.id))
    audit_service.record(
        db,
        action="catchup.pause",
        entity_type="CatchupSession",
        entity_id=str(session.id),
        actor_id=str(user.id),
        payload={"status": session.status, "safe_lsn": session.safe_lsn},
        ip=getattr(request.state, "client_ip", None),
        trace_id=getattr(request.state, "trace_id", None),
    )
    return catchup_service.serialize_session(db, session)


def fill_gap(request: Request, session_id: str, payload: CatchupGapFillRequest, user: User, db: Session) -> dict:
    session = catchup_service.fill_gap(
        db,
        session_id,
        payload.gap_lsn,
        payload.op,
        payload.pk_value,
        payload.after,
        payload.before,
        str(user.id),
    )
    audit_service.record(
        db,
        action="catchup.fill_gap",
        entity_type="CatchupSession",
        entity_id=str(session.id),
        actor_id=str(user.id),
        payload={"gap_lsn": payload.gap_lsn, "op": payload.op, "status": session.status},
        ip=getattr(request.state, "client_ip", None),
        trace_id=getattr(request.state, "trace_id", None),
    )
    return catchup_service.serialize_session(db, session)
