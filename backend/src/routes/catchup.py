from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from src.config.database import get_db
from src.controllers import catchup_controller
from src.middlewares.rbac import require_permission
from src.middlewares.rate_limit import rate_limit
from src.models.user import User
from src.schemas.catchup import CatchupGapFillRequest, CatchupStartRequest

router = APIRouter(prefix="/catchup", tags=["catchup"])


@router.get("/sessions")
def list_sessions(
    limit: int = 30,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("catchup:read")),
) -> dict:
    return catchup_controller.list_sessions(min(limit, 200), db)


@router.get("/streams/{stream_id}/session")
def get_stream_session(
    stream_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("catchup:read")),
) -> dict:
    return catchup_controller.get_stream_session(stream_id, db)


@router.get("/sessions/{session_id}")
def get_session(
    session_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("catchup:read")),
) -> dict:
    return catchup_controller.get_session(session_id, db)


@router.post("/sessions")
def start_session(
    request: Request,
    payload: CatchupStartRequest,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("catchup:trigger")),
    _rate: None = rate_limit("runs"),
) -> dict:
    return catchup_controller.start_session(request, payload, user, db)


@router.post("/sessions/{session_id}/resume")
def resume_session(
    request: Request,
    session_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("catchup:trigger")),
    _rate: None = rate_limit("runs"),
) -> dict:
    return catchup_controller.resume_session(request, session_id, user, db)


@router.post("/sessions/{session_id}/pause")
def pause_session(
    request: Request,
    session_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("catchup:trigger")),
) -> dict:
    return catchup_controller.pause_session(request, session_id, user, db)


@router.post("/sessions/{session_id}/fill-gap")
def fill_gap(
    request: Request,
    session_id: str,
    payload: CatchupGapFillRequest,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("catchup:trigger")),
    _rate: None = rate_limit("runs"),
) -> dict:
    return catchup_controller.fill_gap(request, session_id, payload, user, db)
