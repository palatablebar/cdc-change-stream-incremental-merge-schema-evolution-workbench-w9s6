"""连续增量追赶服务：按可控批量追最新位点，批次确认后才推进水位线。

关键不变量：
  1. safe_lsn（= stream.watermark_lsn）只在某一批 MergeRun 成功落账后推进，
     批次失败/中断时 safe_lsn 原地不动，续跑从最后确认位置重新规划；
  2. 规划只取连续 LSN 前缀，遇到空洞停在空洞之前（gap_lsn 记录阻塞位点）；
  3. 每批复用 run_service.create_batch_run → 同一条 MergeRun/Celery 落库与投递路径，
     重复执行不新增在飞批次（有 queued/running 批次时直接返回当前会话）；
  4. 会话状态机：active（批次在飞）→ idle / blocked / completed / failed，
     paused 是值班同学的主动意图：当前批次跑完后不再自动续批。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.constants import error_codes as codes
from src.constants.enums import CatchupStatus, RunStatus, StreamStatus
from src.models.catchup_session import CatchupSession
from src.models.cdc_event import CdcEvent
from src.models.change_stream import ChangeStream
from src.models.merge_run import MergeRun
from src.services import run_service
from src.services.catchup_planner import describe_recovery, plan_batch
from src.utils.errors import AppError, not_found
from src.utils.lsn import LsnWindow

DEFAULT_BATCH_SIZE = 20
MAX_BATCH_SIZE = 500


# --------------------------------------------------------------------- 查询 ----


def get_open_session(db: Session, stream_id: str) -> CatchupSession | None:
    return (
        db.query(CatchupSession)
        .filter(
            CatchupSession.stream_id == UUID(stream_id),
            CatchupSession.status != CatchupStatus.COMPLETED.value,
        )
        .order_by(CatchupSession.created_at.desc())
        .first()
    )


def get_session_or_404(db: Session, session_id: str) -> CatchupSession:
    row = db.get(CatchupSession, UUID(session_id))
    if row is None:
        raise not_found("CatchupSession", session_id)
    return row


def list_sessions(db: Session, limit: int = 30) -> list[CatchupSession]:
    return (
        db.query(CatchupSession)
        .order_by(CatchupSession.created_at.desc())
        .limit(min(limit, 200))
        .all()
    )


def _available_lsns(db: Session, stream_id: UUID) -> list[int]:
    rows = db.query(CdcEvent.lsn).filter(CdcEvent.stream_id == stream_id).all()
    return [int(row[0]) for row in rows]


def _backlog_event_count(db: Session, stream: ChangeStream) -> int:
    return (
        db.query(func.count(CdcEvent.id))
        .filter(CdcEvent.stream_id == stream.id, CdcEvent.lsn > int(stream.watermark_lsn))
        .scalar()
        or 0
    )


# --------------------------------------------------------------------- 操作 ----


def start_session(db: Session, stream_id: str, batch_size: int, user_id: str) -> CatchupSession:
    """发起一次连续追赶；幂等：已有未终结会话时直接返回该会话。"""
    stream = db.get(ChangeStream, UUID(stream_id))
    if stream is None:
        raise not_found("ChangeStream", stream_id)
    if stream.status == StreamStatus.PAUSED.value:
        raise AppError(codes.STREAM_PAUSED, "变更流已暂停，不能发起追赶", f"stream={stream.name}")

    batch_size = _normalize_batch_size(batch_size)
    existing = get_open_session(db, str(stream.id))
    if existing is not None:
        # 重复发起不新建：已在飞就原样返回；处于暂停态则按一次续跑处理
        if existing.status == CatchupStatus.PAUSED.value:
            return resume_session(db, str(existing.id), user_id)
        return existing

    if int(stream.max_lsn) <= int(stream.watermark_lsn):
        raise AppError(
            codes.CATCHUP_NO_BACKLOG,
            "当前没有积压事件，无需追赶",
            f"watermark={stream.watermark_lsn} max_lsn={stream.max_lsn}",
        )

    session = CatchupSession(
        stream_id=stream.id,
        status=CatchupStatus.IDLE.value,
        batch_size=batch_size,
        safe_lsn=int(stream.watermark_lsn),
        total_batches=0,
        succeeded_batches=0,
        failed_batches=0,
        started_by=UUID(user_id),
    )
    db.add(session)
    db.commit()
    db.refresh(session)
    return _dispatch_next(db, session, kind="start", actor_id=user_id)


def resume_session(db: Session, session_id: str, user_id: str | None = None) -> CatchupSession:
    """从最后确认位置续跑：失败/暂停/空闲态都可续，空洞阻塞态需先补洞。"""
    session = get_session_or_404(db, session_id)
    if session.status == CatchupStatus.COMPLETED.value:
        raise AppError(codes.CATCHUP_NOT_RESUMABLE, "会话已追平，无需续跑", f"session={session_id}")
    if session.status == CatchupStatus.BLOCKED.value:
        raise AppError(
            codes.CATCHUP_GAP_BLOCKING,
            f"位点 LSN {session.gap_lsn} 空洞未补齐，补洞后才能继续",
            f"gap_lsn={session.gap_lsn}",
        )

    # 兜底：会话登记的在飞批次已终结但回调缺失（如 worker 宕机），先做一次状态对齐
    _reconcile_stale_run(db, session)
    db.refresh(session)
    if session.status == CatchupStatus.ACTIVE.value:
        # 对账后批次确实仍在飞：续跑是幂等 no-op，不重复投递
        return session

    session.pause_requested = False
    return _dispatch_next(db, session, kind="resume", actor_id=user_id)


def pause_session(db: Session, session_id: str, user_id: str | None = None) -> CatchupSession:
    """标记暂停意图：在飞批次会跑完（安全点），之后不再自动续批。"""
    session = get_session_or_404(db, session_id)
    if session.status == CatchupStatus.COMPLETED.value:
        return session
    if session.status != CatchupStatus.ACTIVE.value:
        session.pause_requested = False
        session.status = CatchupStatus.PAUSED.value
        session.last_recovery_json = describe_recovery(
            "pause", session.safe_lsn, {"by": user_id, "note": "已在当前安全位点暂停"}
        )
        db.commit()
        db.refresh(session)
        return session
    # active：挂起暂停意图（独立标志，不会被批次记账覆盖），等批次终结回调落到 paused
    session.pause_requested = True
    session.last_recovery_json = describe_recovery(
        "pause_requested",
        session.safe_lsn,
        {"by": user_id, "note": "当前批次跑完后暂停，水位线不会越过确认位点"},
    )
    db.commit()
    db.refresh(session)
    return session


def fill_gap(
    db: Session,
    session_id: str,
    gap_lsn: int,
    op: str,
    pk_value: str | None,
    after: dict[str, Any] | None = None,
    before: dict[str, Any] | None = None,
    user_id: str | None = None,
) -> CatchupSession:
    """补齐空洞位点事件（幂等），补齐后自动解除阻塞并继续追赶。"""
    session = get_session_or_404(db, session_id)
    if session.gap_lsn is None:
        raise AppError(codes.CATCHUP_GAP_MISMATCH, "当前没有待补齐的空洞位点", f"session={session_id}")
    if int(gap_lsn) != int(session.gap_lsn):
        raise AppError(
            codes.CATCHUP_GAP_MISMATCH,
            f"补齐位点必须是当前空洞 LSN {session.gap_lsn}",
            f"given={gap_lsn} expected={session.gap_lsn}",
        )

    stream = db.get(ChangeStream, session.stream_id)
    if stream is None:
        raise not_found("ChangeStream", str(session.stream_id))

    # 幂等：该 LSN 已被其他途径补齐（事件已到达），直接解除阻塞
    already = (
        db.query(CdcEvent)
        .filter(CdcEvent.stream_id == stream.id, CdcEvent.lsn == int(gap_lsn))
        .first()
    )
    if already is None:
        next_seq = (
            db.query(func.max(CdcEvent.received_seq)).filter(CdcEvent.stream_id == stream.id).scalar() or 0
        ) + 1
        event = CdcEvent(
            stream_id=stream.id,
            lsn=int(gap_lsn),
            received_seq=int(next_seq),
            op=op,
            pk_value=pk_value,
            before_json=before,
            after_json=after,
            reject_reason=None,
            reject_detail=None,
            received_at=datetime.now(timezone.utc),
        )
        db.add(event)
        if int(stream.max_lsn) < int(gap_lsn):
            stream.max_lsn = int(gap_lsn)
        db.flush()

    session.gap_lsn = None
    session.last_recovery_json = describe_recovery(
        "gap_filled",
        session.safe_lsn,
        {"gap_lsn": int(gap_lsn), "by": user_id, "note": "空洞已补齐，自动继续追赶"},
    )
    db.commit()
    db.refresh(session)
    return _dispatch_next(db, session, kind="gap_filled", actor_id=user_id)


# ------------------------------------------------------------- 批次编排（内部） ----


def _dispatch_next(
    db: Session,
    session: CatchupSession,
    *,
    kind: str,
    actor_id: str | None,
) -> CatchupSession:
    """规划并投递下一批；任何分支都不推进 safe_lsn（推进只发生在批次成功回调里）。"""
    stream = db.get(ChangeStream, session.stream_id)
    if stream is None:
        raise not_found("ChangeStream", str(session.stream_id))

    # 会话与流的确认位点必须一致；不一致以流水位线为准并登记一次纠偏
    if int(session.safe_lsn) != int(stream.watermark_lsn):
        session.safe_lsn = int(stream.watermark_lsn)

    plan = plan_batch(
        available_lsns=_available_lsns(db, stream.id),
        safe_lsn=int(stream.watermark_lsn),
        max_lsn=int(stream.max_lsn),
        batch_size=int(session.batch_size),
    )

    if plan["caught_up"]:
        session.status = CatchupStatus.COMPLETED.value
        session.gap_lsn = None
        session.current_run_id = None
        session.finished_at = datetime.now(timezone.utc)
        session.last_progress_at = datetime.now(timezone.utc)
        session.last_recovery_json = describe_recovery(
            "completed", int(stream.watermark_lsn), {"note": "已追到最新位点，无剩余积压"}
        )
        if stream.status == StreamStatus.LAGGING.value:
            stream.status = StreamStatus.HEALTHY.value
        db.commit()
        db.refresh(session)
        return session

    if plan["gap_lsn"] is not None and not plan["event_lsns"]:
        # 安全位点的下一位点就是空洞：本批无连续事件可跑，停在空洞之前，安全位点不动。
        # 若仍有连续前缀（event_lsns 非空），先把前缀批次跑完，下一轮再在空洞前停下。
        session.status = CatchupStatus.BLOCKED.value
        session.gap_lsn = int(plan["gap_lsn"])
        session.current_run_id = None
        session.last_progress_at = datetime.now(timezone.utc)
        session.last_recovery_json = describe_recovery(
            "blocked",
            int(stream.watermark_lsn),
            {"gap_lsn": plan["gap_lsn"], "note": "遇到位点空洞，已停在空洞之前，补齐后再继续"},
        )
        stream.status = StreamStatus.LAGGING.value
        db.commit()
        db.refresh(session)
        return session

    run = run_service.create_batch_run(
        db,
        stream=stream,
        window=LsnWindow(start=plan["start_lsn"], end=plan["end_lsn"]),
        triggered_by=actor_id,
        catchup_session_id=session.id,
        validate_window=False,
    )
    session.current_run_id = run.id
    session.current_batch_no = int(session.total_batches) + 1
    session.total_batches = int(session.total_batches) + 1
    session.gap_lsn = None
    # 批次在飞期间统一为 active；暂停意图由 pause_requested 独立承载，批次确认后才落 paused
    session.status = CatchupStatus.ACTIVE.value
    session.last_progress_at = datetime.now(timezone.utc)
    session.last_recovery_json = describe_recovery(
        kind,
        int(stream.watermark_lsn),
        {
            "batch_no": session.current_batch_no,
            "window": [plan["start_lsn"], plan["end_lsn"]],
            "event_count": plan["event_count"],
            "run_id": str(run.id),
        },
    )
    # 投递即失败（broker 异常）：直接落 failed，safe_lsn 未动，可一键续跑
    if run.status == RunStatus.FAILED.value:
        session.status = CatchupStatus.FAILED.value
        session.failed_batches = int(session.failed_batches) + 1
        session.last_error = run.error_message
        session.current_run_id = None
        session.last_recovery_json = describe_recovery(
            "dispatch_failed",
            int(stream.watermark_lsn),
            {"batch_no": session.current_batch_no, "error": run.error_message},
        )
    db.commit()
    db.refresh(session)
    return session


def handle_batch_finished(db: Session, run: MergeRun) -> CatchupSession | None:
    """worker 批次终结回调。成功才推进水位线并自动续批；失败停在确认位点等续跑。"""
    if run.catchup_session_id is None:
        return None
    session = db.get(CatchupSession, run.catchup_session_id)
    if session is None:
        return None
    # 幂等防护：该批次已被回调过（current_run_id 已清空且 last_run 指向它），直接返回
    if session.current_run_id != run.id and session.last_run_id == run.id:
        db.refresh(session)
        return session

    session.last_run_id = run.id
    session.current_run_id = None
    session.last_progress_at = datetime.now(timezone.utc)
    want_pause = bool(session.pause_requested)

    if run.status == RunStatus.SUCCEEDED.value:
        stream = db.get(ChangeStream, session.stream_id)
        # 水位线已由 merge_task 在同一事务口径推进；这里对齐会话确认位点
        session.safe_lsn = int(stream.watermark_lsn)
        session.succeeded_batches = int(session.succeeded_batches) + 1
        session.last_error = None
        session.last_recovery_json = describe_recovery(
            "batch_succeeded",
            session.safe_lsn,
            {
                "batch_no": session.current_batch_no,
                "run_id": str(run.id),
                "window": [run.window_start_lsn, run.window_end_lsn],
                "processed": run.processed_count,
                "duplicate": run.duplicate_count,
                "rejected": run.rejected_count,
                "digest": run.result_digest,
            },
        )
        db.commit()
        if want_pause:
            session.status = CatchupStatus.PAUSED.value
            session.pause_requested = False
            session.last_recovery_json = describe_recovery(
                "paused_at_safe_point",
                session.safe_lsn,
                {"note": "当前批次已确认落账，按暂停意图停在安全位点"},
            )
            db.commit()
            db.refresh(session)
            return session
        return _dispatch_next(db, session, kind="auto_continue", actor_id=None)

    # 失败：确认位点不动，记录恢复结果，等待人工/自动续跑
    db.refresh(run)
    session.failed_batches = int(session.failed_batches) + 1
    session.status = CatchupStatus.FAILED.value
    session.last_error = run.error_message
    session.last_recovery_json = describe_recovery(
        "batch_failed",
        session.safe_lsn,
        {
            "batch_no": session.current_batch_no,
            "run_id": str(run.id),
            "window": [run.window_start_lsn, run.window_end_lsn],
            "error": run.error_message,
            "note": "批次失败，水位线停在最后确认位置，续跑将从该位点重新规划",
        },
    )
    db.commit()
    db.refresh(session)
    return session


def _reconcile_stale_run(db: Session, session: CatchupSession) -> None:
    """会话以为批次在飞、实际批次已终结（worker 宕机/回调丢失）时做一次对账。"""
    if session.current_run_id is None:
        return
    run = db.get(MergeRun, session.current_run_id)
    if run is None or run.status not in RunStatus.terminal():
        return
    handle_batch_finished(db, run)


def _normalize_batch_size(batch_size: int | None) -> int:
    value = batch_size or DEFAULT_BATCH_SIZE
    if value < 1:
        raise AppError(codes.VALIDATION_ERROR, "批量大小至少为 1", f"batch_size={value}")
    if value > MAX_BATCH_SIZE:
        raise AppError(codes.VALIDATION_ERROR, f"批量大小不能超过 {MAX_BATCH_SIZE}", f"batch_size={value}")
    return int(value)


# ----------------------------------------------------------------- 序列化 ----


def serialize_session(db: Session, session: CatchupSession) -> dict[str, Any]:
    from src.constants.enums import ENUM_LABELS
    from src.utils.formatters import format_lsn, iso

    stream = db.get(ChangeStream, session.stream_id)
    backlog = _backlog_event_count(db, stream) if stream else 0
    max_lsn = int(stream.max_lsn) if stream else 0
    current_run = None
    if session.current_run_id is not None:
        run = db.get(MergeRun, session.current_run_id)
        if run is not None:
            current_run = {
                "id": str(run.id),
                "status": run.status,
                "window_start_lsn": run.window_start_lsn,
                "window_end_lsn": run.window_end_lsn,
                "batch_no": session.current_batch_no,
                "received_count": run.received_count,
            }
    return {
        "id": str(session.id),
        "stream_id": str(session.stream_id),
        "stream_name": stream.name if stream else None,
        "status": session.status,
        "status_label": ENUM_LABELS["CatchupStatus"].get(session.status, session.status),
        "batch_size": session.batch_size,
        "safe_lsn": session.safe_lsn,
        "safe_lsn_display": format_lsn(session.safe_lsn),
        "max_lsn": max_lsn,
        "max_lsn_display": format_lsn(max_lsn),
        "gap_lsn": session.gap_lsn,
        "gap_lsn_display": format_lsn(session.gap_lsn) if session.gap_lsn is not None else None,
        "backlog_events": backlog,
        "total_batches": session.total_batches,
        "succeeded_batches": session.succeeded_batches,
        "failed_batches": session.failed_batches,
        "current_batch_no": session.current_batch_no,
        "current_run": current_run,
        "pause_requested": bool(session.pause_requested),
        "last_run_id": str(session.last_run_id) if session.last_run_id else None,
        "last_recovery": session.last_recovery_json,
        "last_error": session.last_error,
        "started_at": iso(session.started_at),
        "last_progress_at": iso(session.last_progress_at),
        "finished_at": iso(session.finished_at),
        "created_at": iso(session.created_at),
    }
