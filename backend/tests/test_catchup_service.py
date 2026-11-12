"""追赶会话编排集成测试（SQLite + 替身 Celery，真实跑 merge_engine 落账逻辑）。

覆盖需求口径：
  - 按批量逐批追最新位点，每批成功才推进安全位点；
  - 遇空洞停在其前，补洞后自动继续；
  - 批次失败后水位不动，续跑从最后确认位置重新规划；
  - 重复执行不新增在飞批次、不跳过未处理事件。
"""
from __future__ import annotations

import uuid

import pytest

from src.constants.enums import CatchupStatus, RunStatus, StreamStatus
from src.models.cdc_event import CdcEvent
from src.models.merge_run import MergeRun
from src.services import catchup_service
from src.utils.errors import AppError

from tests.catchup_helpers import (
    add_event as _add_event,
    execute_run as _execute_run,
    make_stream as _make_stream,
)


def _open(db, stream, batch_size=5):
    return catchup_service.start_session(db, str(stream.id), batch_size, str(uuid.uuid4()))


# --------------------------------------------------------------------- 用例 ----


def test_batch_by_batch_advances_watermark_only_after_commit(db_session, patch_dispatch):
    db = db_session
    stream = _make_stream(db, max_lsn=12)
    for lsn in range(1, 13):
        _add_event(db, stream, lsn, lsn)

    session = _open(db, stream, batch_size=5)
    # 第一批 [1,5] 在飞，安全位点仍是 0
    assert session.status == CatchupStatus.ACTIVE.value
    assert session.safe_lsn == 0
    assert session.current_run_id is not None
    first_run = session.current_run_id

    _execute_run(db, first_run)
    db.refresh(session)
    db.refresh(stream)
    # 成功后自动投出第二批 [6,10]
    assert stream.watermark_lsn == 5
    assert session.safe_lsn == 5
    assert session.status == CatchupStatus.ACTIVE.value
    assert session.succeeded_batches == 1
    assert session.current_run_id != first_run

    _execute_run(db, session)
    db.refresh(session)
    db.refresh(stream)
    assert stream.watermark_lsn == 10

    # 第三批 [11,12] 跑完即追平
    _execute_run(db, session)
    db.refresh(session)
    assert session.status == CatchupStatus.COMPLETED.value
    assert session.safe_lsn == 12
    assert session.gap_lsn is None
    assert session.finished_at is not None
    assert session.last_recovery_json["kind"] == "completed"


def test_gap_blocks_before_it_and_resumes_after_fill(db_session, patch_dispatch):
    db = db_session
    stream = _make_stream(db, max_lsn=9)
    # LSN 6 缺失（空洞），7..9 已到达
    for lsn in [1, 2, 3, 4, 5, 7, 8, 9]:
        _add_event(db, stream, lsn, lsn)

    session = _open(db, stream, batch_size=10)
    # 第一批只能吃连续前缀 1..5
    assert session.status == CatchupStatus.ACTIVE.value
    _execute_run(db, session)
    db.refresh(session)
    assert session.status == CatchupStatus.BLOCKED.value
    assert session.gap_lsn == 6
    assert session.safe_lsn == 5
    db.refresh(stream)
    assert stream.watermark_lsn == 5
    assert stream.status == StreamStatus.LAGGING.value

    # 阻塞态直接续跑应被拒绝
    with pytest.raises(AppError) as exc:
        catchup_service.resume_session(db, str(session.id))
    assert exc.value.code == "CATCHUP_GAP_BLOCKING"

    # 补齐 LSN 6 后自动继续：下一批 [6,9] 跑完追平
    session = catchup_service.fill_gap(
        db, str(session.id), gap_lsn=6, op="insert", pk_value="PK-6",
        after={"order_id": "PK-6", "amount": 60},
    )
    assert session.gap_lsn is None
    assert session.status == CatchupStatus.ACTIVE.value
    _execute_run(db, session)
    db.refresh(session)
    assert session.status == CatchupStatus.COMPLETED.value
    assert session.safe_lsn == 9


def test_fill_gap_is_idempotent(db_session, patch_dispatch):
    db = db_session
    stream = _make_stream(db, max_lsn=7)
    for lsn in [1, 2, 3, 4, 5, 7]:
        _add_event(db, stream, lsn, lsn)
    session = _open(db, stream, batch_size=10)
    _execute_run(db, session)
    db.refresh(session)
    assert session.status == CatchupStatus.BLOCKED.value

    # 外部先把 LSN 6 补进来
    _add_event(db, stream, 6, seq=99)
    # 再调补洞接口：不重复建事件，直接解阻
    session = catchup_service.fill_gap(
        db, str(session.id), 6, "insert", "PK-6", {"order_id": "PK-6"}
    )
    assert session.gap_lsn is None
    count = len([e for e in db.query(CdcEvent).filter(CdcEvent.lsn == 6).all()])
    assert count == 1


def test_failed_batch_keeps_watermark_and_resume_replans(db_session, patch_dispatch):
    db = db_session
    stream = _make_stream(db, max_lsn=8)
    for lsn in range(1, 9):
        _add_event(db, stream, lsn, lsn)

    session = _open(db, stream, batch_size=4)
    first_id = session.current_run_id
    # 第一批失败
    _execute_run(db, first_id, fail=True)
    db.refresh(session)
    db.refresh(stream)
    assert session.status == CatchupStatus.FAILED.value
    assert stream.watermark_lsn == 0
    assert session.safe_lsn == 0
    assert session.failed_batches == 1
    assert session.last_recovery_json["kind"] == "batch_failed"

    # 续跑：从确认位点 0 重新规划，窗口仍是 [1,4]，不是跳过它
    session = catchup_service.resume_session(db, str(session.id))
    assert session.status == CatchupStatus.ACTIVE.value
    rerun = db.get(MergeRun, session.current_run_id)
    assert (rerun.window_start_lsn, rerun.window_end_lsn) == (1, 4)
    assert rerun.id != first_id  # 失败批次保留历史，新批次重跑同一窗口

    _execute_run(db, rerun.id)
    db.refresh(session)
    db.refresh(stream)
    assert stream.watermark_lsn == 4
    # 自动续第二批 [5,8]
    assert session.status == CatchupStatus.ACTIVE.value
    _execute_run(db, session)
    db.refresh(session)
    assert session.status == CatchupStatus.COMPLETED.value
    assert session.safe_lsn == 8


def test_repeated_start_does_not_create_duplicate_inflight(db_session, patch_dispatch):
    db = db_session
    stream = _make_stream(db, max_lsn=6)
    for lsn in range(1, 7):
        _add_event(db, stream, lsn, lsn)

    first = _open(db, stream, batch_size=3)
    again = catchup_service.start_session(db, str(stream.id), 3, str(uuid.uuid4()))
    assert again.id == first.id
    assert again.current_run_id == first.current_run_id
    # 只有一个在飞批次
    inflight = (
        db.query(MergeRun)
        .filter(MergeRun.status.in_([RunStatus.QUEUED.value, RunStatus.RUNNING.value]))
        .count()
    )
    assert inflight == 1


def test_pause_stops_at_safe_point_after_current_batch(db_session, patch_dispatch):
    db = db_session
    stream = _make_stream(db, max_lsn=9)
    for lsn in range(1, 10):
        _add_event(db, stream, lsn, lsn)
    session = _open(db, stream, batch_size=3)

    # 第一批在飞时请求暂停
    catchup_service.pause_session(db, str(session.id))
    _execute_run(db, session)
    db.refresh(session)
    db.refresh(stream)
    # 第一批确认落账（水位到 3），但不自动续批，停在安全点
    assert stream.watermark_lsn == 3
    assert session.status == CatchupStatus.PAUSED.value
    assert session.current_run_id is None

    # 恢复后继续
    session = catchup_service.resume_session(db, str(session.id))
    assert session.status == CatchupStatus.ACTIVE.value
    _execute_run(db, session)
    db.refresh(session)
    assert session.safe_lsn == 6


def test_duplicate_deliveries_are_deduped_within_batch(db_session, patch_dispatch):
    db = db_session
    stream = _make_stream(db, max_lsn=5)
    for lsn in range(1, 6):
        _add_event(db, stream, lsn, lsn, pk=f"PK-{lsn}")
    # LSN 2 重复投递两次
    _add_event(db, stream, 2, seq=10, pk="PK-2")
    _add_event(db, stream, 2, seq=11, pk="PK-2")

    session = _open(db, stream, batch_size=10)
    run = _execute_run(db, session)
    assert run.duplicate_count == 2
    db.refresh(session)
    assert session.status == CatchupStatus.COMPLETED.value
    assert session.safe_lsn == 5


def test_reconcile_stale_run_after_worker_lost(db_session, patch_dispatch):
    db = db_session
    stream = _make_stream(db, max_lsn=6)
    for lsn in range(1, 7):
        _add_event(db, stream, lsn, lsn)
    session = _open(db, stream, batch_size=3)
    run_id = session.current_run_id

    # worker 把批次跑完但回调丢失：run 已 succeeded，会话还以为在飞
    run = db.get(MergeRun, run_id)
    run.status = RunStatus.SUCCEEDED.value
    run.watermark_after = 3
    run.result_digest = "sha256:stale"
    stream.watermark_lsn = 3
    db.commit()

    session = catchup_service.resume_session(db, str(session.id))
    db.refresh(session)
    # 对账后确认位点对齐并自动续上 [4,6]
    assert session.safe_lsn == 3
    assert session.status == CatchupStatus.ACTIVE.value
    rerun = db.get(MergeRun, session.current_run_id)
    assert (rerun.window_start_lsn, rerun.window_end_lsn) == (4, 6)


def test_start_without_backlog_rejected(db_session, patch_dispatch):
    db = db_session
    stream = _make_stream(db, max_lsn=3, watermark=3)
    for lsn in range(1, 4):
        _add_event(db, stream, lsn, lsn)
    with pytest.raises(AppError) as exc:
        _open(db, stream)
    assert exc.value.code == "CATCHUP_NO_BACKLOG"


def test_only_one_open_session_per_stream(db_session, patch_dispatch):
    db = db_session
    stream = _make_stream(db, max_lsn=4)
    for lsn in range(1, 5):
        _add_event(db, stream, lsn, lsn)
    session = _open(db, stream, batch_size=2)
    _execute_run(db, session)
    db.refresh(session)
    _execute_run(db, session)
    db.refresh(session)
    assert session.status == CatchupStatus.COMPLETED.value

    # 追平后允许再开一个新会话（历史 completed 不拦）
    stream.max_lsn = 8
    for lsn in range(5, 9):
        _add_event(db, stream, lsn, lsn)
    second = _open(db, stream, batch_size=2)
    assert second.id != session.id
    assert second.status == CatchupStatus.ACTIVE.value
