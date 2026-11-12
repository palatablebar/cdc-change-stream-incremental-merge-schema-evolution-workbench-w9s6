"""端到端不变量：失败 + 续跑全程不跳过事件、不重复落账，最终安全位点连续覆盖全部 LSN。"""
from __future__ import annotations

import uuid

from src.constants.enums import CatchupStatus, RunStatus
from src.models.merge_run import MergeRun
from src.services import catchup_service

from tests.catchup_helpers import add_event, execute_run, make_stream


def _seed(db, n, missing=()):
    stream = make_stream(db, max_lsn=n)
    for lsn in range(1, n + 1):
        if lsn in missing:
            continue
        add_event(db, stream, lsn)
    return stream


def _all_run_windows(db, stream):
    rows = (
        db.query(MergeRun)
        .filter(MergeRun.stream_id == stream.id, MergeRun.status == RunStatus.SUCCEEDED.value)
        .order_by(MergeRun.window_start_lsn.asc())
        .all()
    )
    return [(r.window_start_lsn, r.window_end_lsn) for r in rows]


def test_no_event_skipped_or_duplicated_across_failures(db_session, patch_dispatch):
    db = db_session
    n = 23
    stream = _seed(db, n)
    session = catchup_service.start_session(db, str(stream.id), 5, str(uuid.uuid4()))

    # 让第 2、4 个批次失败，其余成功；每次失败后从确认位点续跑
    executed = 0
    fail_on = {2, 4}
    while True:
        db.refresh(session)
        if session.status == CatchupStatus.COMPLETED.value:
            break
        if session.status != CatchupStatus.ACTIVE.value:
            session = catchup_service.resume_session(db, str(session.id))
            continue
        executed += 1
        will_fail = executed in fail_on
        execute_run(db, session, fail=will_fail)
        db.refresh(session)
        if will_fail:
            assert session.status == CatchupStatus.FAILED.value
            # 失败批次后立刻续跑
            session = catchup_service.resume_session(db, str(session.id))
        assert executed < 50  # 防死循环兜底

    # 最终追平，安全位点等于最新位点
    db.refresh(stream)
    assert session.status == CatchupStatus.COMPLETED.value
    assert session.safe_lsn == n
    assert stream.watermark_lsn == n

    # 成功批次窗口拼接后必须连续、无重叠、完整覆盖 [1,n]
    windows = _all_run_windows(db, stream)
    covered: list[int] = []
    for start, end in windows:
        for lsn in range(start, end + 1):
            assert lsn not in covered, f"LSN {lsn} 被重复落账"
            covered.append(lsn)
    assert covered == list(range(1, n + 1)), "存在未处理事件或顺序错乱"

    # 失败批次不计入成功，且每个失败窗口都被后续批次完整重跑
    assert session.failed_batches == 2
    assert session.succeeded_batches == len(windows)


def test_gap_then_fill_covers_every_lsn_once(db_session, patch_dispatch):
    db = db_session
    stream = _seed(db, 12, missing={7})  # LSN 7 空洞
    session = catchup_service.start_session(db, str(stream.id), 5, str(uuid.uuid4()))

    # 第一批 [1,5]、第二批 [6,6] 成功后，空洞 7 成为下一期望位点，才阻塞
    execute_run(db, session)
    db.refresh(session)
    assert session.status == CatchupStatus.ACTIVE.value
    execute_run(db, session)
    db.refresh(session)
    assert session.status == CatchupStatus.BLOCKED.value
    assert session.gap_lsn == 7
    assert session.safe_lsn == 6

    # 补齐空洞 7
    session = catchup_service.fill_gap(
        db, str(session.id), 7, "insert", "PK-7", {"order_id": "PK-7", "amount": 7}
    )
    # 自动续到追平
    while True:
        db.refresh(session)
        if session.status == CatchupStatus.COMPLETED.value:
            break
        assert session.status == CatchupStatus.ACTIVE.value
        execute_run(db, session)

    windows = _all_run_windows(db, stream)
    covered = [lsn for start, end in windows for lsn in range(start, end + 1)]
    assert covered == list(range(1, 13))
    db.refresh(stream)
    assert stream.watermark_lsn == 12
