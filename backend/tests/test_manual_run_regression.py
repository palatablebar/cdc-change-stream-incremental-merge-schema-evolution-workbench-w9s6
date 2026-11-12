"""手动归并回归：确认 run_service 重构后原 /runs 校验与建批路径不变。"""
from __future__ import annotations
import uuid
import pytest
from src.constants.enums import RunStatus
from src.services import run_service
from src.utils.errors import AppError
from tests.catchup_helpers import add_event, make_stream


def test_manual_run_still_validates_and_creates(db_session, patch_dispatch):
    db = db_session
    stream = make_stream(db, max_lsn=3)
    for lsn in range(1, 4):
        add_event(db, stream, lsn)
    user = str(uuid.uuid4())

    # 空窗口 → WINDOW_EMPTY
    with pytest.raises(AppError) as exc:
        run_service.create_run(db, str(stream.id), 100, 200, user)
    assert exc.value.code == "WINDOW_EMPTY"

    # 默认窗口（水位+1 到 max）正常建批，且不归属任何追赶会话
    run = run_service.create_run(db, str(stream.id), None, None, user)
    assert run.status == RunStatus.QUEUED.value
    assert run.catchup_session_id is None
    assert (run.window_start_lsn, run.window_end_lsn) == (1, 3)

    # 已有在飞批次时再发起 → RUN_ALREADY_ACTIVE
    with pytest.raises(AppError) as exc2:
        run_service.create_run(db, str(stream.id), None, None, user)
    assert exc2.value.code == "RUN_ALREADY_ACTIVE"
