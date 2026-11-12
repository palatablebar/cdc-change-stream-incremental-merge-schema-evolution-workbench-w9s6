"""追赶测试共享造数与 worker 替身（不依赖 broker/真实 worker）。"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from src.constants.enums import RunStatus
from src.models.cdc_event import CdcEvent
from src.models.change_stream import ChangeStream
from src.models.merge_run import MergeRun
from src.services import catchup_service, run_service
from src.services.merge_engine import compute_merge


class FakeTaskResult:
    def __init__(self, task_id: str):
        self.id = task_id


def install_fake_dispatch(monkeypatch):
    """拦截 Celery 投递：只建 queued 批次；执行由 execute_run 显式驱动。"""
    sent: list[str] = []

    def fake_send_task(name, args=None, **kwargs):
        sent.append(args[0])
        return FakeTaskResult(f"celery-task-{len(sent)}")

    monkeypatch.setattr(run_service.celery_app, "send_task", fake_send_task)
    return sent


def make_stream(db, name=None, max_lsn=0, watermark=0, status="healthy"):
    stream = ChangeStream(
        name=name or f"s{uuid.uuid4().hex[:10]}",
        source_db="shop",
        source_table="shop.t",
        pk_column="order_id",
        watermark_lsn=watermark,
        max_lsn=max_lsn,
        status=status,
    )
    db.add(stream)
    db.commit()
    db.refresh(stream)
    return stream


def add_event(db, stream, lsn, seq=None, op="insert", pk=None, after=None, before=None, reject=None):
    db.add(
        CdcEvent(
            stream_id=stream.id,
            lsn=lsn,
            received_seq=seq if seq is not None else lsn,
            op=op,
            pk_value=pk if pk is not None else f"PK-{lsn}",
            before_json=before,
            after_json=after if after is not None else {"order_id": pk or f"PK-{lsn}", "amount": lsn * 10},
            reject_reason=reject,
            reject_detail=None,
            received_at=datetime.now(timezone.utc),
        )
    )
    db.commit()


def execute_run(db, target, fail: bool = False):
    """模拟 worker：跑真实 compute_merge，成功推进水位并回调会话；target 可为 run_id 或会话。"""
    from src.models.catchup_session import CatchupSession

    if isinstance(target, CatchupSession):
        run_id = db.get(CatchupSession, target.id).current_run_id
    else:
        run_id = target
    run = db.get(MergeRun, run_id)
    if fail:
        run.status = RunStatus.FAILED.value
        run.error_message = "模拟批次失败"
        run.finished_at = datetime.now(timezone.utc)
        db.commit()
        catchup_service.handle_batch_finished(db, run)
        return run

    stream = db.get(ChangeStream, run.stream_id)
    rows = (
        db.query(CdcEvent)
        .filter(
            CdcEvent.stream_id == stream.id,
            CdcEvent.lsn >= run.window_start_lsn,
            CdcEvent.lsn <= run.window_end_lsn,
        )
        .order_by(CdcEvent.lsn.asc(), CdcEvent.received_seq.asc())
        .all()
    )
    payload = [
        {
            "id": str(row.id),
            "lsn": row.lsn,
            "received_seq": row.received_seq,
            "op": row.op,
            "pk_value": row.pk_value,
            "before_json": row.before_json,
            "after_json": row.after_json,
            "reject_reason": row.reject_reason,
            "reject_detail": row.reject_detail,
        }
        for row in rows
    ]
    result = compute_merge(
        payload,
        pk_column=stream.pk_column,
        window_start=run.window_start_lsn,
        window_end=run.window_end_lsn,
    )
    counts = result["counts"]
    run.received_count = counts["received"]
    run.processed_count = counts["processed"]
    run.duplicate_count = counts["duplicate"]
    run.rejected_count = counts["rejected"]
    run.inserted_count = counts["inserted"]
    run.updated_count = counts["updated"]
    run.deleted_count = counts["deleted"]
    run.upserted_count = counts["upserted"]
    run.watermark_after = result["watermark_after"]
    run.result_digest = result["digest"]
    run.result_json = result
    run.status = RunStatus.SUCCEEDED.value
    run.finished_at = datetime.now(timezone.utc)
    if run.window_start_lsn <= int(stream.watermark_lsn) + 1:
        stream.watermark_lsn = max(int(stream.watermark_lsn), int(result["watermark_after"]))
    db.commit()
    catchup_service.handle_batch_finished(db, run)
    db.refresh(run)
    return run
