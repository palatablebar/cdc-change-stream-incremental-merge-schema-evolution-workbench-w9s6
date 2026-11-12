"""连续增量追赶的批次规划（纯函数，无 IO，worker / service / 单测共用一份）。

设计口径（与需求逐条对齐）：
  - 按可控批量向最新位点追赶：每批最多 batch_size 个**不同 LSN**；
  - 遇到事件位点空洞就停在其前：只规划 safe_lsn 之后的 LSN 连续前缀，
    第一个缺失 LSN（空洞）及其后的事件不进本批，gap_lsn 如实返回；
  - 重复投递（同一 LSN 多条）不算新位点，但仍随其 LSN 所在批次一起被归并去重；
  - 规划结果只描述「这一批该跑哪个窗口、有多少事件」，真正落账与水位推进由 worker 完成。
"""
from __future__ import annotations

from typing import Any, Sequence


def plan_batch(
    available_lsns: Sequence[int],
    safe_lsn: int,
    max_lsn: int,
    batch_size: int,
) -> dict[str, Any]:
    """给定流上已到达的全部 LSN（可乱序、可重复），规划下一批。

    返回：
      start_lsn        下一批窗口起点（safe_lsn + 1）
      end_lsn          本批窗口终点（含）；无事件可跑时为 safe_lsn
      event_lsns       本批覆盖的不同 LSN（升序，已截断到 batch_size 且不跨空洞）
      event_count      本批窗口内的事件条数（含同 LSN 的重复投递）
      gap_lsn          阻塞空洞 LSN；无空洞为 None
      remaining_events 剩余积压事件数（本批之后、含空洞后的已到达事件）
      caught_up        是否已追到最新位点
    """
    start_lsn = int(safe_lsn) + 1
    distinct = sorted({int(lsn) for lsn in available_lsns if int(lsn) > int(safe_lsn)})

    if not distinct or int(max_lsn) <= int(safe_lsn):
        return {
            "start_lsn": start_lsn,
            "end_lsn": int(safe_lsn),
            "event_lsns": [],
            "event_count": 0,
            "gap_lsn": None,
            "remaining_events": 0,
            "caught_up": True,
        }

    # 连续前缀：第一个「期望 LSN 没到达」的位置就是空洞，停在它之前
    prefix: list[int] = []
    expected = start_lsn
    gap_ahead: int | None = None
    for lsn in distinct:
        if lsn != expected:
            gap_ahead = expected
            break
        prefix.append(lsn)
        expected += 1

    batch_size = max(1, int(batch_size))
    batch_lsns = prefix[:batch_size]
    # 空洞只有在「连续前缀填不满本批」时才真正阻塞当前批次；
    # 若本批已被连续位点填满，空洞在批次之后，本轮先跑，下一轮重新探测到队首再阻塞。
    gap_lsn = gap_ahead if len(prefix) < batch_size else None
    end_lsn = batch_lsns[-1] if batch_lsns else int(safe_lsn)

    # 事件条数按「窗口内的原始事件」统计（含重复投递），由调用方传入更精确；
    # 这里用 LSN 序列统计，调用方可用数据库 count 覆盖 event_count。
    window_set = set(batch_lsns)
    event_count = sum(1 for lsn in available_lsns if int(lsn) in window_set)
    remaining_events = sum(1 for lsn in available_lsns if int(lsn) > end_lsn)

    # 只有「确认位点已到最新位点、确实没有剩余事件」才算追平；
    # 前缀为空也可能是被空洞挡住（distinct 非空），不能误判追平。
    caught_up = not distinct and int(max_lsn) <= int(safe_lsn)
    return {
        "start_lsn": start_lsn,
        "end_lsn": end_lsn,
        "event_lsns": batch_lsns,
        "event_count": event_count,
        "gap_lsn": gap_lsn,
        "remaining_events": remaining_events,
        "caught_up": caught_up,
    }


def describe_recovery(
    kind: str,
    safe_lsn: int,
    detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """统一构造「最近一次恢复结果」结构，页面直接渲染。"""
    from datetime import datetime, timezone

    return {
        "kind": kind,
        "at": datetime.now(timezone.utc).isoformat(),
        "safe_lsn": int(safe_lsn),
        "detail": detail or {},
    }
