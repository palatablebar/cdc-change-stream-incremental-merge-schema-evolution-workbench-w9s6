"""批次规划器纯函数测试：可控批量、空洞截断、重复投递不占位点。"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.services.catchup_planner import plan_batch


def test_plan_fills_batch_with_continuous_prefix():
    # safe=0，已到达 1..25，批量 10 → 第一批窗口 [1,10]
    plan = plan_batch(list(range(1, 26)), safe_lsn=0, max_lsn=25, batch_size=10)
    assert plan["start_lsn"] == 1
    assert plan["end_lsn"] == 10
    assert plan["event_lsns"] == list(range(1, 11))
    assert plan["gap_lsn"] is None
    assert plan["remaining_events"] == 15
    assert plan["caught_up"] is False


def test_plan_stops_before_gap():
    # 缺 LSN 6：连续前缀只有 1..5，必须停在空洞 6 之前
    lsns = [1, 2, 3, 4, 5, 7, 8, 9]
    plan = plan_batch(lsns, safe_lsn=0, max_lsn=9, batch_size=10)
    assert plan["event_lsns"] == [1, 2, 3, 4, 5]
    assert plan["end_lsn"] == 5
    assert plan["gap_lsn"] == 6
    assert plan["caught_up"] is False


def test_plan_duplicate_lsn_does_not_consume_batch_slot():
    # LSN 3 重复投递 3 次：不同位点仍只有 5 个，批量 5 能整批装下
    lsns = [1, 2, 3, 3, 3, 4, 5]
    plan = plan_batch(lsns, safe_lsn=0, max_lsn=5, batch_size=5)
    assert plan["event_lsns"] == [1, 2, 3, 4, 5]
    assert plan["event_count"] == 7  # 窗口内事件条数含重复
    # 本批仍有事件要跑，caught_up 等这批跑完、下一轮无事件时才为真
    assert plan["caught_up"] is False
    assert plan["remaining_events"] == 0


def test_plan_resumes_from_safe_lsn():
    # safe=10，下一批从 11 开始
    plan = plan_batch(list(range(1, 31)), safe_lsn=10, max_lsn=30, batch_size=10)
    assert plan["start_lsn"] == 11
    assert plan["end_lsn"] == 20
    assert plan["remaining_events"] == 10


def test_plan_gap_right_after_safe_lsn():
    # safe=10，LSN 11 缺失、12 已到达：立刻阻塞在 11
    plan = plan_batch([12, 13], safe_lsn=10, max_lsn=13, batch_size=10)
    assert plan["event_lsns"] == []
    assert plan["end_lsn"] == 10
    assert plan["gap_lsn"] == 11


def test_plan_caught_up_when_no_events_after_safe():
    plan = plan_batch([1, 2, 3], safe_lsn=3, max_lsn=3, batch_size=10)
    assert plan["caught_up"] is True
    assert plan["remaining_events"] == 0
    assert plan["event_lsns"] == []


def test_plan_unordered_input_is_sorted():
    plan = plan_batch([5, 1, 3, 2, 4], safe_lsn=0, max_lsn=5, batch_size=3)
    assert plan["event_lsns"] == [1, 2, 3]
    assert plan["end_lsn"] == 3


def test_plan_multi_batch_then_gap():
    # 1..20 连续，21 缺失；safe=10 批量 5 → 本批 [11,15]，空洞留待后续探测
    lsns = list(range(1, 21)) + [22]
    plan = plan_batch(lsns, safe_lsn=10, max_lsn=22, batch_size=5)
    assert plan["event_lsns"] == [11, 12, 13, 14, 15]
    assert plan["gap_lsn"] is None
    assert plan["remaining_events"] == 6  # 16..20 + 22
