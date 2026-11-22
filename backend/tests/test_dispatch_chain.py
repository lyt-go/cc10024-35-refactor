"""调度链来源交接测试：脚本 → 计划 → 启动回执 → 批次 → 结算 → 运行历史。

覆盖点：
- 同一脚本先后生效生成两份自动计划，各带独立的来源回执；
- 同一请求身份重试只对应一个批次，不同身份则各自生成新批次；
- 回执写出后、批次创建前的中断窗口，重试能补建且不重复；
- 中断窗口内旧批次按 10/6/2 结算，历史仍关联原批次、旧计划与来源脚本；
- 没有来源回执的既有手工批次按原字段与完成行为可读。
"""
from __future__ import annotations

import os
import sys

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from app import persistence as persist

for sub in ["runs", "snapshots", "reports", "exports"]:
    p = persist.data_path(sub, "")
    if os.path.isdir(p):
        for f in os.listdir(p):
            os.remove(os.path.join(p, f))


def _fresh_script(code: str):
    from app.services.behavior_script import BehaviorScriptService

    svc = BehaviorScriptService()
    entry, missing = svc.create_entry({
        "脚本编号": code,
        "目标站点": "chain.example.com",
        "录制步骤": "open,extract",
    })
    assert entry is not None, missing
    return svc, entry


def _plans_for(code: str):
    from app.store import store

    return [r for r in store.rows("script_schedule") if r.get("关联脚本") == code]


def _batches_for(plan_code: str):
    from app.store import store

    return [r for r in store.rows("batch_queue") if r.get("关联计划") == plan_code]


def test_script_reactive_creates_two_auto_plans():
    """同一脚本先后生效两次，应生成两份自动计划且各连回脚本。"""
    from app.services import dispatch

    svc, entry = _fresh_script("CHAIN-001")
    svc.run_action(entry["id"], "标记生效")
    svc.run_action(entry["id"], "标记生效")

    plans = _plans_for("CHAIN-001")
    assert len(plans) == 2, f"预期两份自动计划，实际 {len(plans)}"
    plan_codes = {p["计划编号"] for p in plans}
    assert len(plan_codes) == 2, "两份计划应有不同计划编号"

    for plan in plans:
        assert plan["调度回执"], "自动计划应带来源回执号"
        receipt = dispatch.receipt_for_plan(plan)
        assert receipt is not None, f"计划 {plan['计划编号']} 找不到来源回执"
        assert receipt["来源脚本"] == "CHAIN-001"
        assert receipt["来源计划"] == plan["计划编号"]
        assert receipt["脚本ID"] == entry["id"]

    receipts = [dispatch.receipt_for_plan(p)["回执编号"] for p in plans]
    assert len(set(receipts)) == 2, "两份计划的回执应各自独立"


def test_legacy_manual_batch_unchanged():
    """没有来源回执的既有手工批次：原字段与完成行为保持不变。"""
    from app.services.batch_queue import BatchQueueService
    from app.services import dispatch
    from app import persistence as persist

    svc = BatchQueueService()
    entry, _ = svc.create_entry({
        "批次编号": "BATC-MANUAL-LEGACY",
        "关联计划": "SCRI-LEGACY",
        "任务数量": 5,
    })
    entry["完成数"] = "3"       # 手工维护的统计字段
    entry["失败数"] = "1"
    assert dispatch.receipt_for_batch(entry) is None, "手工批次不应凭空生成回执"

    result, _ = svc.run_action(entry["id"], "确认完成")
    assert result is not None and result["status"] == "已完成"

    run = persist.read_jsonl("runs", "run_history.jsonl")[-1]
    assert run["action"] == "auto_create_from_batch"
    assert run["entry_id"] == result["id"]
    # 原字段：采集行数=任务数量、失败行数=失败数
    assert run.get("status") == "已完成"
    runs = [r for r in __import__("app.store", fromlist=["store"]).store.rows("run_history")
            if r.get("关联计划") == "SCRI-LEGACY"]
    assert runs, "手工批次完成应写入运行历史"
    record = runs[-1]
    assert record["采集行数"] == "5", f"采集行数应为任务数量 5，实际 {record['采集行数']}"
    assert record["失败行数"] == "1", f"失败行数应保留 1，实际 {record['失败行数']}"
    assert "调度回执" not in record, "旧记录不应被补写来源回执"
    assert "实际完成数" not in record, "旧记录不应被补写实际完成数"


def test_start_interruption_and_retry_identity():
    """回执写出但批次未创建的中断窗口：同身份重试不重复建批次，异身份另起一批。"""
    from app.store import store
    from app.services.script_schedule import ScriptScheduleService
    from app.services.batch_queue import BatchQueueService
    from app.services import dispatch

    svc, script = _fresh_script("CHAIN-002")
    svc.run_action(script["id"], "标记生效")
    svc.run_action(script["id"], "标记生效")
    plans = _plans_for("CHAIN-002")
    old_plan = plans[0]
    new_plan = plans[1]

    # 旧计划先准备一份已排队的批次（作为“正在完成”的既有批次）。
    sched = ScriptScheduleService()
    sched.run_action(old_plan["id"], "启动运行")
    old_batches = _batches_for(old_plan["计划编号"])
    assert len(old_batches) == 1
    old_batch = old_batches[0]
    assert old_batch["来源脚本"] == "CHAIN-002"
    assert old_batch["来源计划"] == old_plan["计划编号"]
    start_receipt = dispatch.receipt_for_batch(old_batch)
    assert start_receipt is not None
    assert start_receipt["计划回执"] == dispatch.receipt_for_plan(old_plan)["回执编号"]

    # 新计划发起身份 A 的启动，并在写回执时断开：只落回执，不建批次。
    with dispatch.RECEIPT_LOCK:
        receipt_a = dispatch.write_start_receipt(
            plan=new_plan,
            script_id=script["id"],
            script_code="CHAIN-002",
            request_identity="A",
        )
    assert receipt_a["批次ID"] is None
    assert _batches_for(new_plan["计划编号"]) == [], "中断窗口内不应创建批次"

    batches_before = len(store.rows("batch_queue"))
    history_before = len(store.rows("run_history"))

    # 窗口内：原批次按任务数 10、完成数 6、失败数 2 结算。
    batch_svc = BatchQueueService()
    batch_svc.run_action(
        old_batch["id"], "确认完成",
        context={"identity": "A", "values": {"实际完成数": 6, "失败数": 2}},
    )

    # 用同一请求身份 A 重试：复用回执、补建批次，不重复。
    sched.run_action(new_plan["id"], "启动运行", context={"identity": "A"})
    # 不同身份 B 启动：另一次有意启动，生成新批次。
    sched.run_action(new_plan["id"], "启动运行", context={"identity": "B"})

    new_batches = _batches_for(new_plan["计划编号"])
    assert len(new_batches) == 2, f"应只新增 A、B 两个批次，实际 {len(new_batches)}"
    assert len(store.rows("batch_queue")) == batches_before + 2, "重复身份不应多算批次"
    identities = sorted(b["启动标识"] for b in new_batches)
    assert identities == ["A", "B"], identities

    # 运行历史：新增一条，仍关联原批次、旧计划和来源脚本。
    assert len(store.rows("run_history")) == history_before + 1
    record = store.rows("run_history")[-1]
    assert record["批次编号"] == old_batch["批次编号"]
    assert record["关联计划"] == old_plan["计划编号"]
    assert record["来源计划"] == old_plan["计划编号"]
    assert record["来源脚本"] == "CHAIN-002"
    assert record["采集行数"] == "10", record["采集行数"]
    assert record["失败行数"] == "2", record["失败行数"]
    assert record["实际完成数"] == "6", record["实际完成数"]

    # 调度回执另记实际完成 6、失败 2，并保持原来源。
    settled = dispatch.receipt_for_batch(old_batch)
    assert settled is not None
    assert settled["实际完成数"] == "6"
    assert settled["失败数"] == "2"
    assert settled["来源脚本"] == "CHAIN-002"
    assert settled["来源计划"] == old_plan["计划编号"]

    # 后来的脚本与计划不能把已结算批次改到别的来源。
    old_batch["来源脚本"] = "TAMPERED"
    assert dispatch.receipt_for_batch(old_batch)["来源脚本"] == "CHAIN-002"

    # 旧计划与来源脚本仍可通过批次连回。
    assert dispatch.receipt_for_batch(old_batch)["来源脚本"] == "CHAIN-002"
    assert dispatch.receipt_for_plan(old_plan)["来源脚本"] == "CHAIN-002"


def test_same_identity_does_not_double_batch():
    """同一身份反复确认同一启动，只保留一个批次。"""
    from app.store import store
    from app.services.script_schedule import ScriptScheduleService

    svc, script = _fresh_script("CHAIN-003")
    svc.run_action(script["id"], "标记生效")
    plan = _plans_for("CHAIN-003")[0]
    sched = ScriptScheduleService()
    for _ in range(3):
        sched.run_action(plan["id"], "启动运行", context={"identity": "same"})
    assert len(_batches_for(plan["计划编号"])) == 1


if __name__ == "__main__":
    test_script_reactive_creates_two_auto_plans()
    print("PASS: test_script_reactive_creates_two_auto_plans")
    test_legacy_manual_batch_unchanged()
    print("PASS: test_legacy_manual_batch_unchanged")
    test_start_interruption_and_retry_identity()
    print("PASS: test_start_interruption_and_retry_identity")
    test_same_identity_does_not_double_batch()
    print("PASS: test_same_identity_does_not_double_batch")
    print("\nALL DISPATCH CHAIN TESTS PASSED")
