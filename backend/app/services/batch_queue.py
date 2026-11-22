"""批次队列领域服务：采集任务批次管理，消费script_schedule调度命令，写入run_history运行记录。"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.services.base import BaseModuleService, register_hook
from app.services import dispatch
from app import persistence as persist


def ensure_batch_for_receipt(
    receipt: dict[str, Any],
    *,
    plan: dict[str, Any] | None = None,
    script: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """消费一条「计划启动」调度回执：已有批次则复用，否则创建排队中的自动批次。

    幂等边界是回执本身——同一请求身份重试时回执相同，因此不会多建批次；
    回执已挂批次则直接返回，只有还没挂上时才补建。
    """
    if receipt.get("批次ID") is not None:
        existing = store.find("batch_queue", int(receipt["批次ID"]))
        if existing is not None:
            return existing
    batch_rows = store.rows("batch_queue")
    new_id = max((int(r.get("id", 0)) for r in batch_rows), default=0) + 1
    plan_code = receipt.get("来源计划") or (plan or {}).get("计划编号", "")
    batch = {
        "id": new_id,
        "批次编号": f"BATC-AUTO-{new_id:04d}",
        "关联计划": plan_code,
        "任务数量": 10,
        "完成数": "0",
        "失败数": "0",
        "批次状态": "由调度自动生成",
        "截止时间": persist.now_iso(),
        "优先级": "中",
        "status": "排队中",
        "pending": True,
        "abnormal": False,
        "version": "",
        "updated_at": persist.now_iso(),
    }
    # 来源关系统一来自回执：脚本、计划与本次启动。
    batch.update(dispatch.batch_link_fields(receipt))
    batch_rows.append(batch)
    dispatch.link_batch(receipt, batch)
    persist.append_jsonl("runs", "batch_queue.jsonl", {
        "module": "batch_queue", "entity": "采集批次", "entry_id": new_id,
        "status": "排队中", "version": "", "action": "auto_create_from_schedule",
        "timestamp": persist.now_iso(), "abnormal": False,
        "source_module": "script_schedule", "source_id": receipt.get("来源编号"),
        "调度回执": receipt.get("回执编号", ""),
        "来源脚本": receipt.get("来源脚本", ""),
        "来源计划": plan_code,
    })
    return batch


def _as_int(value: Any) -> int | None:
    """把统计值解析成整数；非数字（样例文本/空值）返回 None，不臆造结果。"""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _settlement_value(context: dict[str, Any], *keys: str) -> int | None:
    """从动作上下文里取结算统计，兼容直接键与 values 子字典。"""
    pool = dict(context)
    values = context.get("values")
    if isinstance(values, dict):
        pool.update(values)
    for key in keys:
        parsed = _as_int(pool.get(key))
        if parsed is not None:
            return parsed
    return None


def _complete_batch_to_history(entry: dict[str, Any], context: dict[str, Any]) -> None:
    """批次确认完成时写入run_history。

    - 原统计字段沿用批次：采集行数=任务数量、失败行数=失败数；
    - 实际完成数/失败数与来源由关联的调度回执补足；
    - 来源以回执为准，后续脚本/计划不会改写已结算批次。
    """
    completed = _settlement_value(context, "实际完成数", "完成数", "completed", "done")
    failed = _settlement_value(context, "失败数", "failed")
    if completed is None:
        completed = _as_int(entry.get("完成数"))
    if failed is None:
        failed = _as_int(entry.get("失败数"))

    receipt = dispatch.receipt_for_batch(entry)
    if completed is not None:
        entry["完成数"] = str(completed)
    if failed is not None:
        entry["失败数"] = str(failed)
    if receipt is not None:
        dispatch.record_batch_settlement(
            entry,
            completed=completed if completed is not None else "",
            failed=failed if failed is not None else "",
        )

    run_rows = store.rows("run_history")
    new_id = max((int(r.get("id", 0)) for r in run_rows), default=0) + 1
    record = {
        "id": new_id,
        "记录编号": f"RUN-BATCH-{new_id:04d}",
        "关联计划": entry.get("关联计划", ""),
        "开始时间": persist.now_iso(),
        "结束时间": persist.now_iso(),
        "采集行数": str(entry.get("任务数量", 0)),
        "失败行数": str(entry.get("失败数", 0)),
        "执行日志": f"批次{entry.get('批次编号')}执行完成",
        "异常摘要": "",
        "status": "已完成",
        "pending": False,
        "abnormal": False,
        "version": "",
        "updated_at": persist.now_iso(),
    }
    # 有来源回执时补足实际完成数、失败数与来源；旧记录保持原字段可读。
    if receipt is not None:
        record["实际完成数"] = "" if completed is None else str(completed)
        record["失败数"] = "" if failed is None else str(failed)
        record.update(dispatch.source_link(receipt))
        record["批次编号"] = entry.get("批次编号", "")
    run_rows.append(record)
    persist.append_jsonl("runs", "run_history.jsonl", {
        "module": "run_history", "entity": "运行记录", "entry_id": new_id,
        "status": "已完成", "version": "", "action": "auto_create_from_batch",
        "timestamp": persist.now_iso(), "abnormal": False,
        "source_module": "batch_queue", "source_id": entry.get("id"),
        **({"调度回执": receipt.get("回执编号", "")} if receipt is not None else {}),
    })

register_hook("batch_queue", "确认完成", _complete_batch_to_history)


class BatchQueueService(BaseModuleService):
    MODULE = "batch_queue"
    ENTITY = "采集批次"
    OPERATION_TERM = "调度"
    LIST_FIELDS = ["批次编号", "关联计划", "任务数量", "完成数", "失败数", "批次状态", "截止时间", "优先级"]
    REQUIRED_FIELDS = ["批次编号", "关联计划", "任务数量"]
    STATUS_ORDER = ["排队中", "执行中", "已完成", "已超时"]
    ACTION_RULES = {"发起执行": "执行中", "确认完成": "已完成", "标记超时": "已超时"}
    NEGATIVE_ACTIONS = []

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        batches = store.rows(self.MODULE)
        total_tasks = sum(int(r.get("任务数量", 0)) for r in batches)
        base["total_tasks"] = total_tasks
        # 关联的run_history
        runs = persist.read_jsonl("runs", "run_history.jsonl")
        batch_runs = [r for r in runs if "batch" in str(r.get("action", ""))]
        base["batch_runs"] = len(batch_runs)
        # 关联的error_recover
        errors = store.rows("error_recover")
        batch_plans = {r.get("关联计划") for r in batches}
        base["recovery_linked"] = sum(1 for e in errors if e.get("关联计划") in batch_plans)
        return base
