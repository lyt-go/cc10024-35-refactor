"""采集调度回执：脚本 → 计划 → 启动 → 批次 → 运行历史的统一来源交接。

重构前 behavior_script / script_schedule / batch_queue 各自拼一份来源字段：
脚本生效只写计划来源，计划启动只写批次来源，批次结算只写运行记录来源，
编号关系在各段重复维护且对不上。这里把「这条调度链是谁创建的」收拢成一份
采集调度回执（内部表 dispatch_receipt）：

- 脚本生效写一条「脚本生效」回执，固定脚本与计划的来源关系；
- 计划启动写一条「计划启动」回执，记录本次启动的请求身份；
- 批次队列消费启动回执创建批次，并把回执号与脚本/计划挂到批次上；
- 批次结算把实际完成数、失败数补进同一条回执，运行历史只读回执取来源。

身份约定：
- 一次启动确认由「计划编号 + 请求身份」唯一确定；
- 同一身份重试复用回执，不会为同一提交多算一个批次；
- 不同身份视为另一次有意启动，各自生成新回执与新批次；
- 未提供身份时按原有行为每次启动都视为新的提交。

旧记录兼容：没有回执的既有批次/运行记录仍按原有字段可读；结算时不会凭空
补写来源，也不会把已结算批次改挂到后来的脚本或计划上。
"""
from __future__ import annotations

import threading
import uuid
from typing import Any

from app.store import store
from app import persistence as persist

RECEIPT_TABLE = "dispatch_receipt"
RECEIPT_PLAN = "脚本生效"
RECEIPT_START = "计划启动"
STATUS_PLAN = "计划已生成"
STATUS_STARTED = "已启动"
STATUS_SETTLED = "已结算"
SOURCE_SCRIPT = "behavior_script"
SOURCE_SCHEDULE = "script_schedule"

# 启动确认的「查重 + 写回执」「查重 + 建批次」需要作为一个整体执行，
# 避免同一提交在并发确认时各写一份回执或各建一个批次。
RECEIPT_LOCK = threading.RLock()


def _now() -> str:
    return persist.now_iso()


def _rows() -> list[dict[str, Any]]:
    return store.internal_rows(RECEIPT_TABLE)


def _next_id() -> int:
    return max((int(row.get("id", 0)) for row in _rows()), default=0) + 1


def _find(**conditions: Any) -> dict[str, Any] | None:
    for receipt in _rows():
        if all(receipt.get(key) == value for key, value in conditions.items()):
            return receipt
    return None


def _save(receipt: dict[str, Any], action: str) -> dict[str, Any]:
    """回执先立即可见再落 JSONL，写入中断也不会丢失已生成的交接关系。"""
    _rows().append(receipt)
    return _append_state(receipt, action)


def _append_state(receipt: dict[str, Any], action: str) -> dict[str, Any]:
    """把回执当前状态追加到 JSONL；后续挂批次/结算以新事件行刷新。"""
    persist.append_jsonl("runs", "dispatch_receipt.jsonl", {
        "module": RECEIPT_TABLE,
        "entity": "采集调度回执",
        "entry_id": receipt.get("id"),
        "status": receipt.get("状态", ""),
        "version": receipt.get("version", ""),
        "action": action,
        "timestamp": _now(),
        "abnormal": False,
        **receipt,
    })
    return receipt


def normalize_identity(*candidates: Any) -> str | None:
    """从动作请求里取出请求身份；空串视为未提供。"""
    for candidate in candidates:
        if candidate is None:
            continue
        value = str(candidate).strip()
        if value:
            return value
    return None


def new_identity() -> str:
    """未显式提供身份时生成一次性身份，保持「每次启动都新建批次」的旧行为。"""
    return f"AUTO-{uuid.uuid4().hex[:12]}"


def _new_receipt(
    *,
    receipt_no: str,
    kind: str,
    status: str,
    source_module: str,
    source_id: int | None,
    script_id: int | None,
    script_code: str,
    plan_id: int | None,
    plan_code: str,
    request_identity: str,
    action: str,
) -> dict[str, Any]:
    timestamp = _now()
    return {
        "id": _next_id(),
        "回执编号": receipt_no,
        "回执类型": kind,
        "状态": status,
        "来源模块": source_module,
        "来源编号": source_id,
        "脚本ID": script_id,
        "来源脚本": script_code,
        "计划ID": plan_id,
        "来源计划": plan_code,
        "计划回执": "",
        "请求身份": request_identity,
        "批次ID": None,
        "批次编号": "",
        "实际完成数": "",
        "失败数": "",
        "动作": action,
        "created_at": timestamp,
        "updated_at": timestamp,
        "version": "",
    }


def write_plan_receipt(
    *,
    script_id: int | None,
    script_code: str,
    plan_id: int | None,
    plan_code: str,
) -> dict[str, Any]:
    """行为脚本生效：写入脚本与计划的来源关系。

    同一脚本重新生效会生成新计划，因此按计划编号各写一条回执，
    历史计划与脚本的关系不会被后一次生效覆盖。
    """
    with RECEIPT_LOCK:
        receipt_no = f"DSPT-PLAN-{plan_code}"
        existing = _find(回执编号=receipt_no)
        if existing is not None:
            return existing
        receipt = _new_receipt(
            receipt_no=receipt_no,
            kind=RECEIPT_PLAN,
            status=STATUS_PLAN,
            source_module=SOURCE_SCRIPT,
            source_id=script_id,
            script_id=script_id,
            script_code=script_code,
            plan_id=plan_id,
            plan_code=plan_code,
            request_identity="",
            action="标记生效",
        )
        return _save(receipt, "dispatch_receipt_plan")


def write_start_receipt(
    *,
    plan: dict[str, Any],
    script_id: int | None,
    script_code: str,
    request_identity: str | None,
) -> dict[str, Any]:
    """计划启动：按「计划 + 请求身份」生成或复用本次启动的调度回执。

    同一身份重试时返回原回执（此时可能还没挂上批次），由批次消费者补建；
    不同身份生成新回执，因此另一次有意启动仍会得到新批次。
    """
    identity = normalize_identity(request_identity) or new_identity()
    plan_code = plan.get("计划编号", "")
    with RECEIPT_LOCK:
        existing = _find(
            回执类型=RECEIPT_START,
            来源计划=plan_code,
            请求身份=identity,
        )
        if existing is not None:
            return existing
        receipt = _new_receipt(
            receipt_no=f"DSPT-START-{plan_code}-{identity}",
            kind=RECEIPT_START,
            status=STATUS_STARTED,
            source_module=SOURCE_SCHEDULE,
            source_id=plan.get("id"),
            script_id=script_id,
            script_code=script_code,
            plan_id=plan.get("id"),
            plan_code=plan_code,
            request_identity=identity,
            action="启动运行",
        )
        plan_receipt = _find(回执类型=RECEIPT_PLAN, 来源计划=plan_code)
        if plan_receipt is not None:
            receipt["计划回执"] = plan_receipt.get("回执编号", "")
        return _save(receipt, "dispatch_receipt_start")


def link_batch(receipt: dict[str, Any], batch: dict[str, Any]) -> dict[str, Any]:
    """批次队列消费回执后，把新建批次挂回本次启动回执。"""
    receipt["批次ID"] = batch.get("id")
    receipt["批次编号"] = batch.get("批次编号", "")
    receipt["状态"] = STATUS_STARTED
    receipt["updated_at"] = _now()
    return _append_state(receipt, "dispatch_receipt_batch_linked")


def receipt_for_batch(batch: dict[str, Any]) -> dict[str, Any] | None:
    """按批次找回执：优先批次ID，再按回执号，最后按批次编号兼容旧记录。"""
    batch_id = batch.get("id")
    if batch_id is not None:
        receipt = _find(批次ID=batch_id)
        if receipt is not None:
            return receipt
    receipt_no = batch.get("调度回执")
    if receipt_no:
        receipt = _find(回执编号=receipt_no)
        if receipt is not None:
            return receipt
    batch_code = batch.get("批次编号")
    if batch_code:
        return _find(批次编号=batch_code)
    return None


def receipt_for_plan(plan: dict[str, Any]) -> dict[str, Any] | None:
    """按计划找回执：优先计划上的回执号，旧记录按计划编号兜底。"""
    receipt_no = plan.get("调度回执")
    if receipt_no:
        receipt = _find(回执编号=receipt_no)
        if receipt is not None:
            return receipt
    plan_code = plan.get("计划编号")
    if plan_code:
        return _find(回执类型=RECEIPT_PLAN, 来源计划=plan_code)
    return None


def receipt_for_script(script: dict[str, Any]) -> dict[str, Any] | None:
    """按脚本找回执：优先脚本ID，旧记录按脚本编号兜底。"""
    script_id = script.get("id")
    if script_id is not None:
        receipt = _find(回执类型=RECEIPT_PLAN, 脚本ID=script_id)
        if receipt is not None:
            return receipt
    script_code = script.get("脚本编号")
    if script_code:
        return _find(回执类型=RECEIPT_PLAN, 来源脚本=script_code)
    return None


def record_batch_settlement(
    batch: dict[str, Any],
    *,
    completed: Any,
    failed: Any,
) -> dict[str, Any] | None:
    """批次结算：实际完成数/失败数写进关联回执，来源保持首次挂接的值。"""
    receipt = receipt_for_batch(batch)
    if receipt is None:
        return None
    receipt["实际完成数"] = "" if completed is None else str(completed)
    receipt["失败数"] = "" if failed is None else str(failed)
    receipt["状态"] = STATUS_SETTLED
    receipt["updated_at"] = _now()
    return _append_state(receipt, "dispatch_receipt_settled")


def source_link(receipt: dict[str, Any] | None) -> dict[str, Any]:
    """运行记录/批次可用的来源关联字段；无回执时返回空字典。"""
    if receipt is None:
        return {}
    return {
        "调度回执": receipt.get("回执编号", ""),
        "来源脚本": receipt.get("来源脚本", ""),
        "来源计划": receipt.get("来源计划", ""),
    }


def batch_link_fields(receipt: dict[str, Any]) -> dict[str, Any]:
    """自动批次创建时写入的来源字段：脚本、计划、本次启动回执与身份。"""
    return {
        **source_link(receipt),
        "启动标识": receipt.get("请求身份", ""),
    }
