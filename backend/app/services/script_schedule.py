"""脚本调度领域服务：消费behavior_script脚本，创建调度计划、驱动batch_queue执行。"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.services.base import BaseModuleService, register_hook
from app.services import dispatch
from app import persistence as persist


def _resolve_script(plan: dict[str, Any]) -> dict[str, Any] | None:
    """按计划上的关联脚本回查行为脚本，用于把脚本ID交给来源回执。"""
    script_code = plan.get("关联脚本", "")
    for script in store.rows("behavior_script"):
        if script.get("脚本编号") == script_code:
            return script
    return None


def _create_batch_on_start(entry: dict[str, Any], context: dict[str, Any]) -> None:
    """调度计划启动运行时，交给batch_queue消费本次启动的调度回执并创建批次。

    回执先行：同一次启动确认（计划 + 请求身份）反复确认只对应一个批次；
    回执写成功而批次尚未创建时，重试会复用回执并补建批次。
    未提供请求身份时按原有行为每次启动生成一次性身份。
    """
    # 延迟导入：批次的消费逻辑归 batch_queue 所有，避免模块级循环依赖。
    from app.services.batch_queue import ensure_batch_for_receipt

    script = _resolve_script(entry)
    with dispatch.RECEIPT_LOCK:
        receipt = dispatch.write_start_receipt(
            plan=entry,
            script_id=script.get("id") if script else None,
            script_code=entry.get("关联脚本", ""),
            request_identity=context.get("identity"),
        )
        ensure_batch_for_receipt(receipt, plan=entry, script=script)

register_hook("script_schedule", "启动运行", _create_batch_on_start)


class ScriptScheduleService(BaseModuleService):
    MODULE = "script_schedule"
    ENTITY = "调度计划"
    OPERATION_TERM = "调度"
    LIST_FIELDS = ["计划编号", "关联脚本", "触发条件", "执行间隔", "重跑策略", "依赖任务", "超时控制", "计划状态"]
    REQUIRED_FIELDS = ["计划编号", "关联脚本", "触发条件"]
    STATUS_ORDER = ["草稿", "待校验", "运行中", "已暂停"]
    ACTION_RULES = {"提交校验": "待校验", "启动运行": "运行中", "暂停调度": "已暂停"}
    NEGATIVE_ACTIONS = []

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        # 关联的batch_queue统计
        batches = store.rows("batch_queue")
        schedules = store.rows(self.MODULE)
        plan_ids = {r.get("计划编号") for r in schedules}
        base["downstream_batches"] = sum(1 for b in batches if b.get("关联计划") in plan_ids)
        # 关联的error_recover任务
        errors = store.rows("error_recover")
        base["recovery_tasks"] = sum(1 for e in errors if e.get("关联计划") in plan_ids)
        return base
