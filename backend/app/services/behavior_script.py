"""行为脚本领域服务：脚本录制、校验、版本追踪，作为数据采集链起点驱动下游调度。"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.services.base import BaseModuleService, register_hook
from app.services import dispatch
from app import persistence as persist


def _auto_schedule_on_active(entry: dict[str, Any], _context: dict[str, Any]) -> None:
    """行为脚本标记生效后，自动在script_schedule创建默认调度计划。

    脚本与计划的关系通过采集调度回执交接，计划上同时保留回执号；
    同一脚本重新生效会生成新计划与新回执，旧计划的关系不受影响。
    先落回执再落计划：即便随后的计划写入中断，重试也不会丢来源。
    """
    schedule_rows = store.rows("script_schedule")
    new_id = max((int(r.get("id", 0)) for r in schedule_rows), default=0) + 1
    script_code = entry.get("脚本编号", f"BEHA-{entry.get('id')}")
    plan_code = f"SCRI-AUTO-{new_id:04d}"
    receipt = dispatch.write_plan_receipt(
        script_id=entry.get("id"),
        script_code=script_code,
        plan_id=new_id,
        plan_code=plan_code,
    )
    schedule = {
        "id": new_id,
        "计划编号": plan_code,
        "关联脚本": script_code,
        "触发条件": "启动后即触发",
        "执行间隔": "3600s",
        "重跑策略": "失败重试3次",
        "依赖任务": "",
        "超时控制": "1800s",
        "计划状态": "由行为脚本自动生成",
        "调度回执": receipt.get("回执编号", ""),
        "status": "草稿",
        "pending": True,
        "abnormal": False,
        "version": "",
        "updated_at": persist.now_iso(),
    }
    schedule_rows.append(schedule)
    persist.append_jsonl("runs", "script_schedule.jsonl", {
        "module": "script_schedule", "entity": "调度计划", "entry_id": new_id,
        "status": "草稿", "version": "", "action": "auto_create_from_behavior_script",
        "timestamp": persist.now_iso(), "abnormal": False,
        "source_module": "behavior_script", "source_id": entry.get("id"),
        "调度回执": receipt.get("回执编号", ""),
        "来源脚本": script_code, "来源计划": plan_code,
    })

# 注册跨模块钩子：behavior_script标记生效 → script_schedule自动创建
register_hook("behavior_script", "标记生效", _auto_schedule_on_active)


class BehaviorScriptService(BaseModuleService):
    MODULE = "behavior_script"
    ENTITY = "行为脚本"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["脚本编号", "目标站点", "录制步骤", "点击元素", "输入参数", "等待策略", "超时阈值", "脚本版本"]
    REQUIRED_FIELDS = ["脚本编号", "目标站点", "录制步骤"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已归档"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "标记归档": "已归档"}
    NEGATIVE_ACTIONS = []

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        # 读取关联的调度计划统计数据
        schedules = store.rows("script_schedule")
        linked_schedules = [r for r in schedules if r.get("关联脚本") in [
            e.get("脚本编号") for e in store.rows(self.MODULE)]]
        base["downstream_schedules"] = len(linked_schedules)
        base["schedules_by_status"] = {
            s: sum(1 for r in linked_schedules if r.get("status") == s)
            for s in ["草稿", "待校验", "运行中", "已暂停"]
        }
        # 读取运行历史数据
        runs = persist.read_jsonl("runs", "run_history.jsonl")
        base["total_runs"] = len(runs)
        base["failed_runs"] = sum(1 for r in runs if r.get("abnormal"))
        return base
