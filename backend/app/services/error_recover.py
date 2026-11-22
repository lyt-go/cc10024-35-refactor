"""错误恢复领域服务：采集失败后的重试与恢复，消费batch_queue失败信息，写入run_history。"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.services.base import BaseModuleService, register_hook
from app import persistence as persist


def _record_recovery_to_history(entry: dict[str, Any], _context: dict[str, Any]) -> None:
    """恢复任务确认恢复后写入run_history。"""
    run_rows = store.rows("run_history")
    new_id = max((int(r.get("id", 0)) for r in run_rows), default=0) + 1
    record = {
        "id": new_id,
        "记录编号": f"RUN-RECOVER-{new_id:04d}",
        "关联计划": entry.get("关联计划", ""),
        "开始时间": persist.now_iso(),
        "结束时间": "",
        "采集行数": "恢复重采",
        "失败行数": entry.get("已重试次数", "0"),
        "执行日志": f"从error_recover任务{entry.get('任务编号')}恢复",
        "异常摘要": entry.get("错误类型", ""),
        "status": "运行中",
        "pending": True,
        "abnormal": False,
        "version": "",
        "updated_at": persist.now_iso(),
    }
    run_rows.append(record)
    persist.append_jsonl("runs", "run_history.jsonl", {
        "module": "run_history", "entity": "运行记录", "entry_id": new_id,
        "status": "运行中", "version": "", "action": "auto_create_from_recovery",
        "timestamp": persist.now_iso(), "abnormal": False,
        "source_module": "error_recover", "source_id": entry.get("id"),
    })

register_hook("error_recover", "确认恢复", _record_recovery_to_history)


class ErrorRecoverService(BaseModuleService):
    MODULE = "error_recover"
    ENTITY = "恢复任务"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["任务编号", "关联计划", "失败步骤", "错误类型", "重试边界", "回滚策略", "已重试次数", "恢复状态"]
    REQUIRED_FIELDS = ["任务编号", "关联计划", "失败步骤"]
    STATUS_ORDER = ["待处理", "重试中", "已恢复", "已放弃"]
    ACTION_RULES = {"发起重试": "重试中", "确认恢复": "已恢复", "标记放弃": "已放弃"}
    NEGATIVE_ACTIONS = []

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        tasks = store.rows(self.MODULE)
        # 关联的batch_queue统计数据
        batches = store.rows("batch_queue")
        plan_ids = {r.get("关联计划") for r in tasks}
        base["linked_batches"] = sum(1 for b in batches if b.get("关联计划") in plan_ids)
        # 恢复关联的run_history
        runs = persist.read_jsonl("runs", "run_history.jsonl")
        recovery_runs = [r for r in runs if "recover" in str(r.get("action", ""))]
        base["recovery_runs"] = len(recovery_runs)
        base["retry_total"] = sum(
            int(str(r.get("已重试次数") or "0")) for r in tasks if str(r.get("已重试次数") or "").isdigit())
        return base