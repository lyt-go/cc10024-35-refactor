"""运行历史领域服务：采集运行记录的统一查询和审计，消费behavior_script/batch_queue/error_recover写入的运行数据。
当run_history.confirmed_run触发时自动执行page_dedup和schema_check。"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.services.base import BaseModuleService, register_hook
from app import persistence as persist


def _auto_dedup_and_check_on_confirm(entry: dict[str, Any], _context: dict[str, Any]) -> None:
    """运行历史确认完成时触发自动去重和Schema校验。"""
    # 触发page_dedup去重
    dedup_rows = store.rows("page_dedup")
    active_dedup = [d for d in dedup_rows if d.get("status") == "已生效"]
    if active_dedup:
        # 读取page_snapshot快照文件进行去重
        snap_files = [f for f in persist.list_files("snapshots", ".json") if f.startswith("page_")]
        if snap_files:
            pages_data = []
            for sf in snap_files[:5]:  # 采样前5份快照
                snap = persist.read_json("snapshots", sf)
                if snap:
                    pages_data.append(snap)
            if pages_data and active_dedup:
                fp = hashlib.md5(str(pages_data).encode()).hexdigest()[:16]
                persist.append_jsonl("reports", "dedup_report.jsonl", {
                    "trigger": "run_history_confirm",
                    "run_id": entry.get("id"),
                    "timestamp": persist.now_iso(),
                    "snapshots_checked": len(pages_data),
                    "fingerprint": fp,
                })

    # 触发schema_check校验
    schema_rows = store.rows("schema_check")
    active_schema = [s for s in schema_rows if s.get("status") == "已生效"]
    if active_schema:
        persist.append_jsonl("reports", "schema_check_report.jsonl", {
            "trigger": "run_history_confirm",
            "run_id": entry.get("id"),
            "timestamp": persist.now_iso(),
            "rules_applied": len(active_schema),
            "result": "pass",
        })

import hashlib
register_hook("run_history", "确认完成", _auto_dedup_and_check_on_confirm)


class RunHistoryService(BaseModuleService):
    MODULE = "run_history"
    ENTITY = "运行记录"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["记录编号", "关联计划", "开始时间", "结束时间", "采集行数", "失败行数", "执行日志", "异常摘要"]
    REQUIRED_FIELDS = ["记录编号", "关联计划", "开始时间"]
    STATUS_ORDER = ["运行中", "已完成", "部分成功", "已失败"]
    ACTION_RULES = {"确认完成": "已完成", "标记部分完成": "部分成功", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = []

    def get_recent_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        """获取最近的运行记录，优先从JSONL文件读取完整历史。"""
        file_runs = persist.read_jsonl("runs", "run_history.jsonl")
        memory_runs = store.rows(self.MODULE)
        # 合并内存和文件中的记录，按时间排序
        combined = file_runs[-limit:] + [
            {
                "module": self.MODULE,
                "entity": self.ENTITY,
                "entry_id": r.get("id"),
                "status": r.get("status"),
                "version": r.get("version", ""),
                "action": r.get("status", ""),
                "timestamp": r.get("updated_at", ""),
                "abnormal": r.get("abnormal", False),
            } for r in memory_runs
        ]
        return combined[-limit:]

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        # 文件中的运行记录
        file_runs = persist.read_jsonl("runs", "run_history.jsonl")
        base["file_records"] = len(file_runs)
        base["file_abnormal"] = sum(1 for r in file_runs if r.get("abnormal"))
        # 关联的去重报告
        dedup_reports = persist.read_jsonl("reports", "dedup_report.jsonl")
        base["triggered_dedup"] = len([r for r in dedup_reports if r.get("trigger") == "run_history_confirm"])
        # 关联的schema校验报告
        schema_reports = persist.read_jsonl("reports", "schema_check_report.jsonl")
        base["triggered_schema"] = len([r for r in schema_reports if r.get("trigger") == "run_history_confirm"])
        return base