"""模块服务基类：共享查询、登记、动作和统计骨架，含跨模块数据流钩子。"""
from __future__ import annotations

import hashlib
from typing import Any, Callable

from app.store import store
from app import persistence as persist


def _version_id(entry: dict[str, Any]) -> str:
    """生成条目版本指纹，用于跨模块版本追踪。"""
    raw = json.dumps({k: str(v) for k, v in sorted(entry.items()) if k != "id"}, default=str)
    return hashlib.md5(raw.encode()).hexdigest()[:12]


# 跨模块副作用注册表：key 为 (module, action)，value 为回调函数列表
_CROSS_MODULE_HOOKS: dict[tuple[str, str], list[Callable[[dict[str, Any]], None]]] = {}


def register_hook(
    module: str,
    action: str,
    callback: Callable[[dict[str, Any], dict[str, Any]], None],
) -> None:
    """注册跨模块钩子：当某个模块执行某个动作时，自动触发回调。

    回调签名 (entry, context)：context 携带动作请求的附加交接信息
    （如启动身份），没有时为空字典。
    """
    key = (module, action)
    if key not in _CROSS_MODULE_HOOKS:
        _CROSS_MODULE_HOOKS[key] = []
    _CROSS_MODULE_HOOKS[key].append(callback)


import json


class BaseModuleService:
    MODULE = ""
    ENTITY = ""
    OPERATION_TERM = "采集"
    LIST_FIELDS: list[str] = []
    REQUIRED_FIELDS: list[str] = []
    STATUS_ORDER: list[str] = []
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS: list[str] = []

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(store.rows(self.MODULE))
        for field, expected in (filters or {}).items():
            if field in self.LIST_FIELDS:
                rows = [
                    row for row in rows
                    if expected.casefold() in str(row.get(field, "")).casefold()
                ]
        if keyword and self.LIST_FIELDS:
            field = self.LIST_FIELDS[0]
            rows = [row for row in rows if keyword.casefold() in str(row.get(field, "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(self.MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing
        rows = store.rows(self.MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in self.LIST_FIELDS})
        entry["status"] = self.STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        entry["version"] = _version_id(entry)
        entry["updated_at"] = persist.now_iso()
        rows.append(entry)

        # 文件持久化：写入JSONL 运行记录（供run_history等消费）
        self._persist_to_jsonl(entry, action="create")
        return entry, []

    def run_action(
        self,
        entry_id: int,
        action: str,
        *,
        context: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"

        previous_status = entry.get("status")
        entry["previous_status"] = previous_status
        entry["status"] = target
        entry["pending"] = target != self.STATUS_ORDER[-1]
        entry["abnormal"] = action in self.NEGATIVE_ACTIONS
        entry["version"] = _version_id(entry)
        entry["updated_at"] = persist.now_iso()
        entry["last_action"] = action

        # 文件持久化
        self._persist_to_jsonl(entry, action=action)

        # 跨模块副作用
        self._fire_cross_module_hooks(entry, action, context or {})

        return entry, f"{self.ENTITY}已{action}"

    def _persist_to_jsonl(self, entry: dict[str, Any], action: str) -> None:
        """将状态变更写入 runs/ 下的JSONL，供run_history等模块消费。"""
        record = {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "entry_id": entry.get("id"),
            "status": entry.get("status"),
            "version": entry.get("version", ""),
            "action": action,
            "timestamp": persist.now_iso(),
            "abnormal": entry.get("abnormal", False),
        }
        persist.append_jsonl("runs", f"{self.MODULE}.jsonl", record)

    def _fire_cross_module_hooks(
        self,
        entry: dict[str, Any],
        action: str,
        context: dict[str, Any] | None = None,
    ) -> None:
        """触发已注册的跨模块钩子；context 承载动作请求里的启动身份等交接信息。"""
        key = (self.MODULE, action)
        for hook in _CROSS_MODULE_HOOKS.get(key, []):
            hook(entry, context or {})

    def domain_snapshot(self) -> dict[str, Any]:
        """输出模块级数据质量快照，供跨模块消费和题位源码锚点使用。"""
        rows = store.rows(self.MODULE)
        by_status = {
            status: sum(1 for row in rows if row.get("status") == status)
            for status in self.STATUS_ORDER
        }
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for row in rows if row.get("abnormal")),
        }