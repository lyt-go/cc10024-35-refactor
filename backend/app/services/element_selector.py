"""元素锚点领域服务：Web元素定位、选择器校验，消费behavior_script的脚本配置，为data_extract提供定位锚点。"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.services.base import BaseModuleService, register_hook
from app import persistence as persist


def _validate_selector_on_active(entry: dict[str, Any], _context: dict[str, Any]) -> None:
    """元素锚点标记生效时，校验关联脚本是否存在且已生效。"""
    script_code = entry.get("关联脚本", "")
    scripts = store.rows("behavior_script")
    linked = [s for s in scripts if s.get("脚本编号") == script_code]
    status_ok = any(s.get("status") == "已生效" for s in linked)
    if not status_ok and linked:
        # 标记锚点为异常，关联脚本未生效
        entry["abnormal"] = True

    # 写入快照目录作为定位产物
    payload = {
        "selector_id": entry.get("id"),
        "selector_type": entry.get("选择器类型"),
        "css_expr": entry.get("CSS表达式"),
        "xpath_expr": entry.get("XPath表达式"),
        "text_match": entry.get("文本匹配"),
        "linked_script": script_code,
        "script_active": status_ok,
        "timestamp": persist.now_iso(),
        "version": entry.get("version", ""),
    }
    persist.write_json("snapshots", f"selector_{entry.get('id')}.json", payload)

register_hook("element_selector", "标记生效", _validate_selector_on_active)


class ElementSelectorService(BaseModuleService):
    MODULE = "element_selector"
    ENTITY = "元素锚点"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["锚点编号", "关联脚本", "选择器类型", "CSS表达式", "XPath表达式", "文本匹配", "等待条件", "元素顺序"]
    REQUIRED_FIELDS = ["锚点编号", "关联脚本", "选择器类型"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已失效"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "标记失效": "已失效"}
    NEGATIVE_ACTIONS = []

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        selectors = store.rows(self.MODULE)
        scripts = store.rows("behavior_script")
        script_ids = {r.get("脚本编号") for r in scripts}
        base["orphan_selectors"] = sum(
            1 for s in selectors if s.get("关联脚本") not in script_ids)
        # 读取selector快照文件
        snap_files = persist.list_files("snapshots", ".json")
        base["selector_snapshots"] = len([f for f in snap_files if f.startswith("selector_")])
        return base