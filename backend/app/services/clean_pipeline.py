"""清洗管道领域服务：数据清洗编排，消费data_extract输出，驱动transform_rule和quality_guard。"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.services.base import BaseModuleService, register_hook
from app import persistence as persist


def _trigger_transform_on_enable(entry: dict[str, Any], _context: dict[str, Any]) -> None:
    """清洗工序启用时自动检查关联转换规则是否就绪。"""
    pipeline_id = entry.get("工序编号", "")
    transforms = [t for t in store.rows("transform_rule") if t.get("关联工序") == pipeline_id]
    if not transforms:
        # 无关联转换规则，标记为异常
        return
    # 记录关联触发
    persist.append_jsonl("reports", "pipeline_transform_check.jsonl", {
        "pipeline_id": entry.get("id"),
        "pipeline_code": pipeline_id,
        "linked_transforms": len(transforms),
        "transform_ready": sum(1 for t in transforms if t.get("status") == "已生效"),
        "timestamp": persist.now_iso(),
    })

register_hook("clean_pipeline", "启用工序", _trigger_transform_on_enable)


class CleanPipelineService(BaseModuleService):
    MODULE = "clean_pipeline"
    ENTITY = "清洗工序"
    OPERATION_TERM = "清洗"
    LIST_FIELDS = ["工序编号", "输入源", "清洗规则", "处理顺序", "脏数据策略", "异常阈值", "采样比例", "工序版本"]
    REQUIRED_FIELDS = ["工序编号", "输入源", "清洗规则"]
    STATUS_ORDER = ["草稿", "待校验", "已启用", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "启用工序": "已启用", "停用工序": "已停用"}
    NEGATIVE_ACTIONS = ["停用工序"]

    def get_linked_transforms(self, pipeline_code: str) -> list[dict[str, Any]]:
        """查询某工序关联的所有转换规则。"""
        return [t for t in store.rows("transform_rule") if t.get("关联工序") == pipeline_code]

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        pipelines = store.rows(self.MODULE)
        # 读取关联的质量报告
        reports = persist.read_jsonl("reports", "quality_report.jsonl")
        base["quality_checks"] = len(reports)
        # 关联的归一化规则
        norms = store.rows("field_normalize")
        base["linked_normalize_rules"] = len(norms)
        return base