"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    action: str
    remark: str | None = None
    # 启动/结算等动作的请求身份：同一次提交重试时携带同一个值，
    # 用于识别重复确认；不同身份视为另一次有意启动。
    request_id: str | None = None
    # 批次结算的实际完成数/失败数，由调用方随动作提交。
    values: dict[str, Any] = Field(default_factory=dict)
