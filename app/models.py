"""API 请求 / 响应模型。"""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field

JobStatus = Literal[
    "pending",      # 已受理，等待运行名额
    "running",      # 运行中
    "cancelling",   # 已收到取消请求，正在中止本轮 SQL
    "succeeded",    # 终态：核对完成（可能一致，也可能有差异）
    "failed",       # 终态：任一侧失败
    "cancelled",    # 终态：用户取消
    "timeout",      # 终态：超过单轮时限
]

TERMINAL_STATUSES = {"succeeded", "failed", "cancelled", "timeout"}
ACTIVE_STATUSES = {"pending", "running", "cancelling"}


class CreateJobRequest(BaseModel):
    left_sql: str = Field(..., min_length=1, description="左侧只读 SELECT（如改写后的日结 SQL）")
    right_sql: str = Field(..., min_length=1, description="右侧只读 SELECT（基准 SQL）")
    name: str | None = Field(None, max_length=128, description="可选备注名")


class DiffRow(BaseModel):
    # 与结果集列顺序对齐；NULL 用 null 表示，空串为 ""，二者不同
    values: list[Any]
    left_count: int
    right_count: int


class ReconcileResult(BaseModel):
    identical: bool
    columns: list[str]
    column_types: list[str]          # bigint | text
    left_row_total: int
    right_row_total: int
    matched_group_count: int         # 行内容与次数都相同的去重行数
    diff_group_total: int            # 存在差异的去重行种类总数（不受 20 条展示上限影响）
    diffs_shown: int
    diffs: list[DiffRow]             # 最多 20 种


class JobOut(BaseModel):
    job_id: str
    name: str | None
    status: JobStatus
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    deadline: datetime | None
    cancel_requested: bool
    error: str | None
    result: ReconcileResult | None

    @staticmethod
    def utcnow() -> datetime:
        return datetime.now(timezone.utc)


# ============================================================ 日结读数核查


AuditStatus = Literal[
    "pending",     # 已受理，等待运行名额
    "running",     # 正在扫描
    "cancelling",  # 已收到取消请求，正在中止扫描
    "succeeded",   # 已发布结论（usable 可能是 false）
    "failed",      # 扫描/保存失败，无结论
    "aborted",     # 扫描期间源表被更正或报告保存失败，无结论
    "cancelled",   # 用户取消
    "timeout",     # 超时
]

AUDIT_TERMINAL_STATUSES = {
    "succeeded",
    "failed",
    "aborted",
    "cancelled",
    "timeout",
}


class AnomalyOut(BaseModel):
    seq: int
    meter_id: int
    previous_time: datetime
    previous_wh: int
    current_time: datetime
    current_wh: int
    gap_seconds: int
    reasons: list[str]              # rollback | gap
    reason_text: str


class MeterWithoutReadings(BaseModel):
    meter_id: int
    line_name: str | None


class DailyAuditReportOut(BaseModel):
    report_id: int
    audit_date: date
    status: AuditStatus
    attempt: int
    usable: bool                    # 本次核查自身结论：异常为 0 且无缺读数
    usable_reason: str | None
    stale: bool                     # 源表在该日期上已有更晚的已提交改动
    stale_reason: str | None
    settlement_usable: bool         # 可用于结算：成功 + usable + 未过期 + 最近一次尝试成功
    latest_attempt_status: AuditStatus | None  # 该日期最新一次尝试的状态
    usable_blocked_reason: str | None          # 被最新失败/未完成尝试阻断的原因
    source_version: int | None
    current_version: int
    anomaly_total: int | None       # 全量异常数（不受展示上限影响）
    readings_count: int | None
    meters_with_readings: int | None
    meters_total: int | None
    meters_without_readings: list[MeterWithoutReadings]
    max_gap_minutes: int
    anomalies: list[AnomalyOut]     # 最多 MAX_ANOMALIES_SHOWN 条（默认 20）
    error: str | None
    started_at: datetime | None
    finished_at: datetime | None
    created_at: datetime


class DailyAuditJobOut(BaseModel):
    audit_id: str
    status: AuditStatus
    audit_date: date
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    error: str | None
    report: DailyAuditReportOut | None

    @staticmethod
    def utcnow() -> datetime:
        return datetime.now(timezone.utc)
