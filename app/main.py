"""FastAPI 应用入口与 HTTP 路由。"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from . import db as dbmod
from .daily_audit import store as daily_store
from .daily_manager import AuditSlotBusyError, daily_manager
from .manager import SlotBusyError, manager
from .models import (
    CreateJobRequest,
    DailyAuditJobOut,
    DailyAuditReportOut,
    JobOut,
)
from .sqlvalidation import SqlValidationError


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # 建报告结构（幂等）；数据库尚未就绪时不阻塞服务启动，
    # 首次使用核查接口时会再次尝试并给出明确错误。
    try:
        daily_store.ensure_schema()
    except Exception as exc:  # noqa: BLE001
        print(f"[startup] 初始化日核查报告结构失败（稍后重试）：{exc}")
    yield


app = FastAPI(
    title="电表日结 SQL 核对 API",
    description=(
        "提交两条只读 SELECT，基于同一已提交快照并行读取，"
        "按行内容与出现次数做有序多重集核对（忽略行序，NULL≠空串），"
        "差异最多展示 20 种。单轮 30 秒，全局同时只允许一个任务。\n\n"
        "另提供按 UTC 日期核查 readings 的日结可用性接口："
        "逐表比较当天相邻累计读数，回退或间隔超过两小时记为异常；"
        "报告持久化到 PostgreSQL，源表发生任何已提交改动后旧报告显示过期。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)


@app.get("/health", tags=["运维"])
def health() -> dict:
    """存活探针；不触碰数据库。"""
    return {"status": "ok"}


@app.get("/jobs", response_model=list[JobOut], tags=["核对任务"])
def list_jobs() -> list[JobOut]:
    """列出近期任务（含终态），状态查询始终可用。"""
    return manager.list_jobs()


@app.post("/jobs", response_model=JobOut, status_code=201, tags=["核对任务"])
def create_job(payload: CreateJobRequest) -> JobOut:
    try:
        job = manager.create_job(payload.left_sql, payload.right_sql, payload.name)
    except SqlValidationError as exc:
        raise HTTPException(status_code=400, detail=f"SQL 校验失败：{exc}") from exc
    except SlotBusyError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "当前已有核对任务在运行，一次只允许一组",
                "active_job_id": exc.active_job_id,
            },
        ) from exc
    return job.snapshot()


@app.get("/jobs/{job_id}", response_model=JobOut, tags=["核对任务"])
def get_job(job_id: str) -> JobOut:
    job = manager.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    return job.snapshot()


@app.post("/jobs/{job_id}/cancel", response_model=JobOut, tags=["核对任务"])
def cancel_job(job_id: str) -> JobOut:
    try:
        job = manager.request_cancel(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="任务不存在") from exc
    return job.snapshot()


# ============================================================ 日结读数核查


@app.post(
    "/daily-audits",
    response_model=DailyAuditJobOut,
    status_code=201,
    tags=["日结读数核查"],
)
def create_daily_audit(
    audit_date: date = Query(
        ..., alias="date", description="要核查的 UTC 日期，格式 YYYY-MM-DD"
    ),
) -> DailyAuditJobOut:
    """核查某 UTC 日期的 readings 是否可用于日结结算。

    逐表按 read_time 比较当天相邻累计 Wh：后一条小于前一条（回退）
    或相邻间隔超过 2 小时记为异常；当天首条不接前一天；当天无读数
    的表不能判定为可用。报告保存到 PostgreSQL，可用 `GET /daily-audits`
    按日期查询。
    """
    try:
        job = daily_manager.create(audit_date)
    except AuditSlotBusyError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "当前已有日核查任务在运行，一次只允许一组",
                "active_audit_id": exc.active_audit_id,
            },
        ) from exc
    except Exception as exc:  # 例如数据库尚未就绪
        raise HTTPException(
            status_code=503, detail=f"无法受理日核查：{exc}"
        ) from exc
    return job.snapshot()


@app.get(
    "/daily-audits/{audit_id}",
    response_model=DailyAuditJobOut,
    tags=["日结读数核查"],
)
def get_daily_audit(audit_id: str) -> DailyAuditJobOut:
    job = daily_manager.get(audit_id)
    if job is None:
        raise HTTPException(status_code=404, detail="核查任务不存在")
    return job.snapshot()


@app.post(
    "/daily-audits/{audit_id}/cancel",
    response_model=DailyAuditJobOut,
    tags=["日结读数核查"],
)
def cancel_daily_audit(audit_id: str) -> DailyAuditJobOut:
    """请求取消；已进入终态则原样返回。取消不会发布核查结论。"""
    try:
        job = daily_manager.request_cancel(audit_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="核查任务不存在") from exc
    return job.snapshot()


@app.get(
    "/daily-reports",
    response_model=DailyAuditReportOut,
    tags=["日结读数核查"],
)
def get_daily_report(
    audit_date: date = Query(
        ..., alias="date", description="UTC 日期 YYYY-MM-DD"
    ),
) -> DailyAuditReportOut:
    """按日期查询最近一次**成功**的核查报告。

    返回的 `stale=true` 表示源表在该日期上已有更晚的已提交改动；
    `settlement_usable=false` 表示该报告不能直接用于结算
    （过期、未通过核查，或该日期最新一次尝试失败/中止）。
    需要重新发起核查。若该日期从未成功核查过则 404。
    """
    try:
        report = daily_store.load_latest_succeeded(audit_date)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"读取报告失败：{exc}") from exc
    if report is None:
        raise HTTPException(status_code=404, detail="该日期尚无成功核查报告")
    return report


@app.get(
    "/daily-reports/latest",
    response_model=DailyAuditReportOut,
    tags=["日结读数核查"],
)
def get_daily_report_latest(
    audit_date: date = Query(..., alias="date", description="UTC 日期 YYYY-MM-DD"),
) -> DailyAuditReportOut:
    """按日期查询**最新一次尝试**（含 running / failed / aborted），用于观察状态。"""
    try:
        report = daily_store.load_latest(audit_date)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"读取报告失败：{exc}") from exc
    if report is None:
        raise HTTPException(status_code=404, detail="该日期尚无核查记录")
    return report


@app.get(
    "/daily-reports/history",
    response_model=list[DailyAuditReportOut],
    tags=["日结读数核查"],
)
def get_daily_report_history(
    audit_date: date = Query(..., alias="date", description="UTC 日期 YYYY-MM-DD"),
    limit: int = Query(20, ge=1, le=200),
) -> list[DailyAuditReportOut]:
    """按日期查看历次核查尝试（新的在前）。"""
    try:
        return daily_store.load_history(audit_date, limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"读取报告失败：{exc}") from exc


@app.get(
    "/daily-reports/{report_id}",
    response_model=DailyAuditReportOut,
    tags=["日结读数核查"],
)
def get_daily_report_by_id(report_id: int) -> DailyAuditReportOut:
    """按报告 ID 查询（历史报告仍可查看，过期状态实时计算）。"""
    try:
        report = daily_store.load_report(report_id)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"读取报告失败：{exc}") from exc
    if report is None:
        raise HTTPException(status_code=404, detail="报告不存在")
    return report


@app.exception_handler(dbmod.DbError)
def _db_error_handler(_request, exc: dbmod.DbError) -> JSONResponse:
    return JSONResponse(status_code=422, detail={"message": str(exc)})
