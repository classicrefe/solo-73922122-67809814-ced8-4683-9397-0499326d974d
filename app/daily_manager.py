"""按 UTC 日期核查 readings：任务生命周期编排。

并发/终止语义与 SQL 核对任务保持一致：
- 进程内单名额，已有任务运行时返回 409；
- 看门狗按 JOB_TIMEOUT_SECONDS 判定超时；取消/超时都会请求
  中止扫描连接上的 SQL（必要时 terminate 兜底）；
- 扫描在 REPEATABLE READ READ ONLY 快照事务中进行；
- 报告先以 running 落库，只有版本复核通过且全部写入成功才
  在同一事务里改成 succeeded。扫描期间源表被更正 -> aborted；
  报告保存失败 -> failed/aborted。两种情况下按日期查询都拿不到
  新的可用结论，旧报告则会因版本变化显示 stale=true。
"""
from __future__ import annotations

import threading
import time
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

from . import db as dbmod
from .config import settings
from .daily_audit import (
    AuditAborted,
    AuditCancelled,
    AuditError,
    AuditTimeout,
    store,
)
from .models import (
    AUDIT_TERMINAL_STATUSES,
    AuditStatus,
    DailyAuditJobOut,
    DailyAuditReportOut,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class AuditSlotBusyError(RuntimeError):
    def __init__(self, active_audit_id: str):
        super().__init__(f"已有日核查任务在运行：audit_id={active_audit_id}")
        self.active_audit_id = active_audit_id


@dataclass
class DailyAuditJob:
    audit_id: str
    audit_date: date
    status: AuditStatus = "pending"
    created_at: datetime = field(default_factory=_utcnow)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    deadline: datetime | None = None
    cancel_requested: bool = False
    timed_out: bool = False
    error: str | None = None
    report_id: int | None = None
    report: DailyAuditReportOut | None = None
    # 运行期句柄
    conn: object | None = None
    session: dbmod.SnapshotSession | None = None
    abort_done: bool = False

    def snapshot(self) -> DailyAuditJobOut:
        return DailyAuditJobOut(
            audit_id=self.audit_id,
            status=self.status,
            audit_date=self.audit_date,
            created_at=self.created_at,
            started_at=self.started_at,
            finished_at=self.finished_at,
            error=self.error,
            report=self.report,
        )


class DailyAuditManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active: DailyAuditJob | None = None
        self._audits: dict[str, DailyAuditJob] = {}
        self._latest: dict[str, str] = {}

    # ---------- 查询 ----------
    def get(self, audit_id: str) -> DailyAuditJob | None:
        with self._lock:
            return self._audits.get(audit_id)

    # ---------- 创建 ----------
    def create(self, day: date) -> DailyAuditJob:
        job = DailyAuditJob(audit_id=uuid.uuid4().hex, audit_date=day)
        with self._lock:
            if self._active is not None:
                raise AuditSlotBusyError(self._active.audit_id)
            self._active = job
            self._audits[job.audit_id] = job
            self._latest[day.isoformat()] = job.audit_id
        thread = threading.Thread(
            target=self._run_guarded,
            args=(job,),
            name=f"daily-audit-{job.audit_id[:8]}",
            daemon=True,
        )
        thread.start()
        return job

    # ---------- 取消 ----------
    def request_cancel(self, audit_id: str) -> DailyAuditJob:
        with self._lock:
            job = self._audits.get(audit_id)
            if job is None:
                raise KeyError(audit_id)
            if job.status in AUDIT_TERMINAL_STATUSES:
                return job
            job.cancel_requested = True
            job.status = "cancelling"
            conn = job.conn
        if conn is not None:
            try:
                conn.cancel()
            except Exception:
                pass
        return job

    # ---------- 主流程 ----------
    def _run_guarded(self, job: DailyAuditJob) -> None:
        try:
            self._run(job)
        except Exception as exc:  # 兜底：任何未预期异常都要到终态并释放名额
            traceback.print_exc()
            self._finalize(
                job,
                status="failed",
                error=f"内部错误：{exc.__class__.__name__}: {exc}",
            )

    def _run(self, job: DailyAuditJob) -> None:
        from . import daily_audit as audit_mod

        job.started_at = _utcnow()
        job.deadline = job.started_at + timedelta(seconds=settings.job_timeout_seconds)
        job.status = "running"

        def is_cancelled() -> bool:
            return job.cancel_requested

        def check_deadline() -> None:
            if job.timed_out or (
                job.deadline is not None and _utcnow() >= job.deadline
            ):
                job.timed_out = True
                raise AuditTimeout()

        watchdog = threading.Thread(
            target=self._watchdog,
            args=(job,),
            name=f"daily-watchdog-{job.audit_id[:8]}",
            daemon=True,
        )
        watchdog.start()
        try:
            report_id = None
            try:
                # 1) 先落一行 running，任何失败都不会留下“成功”报告
                store.ensure_date_version_row(job.audit_date)
                report_id = store.create_running(
                    job.audit_date,
                    max_gap_minutes=settings.max_gap_minutes,
                    attempt=store.next_attempt(job.audit_date),
                )
                job.report_id = report_id

                if is_cancelled():
                    raise AuditCancelled()
                check_deadline()

                # 2) 单一快照事务内完整扫描
                conn = dbmod.open_connection()
                job.conn = conn
                try:
                    conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                    version = audit_mod.current_version(conn, job.audit_date)
                    stats = audit_mod.scan_day(
                        conn,
                        job.audit_date,
                        is_cancelled=is_cancelled,
                        check_deadline=check_deadline,
                        max_anomalies=settings.max_anomalies,
                        max_gap_minutes=settings.max_gap_minutes,
                    )
                    check_deadline()
                    if is_cancelled():
                        raise AuditCancelled()
                    # 只读快照事务在这里结束；读取的数据与 version 一致
                    conn.execute("COMMIT")
                finally:
                    dbmod._quiet_close(conn)
                    job.conn = None

                check_deadline()
                if is_cancelled():
                    raise AuditCancelled()

                # 3) 另起事务复核版本并原子写入报告；版本变化->aborted
                report = store.commit_success(
                    report_id,
                    job.audit_date,
                    expected_version=version,
                    stats=stats,
                )
                job.report = report
            except AuditCancelled:
                if report_id is not None:
                    self._safe_mark(report_id, aborted=False, error="用户取消，未发布结论")
                self._finalize(job, status="cancelled",
                               error="本轮已取消，未发布核查结论")
                return
            except AuditTimeout:
                if report_id is not None:
                    self._safe_mark(report_id, aborted=False,
                                    error="超过单轮时限，未发布结论")
                self._finalize(
                    job,
                    status="timeout",
                    error=f"超过单轮 {settings.job_timeout_seconds} 秒时限，"
                          f"已终止本轮扫描，未发布核查结论",
                )
                return
            except AuditAborted as exc:
                if report_id is not None:
                    self._safe_mark(report_id, aborted=True, error=str(exc))
                self._finalize(job, status="aborted", error=str(exc))
                return
            except AuditError as exc:
                if report_id is not None:
                    self._safe_mark(report_id, aborted=False, error=str(exc))
                self._finalize(job, status="failed", error=str(exc))
                return
            except Exception as exc:  # 数据库/保存等任何异常
                if report_id is not None:
                    self._safe_mark(report_id, aborted=True,
                                    error=f"报告保存失败：{exc}")
                self._finalize(
                    job,
                    status="aborted",
                    error=f"报告保存失败，未发布核查结论：{exc}",
                )
                return
        finally:
            job.deadline = None
            watchdog.join(timeout=2)

        self._finalize(job, status="succeeded", error=None)

    def _safe_mark(self, report_id: int, *, aborted: bool, error: str) -> None:
        try:
            if aborted:
                store.mark_aborted(report_id, error)
            else:
                store.mark_failed(report_id, error)
        except Exception:
            # 标记失败也不能覆盖真正的错误结论；按日期查询时该报告
            # 仍不是 succeeded，不会被视为可用。
            traceback.print_exc()

    def _watchdog(self, job: DailyAuditJob) -> None:
        while True:
            deadline = job.deadline
            if deadline is None:
                return
            remaining = (deadline - _utcnow()).total_seconds()
            if remaining <= 0:
                if job.status not in AUDIT_TERMINAL_STATUSES:
                    job.timed_out = True
                    conn = job.conn
                    if conn is not None:
                        try:
                            conn.cancel()
                        except Exception:
                            pass
                return
            time.sleep(min(0.2, remaining))

    def _finalize(
        self, job: DailyAuditJob, *, status: AuditStatus, error: str | None
    ) -> None:
        if job.status in AUDIT_TERMINAL_STATUSES:
            return
        conn = job.conn
        job.deadline = None
        if conn is not None and status != "succeeded":
            try:
                conn.cancel()
            except Exception:
                pass
        if conn is not None:
            dbmod._quiet_close(conn)
            job.conn = None

        with self._lock:
            if self._active is job:
                self._active = None
            if len(self._audits) > 20:
                for aid in list(self._audits):
                    if self._audits[aid].status in AUDIT_TERMINAL_STATUSES:
                        self._audits.pop(aid, None)
                        break

        job.error = error
        job.finished_at = _utcnow()
        job.status = status


daily_manager = DailyAuditManager()
