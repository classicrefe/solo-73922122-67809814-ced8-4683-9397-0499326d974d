"""按 UTC 日期核查 readings 的日结可用性。

判定规则：
- 只看 ``[date 00:00:00Z, date+1 00:00:00Z)`` 内、该日期各表的读数；
- 每块表按 read_time 排序，逐对比较相邻累计 Wh：
    * 后一条 < 前一条（读数回退）
    * 相邻间隔 > max_gap_minutes（默认 120 分钟）
  任一条件成立即记为该表的一条异常，报告给出表号、前后时刻与读数；
- 当天**首条**读数不接前一日，因此不参与跨日比较；
- 某块表当天没有任何读数时，不能判定为“可用于结算”。

一致性：
- 扫描在单个 ``REPEATABLE READ READ ONLY`` 事务内完成，版本号也取自
  同一快照，保证“扫描到的数据”和“记录下来的版本”一致；
- 版本行先确保存在，保存结论时在同一事务内 ``FOR SHARE`` 锁住该日
  版本行并复核。任何在扫描期间提交的更正都会使版本变化，本轮直接
  作废（aborted），绝不发布旧口径结论；
- 报告先以 ``running`` 落库，只有全部校验通过才原子地改成
  ``succeeded``；保存失败时报告停留在 running 或被标记 failed/aborted，
  按日期查询时 ``usable`` 恒为 false。

过期语义：
- 报告记录扫描时看到的 ``source_version``。查询时与当前
  ``recon.reading_dates.version`` 比较，只要源表在该 UTC 日期上有任何
  已提交改动（包括直接 SQL），旧报告即显示 ``stale=true``；
- UPDATE 触发器同时标记旧、新两个 UTC 日期，读数跨日移动时两天都过期。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import psycopg

from .config import settings
from .models import (
    AnomalyOut,
    DailyAuditReportOut,
    MeterWithoutReadings,
)


class AuditError(RuntimeError):
    """核查无法完成（不发布结论）。"""


class AuditAborted(AuditError):
    """扫描期间源表被更正 / 报告保存失败：本轮不发布结论。"""


class AuditCancelled(AuditError):
    pass


class AuditTimeout(AuditError):
    pass


# ---------------------------------------------------------------- 数据结构


@dataclass
class Anomaly:
    meter_id: int
    previous_time: datetime
    previous_wh: int
    current_time: datetime
    current_wh: int
    gap_seconds: int
    reasons: list[str]

    @property
    def sort_key(self) -> tuple:
        return (self.meter_id, self.current_time, self.previous_time)


@dataclass
class DayStats:
    readings_count: int = 0
    meters_with_readings: int = 0
    meters_total: int = 0
    meters_without_readings: list[int] = field(default_factory=list)
    anomalies: list[Anomaly] = field(default_factory=list)
    # 覆盖全量的异常计数；anomalies 只保留排序后的前 max_anomalies 条
    anomalies_seen_total: int = 0


def utc_day_start(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, tzinfo=timezone.utc)


def audit_date_range(day: date) -> tuple[datetime, datetime]:
    start = utc_day_start(day)
    return start, start + timedelta(days=1)


def reason_label(reasons: list[str], max_gap_minutes: int) -> str:
    labels = []
    if "rollback" in reasons:
        labels.append("读数回退")
    if "gap" in reasons:
        labels.append(f"间隔超过 {max_gap_minutes} 分钟")
    return "；".join(labels)


def current_version(conn: psycopg.Connection, day: date) -> int:
    row = conn.execute(
        "SELECT version FROM recon.reading_dates WHERE audit_date = %s", (day,)
    ).fetchone()
    return int(row[0]) if row else 0


# ---------------------------------------------------------------- 扫描


def scan_day(
    conn: psycopg.Connection,
    day: date,
    *,
    is_cancelled: Callable[[], bool],
    check_deadline: Callable[[], None],
    max_anomalies: int,
    max_gap_minutes: int,
) -> DayStats:
    """在调用方事务内扫描一天（不发布结论，仅收集事实）。"""
    start, end = audit_date_range(day)
    stats = DayStats()

    stats.meters_total = int(
        conn.execute("SELECT count(*) FROM meters").fetchone()[0]
    )
    stats.readings_count = int(
        conn.execute(
            "SELECT count(*) FROM readings "
            "WHERE read_time >= %s AND read_time < %s",
            (start, end),
        ).fetchone()[0]
    )
    stats.meters_with_readings = int(
        conn.execute(
            "SELECT count(DISTINCT meter_id) FROM readings "
            "WHERE read_time >= %s AND read_time < %s",
            (start, end),
        ).fetchone()[0]
    )
    stats.meters_without_readings = [
        int(r[0])
        for r in conn.execute(
            "SELECT m.meter_id FROM meters m "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM readings r WHERE r.meter_id = m.meter_id"
            "    AND r.read_time >= %s AND r.read_time < %s"
            ") ORDER BY m.meter_id",
            (start, end),
        ).fetchall()
    ]

    max_gap_seconds = int(max_gap_minutes) * 60

    # 单条命名游标按 meter_id, read_time 顺序流式扫描；内存里同时只保留
    # “当前表的上一条读数”与最多 max_anomalies 条异常明细。
    with conn.cursor(name=f"daily_scan_{day.isoformat()}") as cur:
        cur.itersize = max(1000, min(settings.daily_read_chunk_rows, 50000))
        cur.execute(
            "SELECT meter_id, read_time, wh FROM readings "
            "WHERE read_time >= %s AND read_time < %s "
            "ORDER BY meter_id, read_time",
            (start, end),
        )
        prev_meter: int | None = None
        prev_time: datetime | None = None
        prev_wh: int | None = None
        while True:
            if is_cancelled():
                raise AuditCancelled()
            check_deadline()
            rows = cur.fetchmany(cur.itersize)
            if not rows:
                break
            for meter_id, read_time, wh in rows:
                meter_id = int(meter_id)
                wh = int(wh)
                if meter_id != prev_meter:
                    prev_meter = meter_id
                    prev_time = None
                    prev_wh = None
                # 当天首条（prev_time is None）不接前一天，不做跨日比较
                if prev_time is not None and prev_wh is not None:
                    gap_seconds = int((read_time - prev_time).total_seconds())
                    reasons: list[str] = []
                    if wh < prev_wh:
                        reasons.append("rollback")
                    if gap_seconds > max_gap_seconds:
                        reasons.append("gap")
                    if reasons:
                        stats.anomalies_seen_total += 1
                        if len(stats.anomalies) < max_anomalies:
                            stats.anomalies.append(
                                Anomaly(
                                    meter_id=meter_id,
                                    previous_time=prev_time,
                                    previous_wh=prev_wh,
                                    current_time=read_time,
                                    current_wh=wh,
                                    gap_seconds=gap_seconds,
                                    reasons=reasons,
                                )
                            )
                prev_time = read_time
                prev_wh = wh

    return stats


# ---------------------------------------------------------------- 报告读写


class ReportStore:
    """报告的读写；所有写操作都在独立事务中完成。"""

    def __init__(self, dsn: str | None = None) -> None:
        self._dsn = dsn or settings.pg_dsn

    def _connect(self) -> psycopg.Connection:
        conn = psycopg.connect(
            self._dsn,
            connect_timeout=settings.connect_timeout,
            autocommit=False,
            application_name="recon-audit-store",
        )
        conn.execute(
            f"SET SESSION statement_timeout = {int(settings.statement_timeout_ms)}"
        )
        conn.execute(f"SET SESSION lock_timeout = {int(settings.lock_timeout_ms)}")
        return conn

    def ensure_schema(self) -> None:
        """应用启动时补齐报告结构（幂等）。"""
        sql_path = Path(__file__).resolve().parents[1] / "sql" / "audit.sql"
        with self._connect() as conn:
            conn.execute(sql_path.read_text(encoding="utf-8"))
            conn.commit()

    def ensure_date_version_row(self, day: date) -> None:
        """确保版本行存在（不改变版本号）。

        这样保存结论时能对固定行加 ``FOR SHARE`` 锁：若结算扫描期间
        有更正提交，锁会与触发器的 ``ON CONFLICT DO UPDATE`` 冲突，
        或版本复核直接看到变化，从而拒绝发布旧结论。
        """
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO recon.reading_dates (audit_date, version) "
                "VALUES (%s, 1) ON CONFLICT (audit_date) DO NOTHING",
                (day,),
            )
            conn.commit()

    def next_attempt(self, day: date) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COALESCE(max(attempt), 0) + 1 "
                "FROM recon.daily_audit_reports WHERE audit_date = %s",
                (day,),
            ).fetchone()
            return int(row[0])

    def create_running(
        self, day: date, *, max_gap_minutes: int, attempt: int
    ) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "INSERT INTO recon.daily_audit_reports "
                "(audit_date, status, attempt, max_gap_minutes) "
                "VALUES (%s, 'running', %s, %s) RETURNING id",
                (day, attempt, max_gap_minutes),
            ).fetchone()
            conn.commit()
            return int(row[0])

    def mark_failed(self, report_id: int, error: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE recon.daily_audit_reports SET status = 'failed', "
                "usable = false, error = %s, finished_at = now() WHERE id = %s",
                (error[:2000], report_id),
            )
            conn.commit()

    def mark_aborted(self, report_id: int, error: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE recon.daily_audit_reports SET status = 'aborted', "
                "usable = false, error = %s, finished_at = now() WHERE id = %s",
                (error[:2000], report_id),
            )
            conn.commit()

    def commit_success(
        self,
        report_id: int,
        day: date,
        *,
        expected_version: int,
        stats: DayStats,
    ) -> DailyAuditReportOut:
        """在一个事务里复核版本并写入结论。

        版本变化或任一 SQL 失败 -> 事务回滚、报告保持 running，
        调用方随后标记 aborted；绝不留下半份“成功”报告。
        """
        with self._connect() as conn:
            try:
                version_row = conn.execute(
                    "SELECT version FROM recon.reading_dates "
                    "WHERE audit_date = %s FOR SHARE",
                    (day,),
                ).fetchone()
                current_version = int(version_row[0]) if version_row else 0
                if current_version != expected_version:
                    conn.rollback()
                    raise AuditAborted(
                        f"扫描期间源表已被更正（版本 {expected_version} -> "
                        f"{current_version}），本轮结论作废"
                    )

                anomalies = sorted(stats.anomalies, key=lambda a: a.sort_key)
                anomaly_total = stats.anomalies_seen_total
                # 没有任何读数（或连表都没有）同样不能判定为可用
                usable = (
                    anomaly_total == 0
                    and not stats.meters_without_readings
                    and stats.readings_count > 0
                    and stats.meters_total > 0
                )

                reasons: list[str] = []
                if anomaly_total:
                    reasons.append(f"发现 {anomaly_total} 条相邻读数异常")
                if stats.meters_without_readings:
                    reasons.append(
                        f"{len(stats.meters_without_readings)} 块表当天无读数，"
                        f"不能判定为可用"
                    )
                if stats.readings_count == 0:
                    reasons.append("当天没有任何读数，不能判定为可用")
                if stats.meters_total == 0:
                    reasons.append("表计档案为空，不能判定为可用")
                usable_reason = "；".join(reasons) if reasons else "当天读数通过核查"

                conn.execute(
                    "UPDATE recon.daily_audit_reports SET status = 'succeeded', "
                    "source_version = %s, usable = %s, usable_reason = %s, "
                    "anomaly_total = %s, readings_count = %s, "
                    "meters_with_readings = %s, meters_total = %s, "
                    "finished_at = now() WHERE id = %s",
                    (
                        expected_version,
                        usable,
                        usable_reason,
                        anomaly_total,
                        stats.readings_count,
                        stats.meters_with_readings,
                        stats.meters_total,
                        report_id,
                    ),
                )
                for seq, anomaly in enumerate(anomalies, start=1):
                    conn.execute(
                        "INSERT INTO recon.daily_audit_anomalies "
                        "(report_id, seq, meter_id, prev_time, prev_wh, "
                        " curr_time, curr_wh, gap_seconds, reasons) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (
                            report_id,
                            seq,
                            anomaly.meter_id,
                            anomaly.previous_time,
                            anomaly.previous_wh,
                            anomaly.current_time,
                            anomaly.current_wh,
                            anomaly.gap_seconds,
                            anomaly.reasons,
                        ),
                    )
                for meter_id in stats.meters_without_readings:
                    conn.execute(
                        "INSERT INTO recon.daily_audit_missing_meters "
                        "(report_id, meter_id) VALUES (%s, %s)",
                        (report_id, meter_id),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            return self._build_report(conn, report_id)

    def _build_report(
        self, conn: psycopg.Connection, report_id: int
    ) -> DailyAuditReportOut:
        row = conn.execute(
            "SELECT id, audit_date, status, attempt, source_version, usable, "
            "usable_reason, anomaly_total, readings_count, meters_with_readings, "
            "meters_total, max_gap_minutes, error, started_at, finished_at, created_at "
            "FROM recon.daily_audit_reports WHERE id = %s",
            (report_id,),
        ).fetchone()
        if row is None:
            raise AuditError("报告不存在")
        (
            rid, audit_date, status, attempt, source_version, usable,
            usable_reason, anomaly_total, readings_count, meters_with_readings,
            meters_total, max_gap_minutes, error, started_at, finished_at, created_at,
        ) = row

        version_now = current_version(conn, audit_date)
        stale = (
            source_version is not None
            and int(source_version) != int(version_now)
        )
        anomaly_rows = conn.execute(
            "SELECT seq, meter_id, prev_time, prev_wh, curr_time, "
            "curr_wh, gap_seconds, reasons "
            "FROM recon.daily_audit_anomalies WHERE report_id = %s ORDER BY seq",
            (report_id,),
        ).fetchall()
        anomalies = [
            AnomalyOut(
                seq=int(r[0]),
                meter_id=int(r[1]),
                previous_time=r[2],
                previous_wh=int(r[3]),
                current_time=r[4],
                current_wh=int(r[5]),
                gap_seconds=int(r[6]),
                reasons=list(r[7]),
                reason_text=reason_label(list(r[7]), int(max_gap_minutes)),
            )
            for r in anomaly_rows
        ]
        missing_rows = conn.execute(
            "SELECT m.meter_id, m.line_name "
            "FROM recon.daily_audit_missing_meters mm "
            "LEFT JOIN meters m ON m.meter_id = mm.meter_id "
            "WHERE mm.report_id = %s ORDER BY mm.meter_id",
            (report_id,),
        ).fetchall()
        missing = [
            MeterWithoutReadings(meter_id=int(r[0]), line_name=r[1])
            for r in missing_rows
        ]

        # 该日期最新一次尝试：若它失败/中止/仍在运行，则即使较早的
        # 报告本身成功且未过期，也不能再把它当作结算依据。
        latest = conn.execute(
            "SELECT status, error FROM recon.daily_audit_reports "
            "WHERE audit_date = %s ORDER BY id DESC LIMIT 1",
            (audit_date,),
        ).fetchone()
        latest_status = latest[0] if latest else None
        latest_error = latest[1] if latest else None
        blocked_reason = None
        if latest_status is not None and latest_status != "succeeded":
            blocked_reason = (
                f"该日期最新一次核查为 {latest_status}，未发布可用结论"
                + (f"：{latest_error}" if latest_error else "")
            )
        elif latest_status is None:
            blocked_reason = "该日期没有核查记录"

        return DailyAuditReportOut(
            report_id=int(rid),
            audit_date=audit_date,
            status=status,
            attempt=int(attempt),
            usable=bool(usable),
            usable_reason=usable_reason,
            stale=stale,
            stale_reason=(
                f"源表在该 UTC 日期上有已提交改动（版本 {source_version} -> "
                f"{version_now}），请重新核查"
                if stale
                else None
            ),
            # 只有“最新一次尝试成功、未过期且通过核查”才可用于结算
            settlement_usable=bool(
                status == "succeeded"
                and usable
                and not stale
                and latest_status == "succeeded"
            ),
            latest_attempt_status=latest_status,
            usable_blocked_reason=(
                None
                if latest_status == "succeeded"
                else blocked_reason
            ),
            source_version=(
                int(source_version) if source_version is not None else None
            ),
            current_version=int(version_now),
            anomaly_total=(
                int(anomaly_total) if anomaly_total is not None else None
            ),
            readings_count=(
                int(readings_count) if readings_count is not None else None
            ),
            meters_with_readings=(
                int(meters_with_readings)
                if meters_with_readings is not None
                else None
            ),
            meters_total=(
                int(meters_total) if meters_total is not None else None
            ),
            meters_without_readings=missing,
            max_gap_minutes=int(max_gap_minutes),
            anomalies=anomalies,
            error=error,
            started_at=started_at,
            finished_at=finished_at,
            created_at=created_at,
        )

    def _load_id(self, day: date, *, succeeded_only: bool) -> int | None:
        with self._connect() as conn:
            condition = "AND status = 'succeeded' " if succeeded_only else ""
            row = conn.execute(
                "SELECT id FROM recon.daily_audit_reports "
                f"WHERE audit_date = %s {condition}"
                "ORDER BY id DESC LIMIT 1",
                (day,),
            ).fetchone()
            return int(row[0]) if row else None

    def load_latest(self, day: date) -> DailyAuditReportOut | None:
        """最新一次尝试（含 running / failed / aborted）。"""
        report_id = self._load_id(day, succeeded_only=False)
        if report_id is None:
            return None
        with self._connect() as conn:
            return self._build_report(conn, report_id)

    def load_latest_succeeded(self, day: date) -> DailyAuditReportOut | None:
        """最近一次成功报告（历史查看；可能已 stale）。"""
        report_id = self._load_id(day, succeeded_only=True)
        if report_id is None:
            return None
        with self._connect() as conn:
            return self._build_report(conn, report_id)

    def load_report(self, report_id: int) -> DailyAuditReportOut | None:
        with self._connect() as conn:
            exists = conn.execute(
                "SELECT 1 FROM recon.daily_audit_reports WHERE id = %s",
                (report_id,),
            ).fetchone()
            if not exists:
                return None
            return self._build_report(conn, report_id)

    def load_history(self, day: date, limit: int = 50) -> list[DailyAuditReportOut]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id FROM recon.daily_audit_reports "
                "WHERE audit_date = %s ORDER BY id DESC LIMIT %s",
                (day, max(1, min(limit, 200))),
            ).fetchall()
            return [self._build_report(conn, int(r[0])) for r in rows]


store = ReportStore()
