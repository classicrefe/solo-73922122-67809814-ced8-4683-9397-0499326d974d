"""日结读数核查：API、持久化、过期与失败语义的端到端测试。"""
from __future__ import annotations

import time
from datetime import date, timedelta

import psycopg
import pytest
from fastapi.testclient import TestClient

# 使用独立日期与独立表号，避免与示例数据、其他测试互相干扰
DAY = date(2027, 1, 10)
NEXT_DAY = date(2027, 1, 11)
M1, M2, M3 = 9101, 9102, 9103


def _wait_audit(client: TestClient, audit_id: str, timeout: float = 30) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = client.get(f"/daily-audits/{audit_id}")
        assert resp.status_code == 200
        body = resp.json()
        if body["status"] in (
            "succeeded", "failed", "aborted", "cancelled", "timeout"
        ):
            return body
        time.sleep(0.05)
    raise AssertionError("日核查未在时限内到达终态")


def _run(client: TestClient, day: date = DAY) -> dict:
    resp = client.post(f"/daily-audits?date={day.isoformat()}")
    assert resp.status_code == 201, resp.text
    return _wait_audit(client, resp.json()["audit_id"])


@pytest.fixture()
def clean_day(pg_dsn: str):
    """准备独立表号，并清空两个测试日期的全部读数与旧报告。

    抽查范围是 meters 表中的**全部**表，因此这里也会为示例表补齐
    测试日期的读数，保证“干净日”确实干净。
    """
    conn = psycopg.connect(pg_dsn, autocommit=True)
    try:
        _wipe(conn)
        conn.execute(
            "INSERT INTO meters (meter_id, line_name) VALUES (%s,'T1'),(%s,'T2'),(%s,'T3') "
            "ON CONFLICT DO NOTHING",
            (M1, M2, M3),
        )
        _seed(conn, DAY)
        _seed(conn, NEXT_DAY)
        yield conn
    finally:
        _wipe(conn)
        conn.execute("DELETE FROM meters WHERE meter_id = ANY(%s)",
                     ([M1, M2, M3],))
        conn.close()


def _wipe(conn) -> None:
    """清空测试日期附近**所有表**的读数，避免压测种子数据干扰。

    示例数据固定在 2026-11-15，这里只清理 2026-12-31 之后的区间。
    """
    conn.execute(
        "DELETE FROM readings WHERE read_time >= %s::timestamptz "
        "AND read_time < %s::timestamptz",
        ("2026-12-31 00:00:00+00", "2027-02-01 00:00:00+00"),
    )
    conn.execute(
        "DELETE FROM recon.daily_audit_reports WHERE audit_date IN (%s, %s)",
        (DAY, NEXT_DAY),
    )


def _seed(conn, day: date) -> None:
    """正常数据：所有表都在 08:00/09:00/10:00 有三个递增读数。"""
    iso = day.isoformat()
    meters = [int(r[0]) for r in conn.execute(
        "SELECT meter_id FROM meters ORDER BY meter_id"
    ).fetchall()]
    rows = []
    for meter in meters:
        for hour, wh in ((8, 1000), (9, 1100), (10, 1200)):
            rows.append((meter, f"{iso} {hour:02d}:00:00+00", wh))
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO readings (meter_id, read_time, wh) "
            "VALUES (%s, %s::timestamptz, %s) ON CONFLICT DO NOTHING",
            rows,
        )


def test_clean_day_is_usable_and_report_persisted(
    client: TestClient, clean_day: psycopg.Connection
) -> None:
    out = _run(client)
    assert out["status"] == "succeeded"
    report = out["report"]
    assert report["audit_date"] == DAY.isoformat()
    assert report["usable"] is True
    assert report["settlement_usable"] is True
    assert report["stale"] is False
    assert report["anomaly_total"] == 0
    assert report["anomalies"] == []
    # 夹具为每块表写入 3 条读数（表数取决于当前测试库，不写死）
    assert report["readings_count"] == report["meters_total"] * 3
    assert report["meters_with_readings"] == report["meters_total"]
    assert report["meters_with_readings"] >= 3
    assert report["meters_without_readings"] == []

    # 按日期查询同一份持久化报告
    got = client.get(f"/daily-reports?date={DAY.isoformat()}")
    assert got.status_code == 200
    assert got.json()["report_id"] == report["report_id"]
    assert got.json()["settlement_usable"] is True

    # 按 ID 查询
    by_id = client.get(f"/daily-reports/{report['report_id']}")
    assert by_id.status_code == 200
    assert by_id.json()["report_id"] == report["report_id"]


def test_rollback_and_gap_are_reported_with_times_and_values(
    client: TestClient, clean_day: psycopg.Connection
) -> None:
    # M2: 10:00 回退；M3: 08:00 -> 11:00 间隔 3 小时
    clean_day.execute(
        "UPDATE readings SET wh = 900 WHERE meter_id = %s "
        "AND read_time = %s::timestamptz",
        (M2, f"{DAY.isoformat()} 10:00:00+00"),
    )
    clean_day.execute(
        "DELETE FROM readings WHERE meter_id = %s AND read_time >= %s::timestamptz",
        (M3, f"{DAY.isoformat()} 09:00:00+00"),
    )
    clean_day.execute(
        "INSERT INTO readings (meter_id, read_time, wh) VALUES "
        "(%s, %s::timestamptz, 1200)",
        (M3, f"{DAY.isoformat()} 11:00:00+00"),
    )

    out = _run(client)
    report = out["report"]
    assert report["usable"] is False
    assert report["settlement_usable"] is False
    assert report["anomaly_total"] == 2
    assert len(report["anomalies"]) == 2
    assert "无读数" not in (report["usable_reason"] or "")

    by_meter = {a["meter_id"]: a for a in report["anomalies"]}
    m2 = by_meter[M2]
    assert m2["previous_wh"] == 1100 and m2["current_wh"] == 900
    assert m2["previous_time"].startswith("2027-01-10T09:00:00")
    assert m2["current_time"].startswith("2027-01-10T10:00:00")
    assert m2["reasons"] == ["rollback"]
    assert "回退" in m2["reason_text"]

    m3 = by_meter[M3]
    assert m3["reasons"] == ["gap"]
    assert m3["gap_seconds"] == 3 * 3600
    assert m3["previous_wh"] == 1000 and m3["current_wh"] == 1200

    # 查询接口返回同样的明细
    got = client.get(f"/daily-reports?date={DAY.isoformat()}").json()
    assert got["anomaly_total"] == 2
    assert [a["meter_id"] for a in got["anomalies"]] == [M2, M3]


def test_gap_exactly_two_hours_is_not_anomalous(
    client: TestClient, clean_day: psycopg.Connection
) -> None:
    clean_day.execute(
        "DELETE FROM readings WHERE meter_id = ANY(%s) AND read_time = %s::timestamptz",
        ([M1, M2, M3], f"{DAY.isoformat()} 09:00:00+00"),
    )
    out = _run(client)
    report = out["report"]
    assert report["anomaly_total"] == 0
    assert report["usable"] is True


def test_first_reading_of_day_is_not_compared_with_previous_day(
    client: TestClient, clean_day: psycopg.Connection
) -> None:
    # 前一天 23:00 读数很高，当天 00:30 更低：属于跨日，不该报回退
    clean_day.execute(
        "DELETE FROM readings WHERE meter_id = ANY(%s)",
        ([M1, M2, M3],),
    )
    for meter in (M1, M2, M3):
        clean_day.execute(
            "INSERT INTO readings (meter_id, read_time, wh) VALUES "
            "(%s, %s::timestamptz, 99999), (%s, %s::timestamptz, 10), "
            "(%s, %s::timestamptz, 20)",
            (
                meter, f"{(DAY.replace(day=9)).isoformat()} 23:00:00+00",
                meter, f"{DAY.isoformat()} 00:30:00+00",
                meter, f"{DAY.isoformat()} 01:30:00+00",
            ),
        )
    out = _run(client)
    report = out["report"]
    assert report["anomaly_total"] == 0
    assert report["usable"] is True


def test_meter_without_readings_blocks_usability(
    client: TestClient, clean_day: psycopg.Connection
) -> None:
    clean_day.execute("DELETE FROM readings WHERE meter_id = %s", (M3,))
    out = _run(client)
    report = out["report"]
    assert report["usable"] is False
    assert report["settlement_usable"] is False
    assert report["anomalies"] == []
    assert report["anomaly_total"] == 0
    assert [m["meter_id"] for m in report["meters_without_readings"]] == [M3]
    assert "无读数" in report["usable_reason"]


def test_anomalies_capped_at_20_but_total_is_full(
    client: TestClient, clean_day: psycopg.Connection
) -> None:
    # M1 当天追加 26 条持续回退的读数（每 30 分钟一条，后一条比前一条小）
    clean_day.execute("DELETE FROM readings WHERE meter_id = %s", (M1,))
    with clean_day.cursor() as cur:
        for i in range(26):
            cur.execute(
                "INSERT INTO readings (meter_id, read_time, wh) VALUES "
                "(%s, %s::timestamptz, %s)",
                (M1, f"{DAY.isoformat()} {i // 2:02d}:{(i % 2) * 30:02d}:00+00",
                 5000 - i * 10),
            )
    out = _run(client)
    report = out["report"]
    assert report["anomaly_total"] == 25  # 26 条读数 -> 25 个相邻对
    assert len(report["anomalies"]) == 20
    # 序列按（表号，当前时刻）排序且编号连续
    assert [a["seq"] for a in report["anomalies"]] == list(range(1, 21))


def test_stale_after_direct_sql_insert_update_delete(
    client: TestClient, clean_day: psycopg.Connection
) -> None:
    first = _run(client)["report"]
    assert first["settlement_usable"] is True

    # 1) 直接 SQL 插入（不经过应用）-> 旧报告过期
    clean_day.execute(
        "INSERT INTO readings (meter_id, read_time, wh) VALUES "
        "(%s, %s::timestamptz, 1300)",
        (M1, f"{DAY.isoformat()} 11:00:00+00"),
    )
    got = client.get(f"/daily-reports?date={DAY.isoformat()}").json()
    assert got["stale"] is True
    assert got["settlement_usable"] is False
    assert "版本" in got["stale_reason"]

    # 2) 重新核查后恢复可用
    second = _run(client)["report"]
    assert second["report_id"] != first["report_id"]
    assert second["stale"] is False
    assert second["settlement_usable"] is True

    # 3) 直接 SQL 删除 -> 再次过期
    clean_day.execute(
        "DELETE FROM readings WHERE meter_id = %s AND read_time = %s::timestamptz",
        (M1, f"{DAY.isoformat()} 11:00:00+00"),
    )
    assert client.get(
        f"/daily-reports?date={DAY.isoformat()}"
    ).json()["stale"] is True

    # 4) 重查 -> 可用；随后直接 SQL 更正 -> 过期
    third = _run(client)["report"]
    assert third["settlement_usable"] is True
    clean_day.execute(
        "UPDATE readings SET wh = wh + 1 WHERE meter_id = %s "
        "AND read_time = %s::timestamptz",
        (M2, f"{DAY.isoformat()} 09:00:00+00"),
    )
    assert client.get(
        f"/daily-reports?date={DAY.isoformat()}"
    ).json()["stale"] is True


def test_moving_reading_across_days_expires_both_days(
    client: TestClient, clean_day: psycopg.Connection
) -> None:
    # 先核查两天并拿到可用报告
    first_day = _run(client, DAY)["report"]
    second_day = _run(client, NEXT_DAY)["report"]
    assert first_day["settlement_usable"] is True
    assert second_day["settlement_usable"] is True

    # 把 DAY 10:00 的读数移动到 NEXT_DAY 12:00（同一表，目标时刻空闲）
    clean_day.execute(
        "UPDATE readings SET read_time = %s::timestamptz "
        "WHERE meter_id = %s AND read_time = %s::timestamptz",
        (f"{NEXT_DAY.isoformat()} 12:00:00+00", M1,
         f"{DAY.isoformat()} 10:00:00+00"),
    )
    d1 = client.get(f"/daily-reports?date={DAY.isoformat()}").json()
    d2 = client.get(f"/daily-reports?date={NEXT_DAY.isoformat()}").json()
    assert d1["stale"] is True and d1["settlement_usable"] is False
    assert d2["stale"] is True and d2["settlement_usable"] is False


def test_source_change_during_scan_aborts_without_conclusion(
    client: TestClient, clean_day: psycopg.Connection, monkeypatch
) -> None:
    from app import daily_audit as audit_mod

    real_scan = audit_mod.scan_day
    mutated = {"done": False}

    def scan_then_mutate(conn, day, **kwargs):
        stats = real_scan(conn, day, **kwargs)
        if not mutated["done"]:
            mutated["done"] = True
            # 模拟扫描过程中另一会话提交的直接 SQL 更正
            clean_day.execute(
                "INSERT INTO readings (meter_id, read_time, wh) VALUES "
                "(%s, %s::timestamptz, 7777)",
                (M2, f"{DAY.isoformat()} 12:00:00+00"),
            )
        return stats

    monkeypatch.setattr(audit_mod, "scan_day", scan_then_mutate)

    out = _run(client)
    assert out["status"] == "aborted"
    assert out["report"] is None
    assert "版本" in out["error"]

    # 没有发布结论：最新尝试是 aborted，按日期查询无成功报告
    latest = client.get(f"/daily-reports/latest?date={DAY.isoformat()}")
    assert latest.status_code == 200
    assert latest.json()["status"] == "aborted"
    assert latest.json()["settlement_usable"] is False
    assert client.get(f"/daily-reports?date={DAY.isoformat()}").status_code == 404

    # 重查成功后才恢复可用
    monkeypatch.setattr(audit_mod, "scan_day", real_scan)
    ok = _run(client)["report"]
    assert ok["settlement_usable"] is True


def test_report_save_failure_publishes_no_conclusion_and_keeps_old_stale(
    client: TestClient, clean_day: psycopg.Connection, monkeypatch
) -> None:
    old = _run(client)["report"]
    assert old["settlement_usable"] is True

    # 源表变动 -> 旧报告过期
    clean_day.execute(
        "INSERT INTO readings (meter_id, read_time, wh) VALUES "
        "(%s, %s::timestamptz, 1400)",
        (M1, f"{DAY.isoformat()} 11:00:00+00"),
    )
    assert client.get(
        f"/daily-reports?date={DAY.isoformat()}"
    ).json()["stale"] is True

    from app import daily_audit as audit_mod

    def boom(*_a, **_kw):
        raise RuntimeError("模拟报告保存失败")

    monkeypatch.setattr(audit_mod.store, "commit_success", boom)
    failed = _run(client)
    assert failed["status"] == "aborted"
    assert failed["report"] is None
    assert "保存失败" in failed["error"]

    # 没有任何新结论；旧报告仍可见且保持过期
    old_again = client.get(f"/daily-reports?date={DAY.isoformat()}").json()
    assert old_again["report_id"] == old["report_id"]
    assert old_again["stale"] is True
    assert old_again["settlement_usable"] is False

    # 恢复后重查 -> 新报告可用
    monkeypatch.undo()
    ok = _run(client)["report"]
    assert ok["report_id"] != old["report_id"]
    assert ok["stale"] is False
    assert ok["settlement_usable"] is True


def test_report_save_failure_blocks_old_report_even_without_source_change(
    client: TestClient, clean_day: psycopg.Connection, monkeypatch
) -> None:
    """报告保存失败但源表未变：旧报告不能继续显示为可用。"""
    old = _run(client)["report"]
    assert old["settlement_usable"] is True

    from app import daily_audit as audit_mod

    def boom(*_a, **_kw):
        raise RuntimeError("模拟报告保存失败")

    monkeypatch.setattr(audit_mod.store, "commit_success", boom)
    failed = _run(client)
    assert failed["status"] == "aborted"
    assert failed["report"] is None

    # 源表版本没变（stale=false），但“最新一次尝试失败”必须阻断可用标记
    got = client.get(f"/daily-reports?date={DAY.isoformat()}").json()
    assert got["report_id"] == old["report_id"]
    assert got["stale"] is False
    assert got["latest_attempt_status"] == "aborted"
    assert got["usable_blocked_reason"]
    assert got["settlement_usable"] is False

    # 重查成功后才恢复
    monkeypatch.undo()
    ok = _run(client)["report"]
    assert ok["settlement_usable"] is True


def test_cancel_publishes_no_conclusion(
    client: TestClient, clean_day: psycopg.Connection, monkeypatch
) -> None:
    from app import daily_audit as audit_mod

    real_scan = audit_mod.scan_day

    def slow_scan(conn, day, **kwargs):
        time.sleep(1.0)
        return real_scan(conn, day, **kwargs)

    monkeypatch.setattr(audit_mod, "scan_day", slow_scan)
    resp = client.post(f"/daily-audits?date={DAY.isoformat()}")
    audit_id = resp.json()["audit_id"]
    cancelled = client.post(f"/daily-audits/{audit_id}/cancel")
    assert cancelled.status_code == 200
    out = _wait_audit(client, audit_id)
    assert out["status"] == "cancelled"
    assert out["report"] is None

    # 没有成功报告；最新尝试为 failed（取消/超时统一不发布结论）
    assert client.get(f"/daily-reports?date={DAY.isoformat()}").status_code == 404
    latest = client.get(f"/daily-reports/latest?date={DAY.isoformat()}").json()
    assert latest["settlement_usable"] is False
    assert latest["status"] != "succeeded"


def test_day_without_any_readings_is_not_usable(
    client: TestClient, clean_day: psycopg.Connection
) -> None:
    clean_day.execute(
        "DELETE FROM readings WHERE read_time >= %s::timestamptz "
        "AND read_time < %s::timestamptz",
        (f"{DAY.isoformat()} 00:00:00+00", f"{NEXT_DAY.isoformat()} 00:00:00+00"),
    )
    report = _run(client)["report"]
    assert report["usable"] is False
    assert report["settlement_usable"] is False
    assert report["readings_count"] == 0
    assert "没有任何读数" in report["usable_reason"]
    # 每块表都被列为当天无读数
    assert len(report["meters_without_readings"]) == report["meters_total"]


def test_concurrent_committed_change_between_scan_and_save_aborts(
    client: TestClient, clean_day: psycopg.Connection, monkeypatch
) -> None:
    """扫描事务结束后、保存结论前，另一会话提交更正 -> aborted。

    这条路径依赖真实的 ``FOR SHARE`` 版本复核：在 scan_day 返回后、
    commit_success 执行前插入一次已提交写。
    """
    from app import daily_audit as audit_mod

    fired = {"done": False}

    # 扫描事务 COMMIT 之后、保存结论之前，另一会话提交一次更正
    original_commit_success = audit_mod.store.commit_success

    def commit_with_race(*args, **kwargs):
        if not fired["done"]:
            fired["done"] = True
            clean_day.execute(
                "INSERT INTO readings (meter_id, read_time, wh) VALUES "
                "(%s, %s::timestamptz, 8888)",
                (M1, f"{DAY.isoformat()} 12:00:00+00"),
            )
        return original_commit_success(*args, **kwargs)

    monkeypatch.setattr(audit_mod.store, "commit_success", commit_with_race)
    out = _run(client)
    assert out["status"] == "aborted"
    assert out["report"] is None
    assert "版本" in out["error"]
    latest = client.get(f"/daily-reports/latest?date={DAY.isoformat()}").json()
    assert latest["status"] == "aborted"
    assert latest["settlement_usable"] is False


def test_history_lists_attempts(
    client: TestClient, clean_day: psycopg.Connection
) -> None:
    first = _run(client)["report"]
    clean_day.execute(
        "INSERT INTO readings (meter_id, read_time, wh) VALUES "
        "(%s, %s::timestamptz, 1500)",
        (M1, f"{DAY.isoformat()} 11:00:00+00"),
    )
    second = _run(client)["report"]
    history = client.get(
        f"/daily-reports/history?date={DAY.isoformat()}&limit=10"
    )
    assert history.status_code == 200
    ids = [r["report_id"] for r in history.json()]
    assert ids[0] == second["report_id"]
    assert ids[1] == first["report_id"]
    # 历史报告仍显示各自扫描时的结论与实时过期标记
    assert history.json()[0]["stale"] is False
    assert history.json()[1]["stale"] is True


def test_daily_audit_slot_busy_returns_409(
    client: TestClient, clean_day: psycopg.Connection, monkeypatch
) -> None:
    from app import daily_audit as audit_mod

    real_scan = audit_mod.scan_day

    def slow_scan(conn, day, **kwargs):
        time.sleep(0.6)
        return real_scan(conn, day, **kwargs)

    monkeypatch.setattr(audit_mod, "scan_day", slow_scan)
    first = client.post(f"/daily-audits?date={DAY.isoformat()}")
    assert first.status_code == 201
    second = client.post(f"/daily-audits?date={DAY.isoformat()}")
    assert second.status_code == 409
    assert second.json()["detail"]["active_audit_id"] == first.json()["audit_id"]
    assert _wait_audit(client, first.json()["audit_id"])["status"] == "succeeded"

    # 名额释放后可再受理
    monkeypatch.setattr(audit_mod, "scan_day", real_scan)
    assert client.post(
        f"/daily-audits?date={NEXT_DAY.isoformat()}"
    ).status_code == 201


def test_unknown_report_and_missing_date(
    client: TestClient, clean_day: psycopg.Connection
) -> None:
    assert client.get("/daily-reports/99999999").status_code == 404
    assert client.get("/daily-reports?date=2020-01-01").status_code == 404
    assert client.get(
        "/daily-reports/latest?date=2020-01-01"
    ).status_code == 404
    assert client.get("/daily-audits/nope").status_code == 404


def test_invalid_date_format_rejected(client: TestClient) -> None:
    assert client.post("/daily-audits?date=2027-13-40").status_code == 422
