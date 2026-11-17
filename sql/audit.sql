-- ============================================================
-- 日结读数核查：审计元数据、报告表、过期检测触发器
--
-- 本文件幂等，既由 docker-entrypoint-initdb.d 在首次启动时执行，
-- 也由应用启动时执行一次，保证已有数据卷也能补齐结构。
-- ============================================================

CREATE SCHEMA IF NOT EXISTS recon;

-- 每个 UTC 日期一个变更版本号；源表任何一个已提交的
-- INSERT / DELETE / UPDATE / TRUNCATE 都会把受影响日期的版本 +1。
CREATE TABLE IF NOT EXISTS recon.reading_dates (
    audit_date date PRIMARY KEY,
    version    bigint      NOT NULL DEFAULT 1,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- 每次核查尝试一行。先落 running，成功后才改成 succeeded，
-- 因此“报告保存失败”时绝不会留下一条看似可用的旧结论。
CREATE TABLE IF NOT EXISTS recon.daily_audit_reports (
    id                  bigserial PRIMARY KEY,
    audit_date          date        NOT NULL,
    status              text        NOT NULL
        CHECK (status IN ('running', 'succeeded', 'failed', 'aborted')),
    attempt             integer     NOT NULL DEFAULT 1,
    source_version      bigint,                 -- 扫描快照看到的日期版本
    usable              boolean     NOT NULL DEFAULT false,
    usable_reason       text,
    anomaly_total       bigint,
    readings_count      bigint,
    meters_with_readings integer,
    meters_total        integer,
    max_gap_minutes     integer     NOT NULL,
    error               text,
    started_at          timestamptz NOT NULL DEFAULT now(),
    finished_at         timestamptz,
    created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_daily_reports_date_id
    ON recon.daily_audit_reports (audit_date, id DESC);

-- 当天无读数的表（不能判定为可用），随报告一起保存。
CREATE TABLE IF NOT EXISTS recon.daily_audit_missing_meters (
    report_id bigint NOT NULL
        REFERENCES recon.daily_audit_reports(id) ON DELETE CASCADE,
    meter_id  bigint NOT NULL,
    PRIMARY KEY (report_id, meter_id)
);

-- 只保存排序后的前 N 条异常（默认 20）；anomaly_total 覆盖全量计数。
-- prev_* / curr_* 是异常相邻两条读数（current_time 是 SQL 保留字）。
CREATE TABLE IF NOT EXISTS recon.daily_audit_anomalies (
    report_id  bigint  NOT NULL
        REFERENCES recon.daily_audit_reports(id) ON DELETE CASCADE,
    seq        integer NOT NULL,
    meter_id   bigint  NOT NULL,
    prev_time  timestamptz NOT NULL,
    prev_wh    bigint  NOT NULL,
    curr_time  timestamptz NOT NULL,
    curr_wh    bigint  NOT NULL,
    gap_seconds bigint NOT NULL,
    reasons    text[]  NOT NULL,
    PRIMARY KEY (report_id, seq)
);

-- ------------------------------------------------------------
-- 版本号触发器
--
-- 使用语句级 transition table，一次批量写入只按“受影响日期”
-- 各 +1，不按行放大。UPDATE 同时统计 OLD 与 NEW 的日期，
-- 因此读数从一个 UTC 日移动到另一日时，两天都会过期。
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION recon.readings_changed_ins() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO recon.reading_dates (audit_date, version, updated_at)
    SELECT d, 1, now()
    FROM (
        SELECT DISTINCT (read_time AT TIME ZONE 'UTC')::date AS d
        FROM new_rows
    ) AS changed
    ON CONFLICT (audit_date) DO UPDATE
        SET version = recon.reading_dates.version + 1,
            updated_at = now();
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION recon.readings_changed_del() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO recon.reading_dates (audit_date, version, updated_at)
    SELECT d, 1, now()
    FROM (
        SELECT DISTINCT (read_time AT TIME ZONE 'UTC')::date AS d
        FROM old_rows
    ) AS changed
    ON CONFLICT (audit_date) DO UPDATE
        SET version = recon.reading_dates.version + 1,
            updated_at = now();
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION recon.readings_changed_upd() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO recon.reading_dates (audit_date, version, updated_at)
    SELECT d, 1, now()
    FROM (
        SELECT (read_time AT TIME ZONE 'UTC')::date AS d FROM old_rows
        UNION
        SELECT (read_time AT TIME ZONE 'UTC')::date AS d FROM new_rows
    ) AS changed
    ON CONFLICT (audit_date) DO UPDATE
        SET version = recon.reading_dates.version + 1,
            updated_at = now();
    RETURN NULL;
END;
$$;

-- TRUNCATE 没有 transition table，用 BEFORE 触发器在清空之前
-- 读取仍存在的日期并逐个标记。
CREATE OR REPLACE FUNCTION recon.readings_changed_trunc() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO recon.reading_dates (audit_date, version, updated_at)
    SELECT d, 1, now()
    FROM (
        SELECT DISTINCT (read_time AT TIME ZONE 'UTC')::date AS d
        FROM readings
    ) AS changed
    ON CONFLICT (audit_date) DO UPDATE
        SET version = recon.reading_dates.version + 1,
            updated_at = now();
    RETURN NULL;
END;
$$;

-- 触发器只在缺失时创建，应用每次启动执行本文件时不再反复加锁。
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'readings_changed_ins'
          AND tgrelid = 'readings'::regclass
    ) THEN
        CREATE TRIGGER readings_changed_ins
            AFTER INSERT ON readings
            REFERENCING NEW TABLE AS new_rows
            FOR EACH STATEMENT EXECUTE FUNCTION recon.readings_changed_ins();
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'readings_changed_del'
          AND tgrelid = 'readings'::regclass
    ) THEN
        CREATE TRIGGER readings_changed_del
            AFTER DELETE ON readings
            REFERENCING OLD TABLE AS old_rows
            FOR EACH STATEMENT EXECUTE FUNCTION recon.readings_changed_del();
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'readings_changed_upd'
          AND tgrelid = 'readings'::regclass
    ) THEN
        CREATE TRIGGER readings_changed_upd
            AFTER UPDATE ON readings
            REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
            FOR EACH STATEMENT EXECUTE FUNCTION recon.readings_changed_upd();
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'readings_changed_trunc'
          AND tgrelid = 'readings'::regclass
    ) THEN
        CREATE TRIGGER readings_changed_trunc
            BEFORE TRUNCATE ON readings
            FOR EACH STATEMENT EXECUTE FUNCTION recon.readings_changed_trunc();
    END IF;
END;
$$;

-- 让按 UTC 日 + 表号顺序扫描更友好（原主键是 meter_id, read_time）。
CREATE INDEX IF NOT EXISTS idx_readings_time_meter
    ON readings (read_time, meter_id) INCLUDE (wh);
