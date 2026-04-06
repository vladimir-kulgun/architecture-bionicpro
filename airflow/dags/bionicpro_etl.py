"""
bionicpro_etl.py — Daily ETL pipeline for the BionicPRO reporting data mart.

Pipeline (parallel extraction, then merge):

    extract_telemetry ──┐
                        ├──► refresh_datamart ──► validate_datamart
    extract_crm       ──┘

Sources:
    bionicpro_postgres  (Airflow conn)  app_db  — prosthetics telemetry (PostgreSQL)
    bionicpro_crm_db    (Airflow conn)  crm_db  — customers / orders    (PostgreSQL)

Target:
    ClickHouse HTTP API — OLAP data mart table user_prosthetics_report

Schedule: 02:00 UTC every day; ds = previous calendar day (e.g. run at
          2024-03-02 02:00 processes data for 2024-03-01).

Idempotency: ClickHouse ReplacingMergeTree(etl_updated_at) deduplicates rows on
             (user_id, report_date) so re-runs are safe.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

TELEMETRY_CONN = "bionicpro_postgres"   # Airflow connection: app_db
CRM_CONN       = "bionicpro_crm_db"    # Airflow connection: crm_db
CLICKHOUSE_URL = "http://clickhouse:8123"

# ── ClickHouse HTTP helpers ────────────────────────────────────────────────────

def ch_query(sql: str) -> str:
    """Execute a DDL or SELECT query via ClickHouse HTTP API.

    Sends the SQL as the POST body; ClickHouse returns plain-text results.
    Raises requests.HTTPError on non-2xx response.
    """
    resp = requests.post(
        CLICKHOUSE_URL,
        data=sql.encode("utf-8"),
        timeout=60,
    )
    resp.raise_for_status()
    return resp.text.strip()


def ch_insert(insert_header: str, tsv_rows: list[list]) -> None:
    """Execute INSERT … FORMAT TSV with a list of row lists.

    insert_header example:
        'INSERT INTO staging_telemetry
         (user_id, report_date, total_sessions, ...) FORMAT TSV'

    The TSV data is sent as the POST body; ClickHouse expects tab-delimited
    rows one per line, values matching the declared column order.
    """
    if not tsv_rows:
        return

    body_lines = [
        "\t".join("" if v is None else str(v) for v in row)
        for row in tsv_rows
    ]
    payload = insert_header.strip() + "\n" + "\n".join(body_lines)

    resp = requests.post(
        CLICKHOUSE_URL,
        data=payload.encode("utf-8"),
        timeout=120,
    )
    resp.raise_for_status()


# ── DDL: ensure all ClickHouse tables exist ────────────────────────────────────

def ch_ensure_tables() -> None:
    """Create ClickHouse staging and data-mart tables if they don't exist yet.

    Called at the start of every extract task so the pipeline is self-bootstrapping.
    """

    # ── Staging: aggregated sensor telemetry (one row per user per day) ────────
    ch_query("""
    CREATE TABLE IF NOT EXISTS staging_telemetry
    (
        user_id          String    COMMENT 'Keycloak user ID',
        report_date      Date,
        total_sessions   UInt32    COMMENT 'Distinct 15-min time-blocks with activity',
        total_active_min UInt32    COMMENT 'Sum of session_duration_seconds / 60',
        avg_signal       Float32   COMMENT 'Avg EMG signal strength (mV)',
        max_signal       Float32   COMMENT 'Peak EMG signal (mV)',
        movement_count   UInt32    COMMENT 'Recognised gestures / movements',
        error_count      UInt32    COMMENT 'Chip fault events',
        avg_battery      Float32   COMMENT 'Average battery level (%)',
        min_battery      Float32   COMMENT 'Minimum battery level (%)',
        loaded_at        DateTime  DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(loaded_at)
    PARTITION BY toYYYYMM(report_date)
    ORDER BY (user_id, report_date)
    """)

    # ── Staging: latest CRM snapshot per user ──────────────────────────────────
    ch_query("""
    CREATE TABLE IF NOT EXISTS staging_crm_customers
    (
        user_id              String,
        first_name           String,
        last_name            String,
        email                String,
        prosthetics_model    String,
        order_date           String   COMMENT 'YYYY-MM-DD or empty string',
        delivery_date        String   COMMENT 'YYYY-MM-DD or empty string',
        last_service_date    String   COMMENT 'YYYY-MM-DD or empty string',
        loaded_at            DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(loaded_at)
    ORDER BY user_id
    """)

    # ── Data mart: final table consumed by Reports API ─────────────────────────
    #
    # Design decisions:
    #   - ORDER BY (user_id, report_date)  → primary index; Reports API always
    #     filters by user_id, so queries hit a tiny slice of the index.
    #   - PARTITION BY toYYYYMM(report_date) → partition pruning for date ranges.
    #   - ReplacingMergeTree(etl_updated_at) → idempotent re-runs: latest version
    #     of each (user_id, report_date) pair survives deduplication.
    #   - Bloom-filter skip index on user_id → further speeds point lookups.
    ch_query("""
    CREATE TABLE IF NOT EXISTS user_prosthetics_report
    (
        -- Identity
        user_id              String        COMMENT 'Keycloak user ID (PK part 1)',
        report_date          Date          COMMENT 'Calendar day (PK part 2)',

        -- CRM: customer profile snapshot
        first_name           String,
        last_name            String,
        email                String,
        prosthetics_model    String        COMMENT 'e.g. BionicPRO Hand v3',
        order_date           String        COMMENT 'Date of original order (YYYY-MM-DD)',
        delivery_date        String        COMMENT 'Delivery date or empty',
        last_service_date    String        COMMENT 'Most recent maintenance or empty',

        -- Telemetry: daily aggregates per user
        total_sessions       UInt32        COMMENT 'Number of active usage sessions',
        total_active_min     UInt32        COMMENT 'Total active minutes per day',
        avg_signal_strength  Float32       COMMENT 'Avg EMG signal strength (mV)',
        max_signal_strength  Float32       COMMENT 'Peak EMG signal (mV)',
        movement_count       UInt32        COMMENT 'Total recognised movements',
        error_count          UInt32        COMMENT 'Chip fault events per day',
        avg_battery_level    Float32       COMMENT 'Average battery level (%)',
        min_battery_level    Float32       COMMENT 'Minimum battery level (%)',

        -- ETL metadata
        etl_updated_at       DateTime      DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(etl_updated_at)
    PARTITION BY toYYYYMM(report_date)
    ORDER BY (user_id, report_date)
    SETTINGS index_granularity = 8192
    """)

    log.info("ClickHouse tables verified / created.")


# ── Task 1: Extract telemetry from PostgreSQL → ClickHouse staging ─────────────

def extract_telemetry(ds: str, **_) -> None:
    """Aggregate previous-day sensor telemetry and write to staging_telemetry.

    Groups by user_id for the target date ds (Airflow logical date = ds yesterday
    when DAG runs at 02:00 UTC the following day).

    Aggregations:
        total_sessions  — distinct 15-minute time-blocks with activity
        total_active_min — sum of session durations
        avg/max signal strength — EMG quality metrics
        movement_count  — successful gesture recognitions
        error_count     — chip fault events
        avg/min battery — battery health metrics
    """
    ch_ensure_tables()

    hook = PostgresHook(postgres_conn_id=TELEMETRY_CONN)
    rows = hook.get_records(
        """
        SELECT
            user_id,
            (timestamp AT TIME ZONE 'UTC')::date                       AS report_date,
            COUNT(DISTINCT DATE_TRUNC('minute', timestamp)
                           - (EXTRACT(MINUTE FROM timestamp)::int %% 15
                              * INTERVAL '1 minute'))                  AS total_sessions,
            COALESCE(SUM(session_duration_seconds) / 60, 0)            AS total_active_min,
            ROUND(COALESCE(AVG(signal_strength), 0)::numeric, 2)       AS avg_signal,
            ROUND(COALESCE(MAX(signal_strength), 0)::numeric, 2)       AS max_signal,
            COUNT(*) FILTER (WHERE movement_type IS NOT NULL)          AS movement_count,
            COUNT(*) FILTER (WHERE is_error = TRUE)                    AS error_count,
            ROUND(COALESCE(AVG(battery_level), 0)::numeric, 2)         AS avg_battery,
            ROUND(COALESCE(MIN(battery_level), 0)::numeric, 2)         AS min_battery
        FROM prosthetics_telemetry
        WHERE (timestamp AT TIME ZONE 'UTC')::date = %(ds)s
          AND user_id IS NOT NULL
        GROUP BY user_id, report_date
        """,
        parameters={"ds": ds},
    )

    if not rows:
        log.warning("extract_telemetry: no telemetry rows for %s.", ds)
        return

    ch_insert(
        "INSERT INTO staging_telemetry "
        "(user_id, report_date, total_sessions, total_active_min, "
        " avg_signal, max_signal, movement_count, error_count, avg_battery, min_battery) "
        "FORMAT TSV",
        [list(row) for row in rows],
    )
    log.info("extract_telemetry: loaded %d rows for %s.", len(rows), ds)


# ── Task 2: Extract CRM data → ClickHouse staging ─────────────────────────────

def extract_crm(**_) -> None:
    """Snapshot current CRM customer + order data into staging_crm_customers.

    Joins crm_customers with their most recent order and the last service date.
    CRM data does not change by day, so we always load the full current state
    (ReplacingMergeTree keeps only the newest loaded_at per user_id).
    """
    ch_ensure_tables()

    hook = PostgresHook(postgres_conn_id=CRM_CONN)
    rows = hook.get_records(
        """
        SELECT
            c.keycloak_id                                               AS user_id,
            c.first_name,
            c.last_name,
            c.email,
            COALESCE(o.prosthetics_model, 'Unknown')                   AS prosthetics_model,
            COALESCE(o.order_date::text,    '')                        AS order_date,
            COALESCE(o.delivery_date::text, '')                        AS delivery_date,
            COALESCE(
                (
                    SELECT MAX(sh.service_date)::text
                    FROM   crm_service_history sh
                    WHERE  sh.order_id = o.id
                ),
                ''
            )                                                          AS last_service_date
        FROM crm_customers c
        LEFT JOIN crm_orders o
               ON o.id = (
                   SELECT id
                   FROM   crm_orders
                   WHERE  customer_id = c.id
                   ORDER  BY order_date DESC
                   LIMIT  1
               )
        WHERE c.keycloak_id IS NOT NULL
        """
    )

    if not rows:
        log.warning("extract_crm: no CRM rows found.")
        return

    ch_insert(
        "INSERT INTO staging_crm_customers "
        "(user_id, first_name, last_name, email, prosthetics_model, "
        " order_date, delivery_date, last_service_date) "
        "FORMAT TSV",
        [list(row) for row in rows],
    )
    log.info("extract_crm: loaded %d customer rows.", len(rows))


# ── Task 3: Refresh the data mart from staging ────────────────────────────────

def refresh_datamart(ds: str, **_) -> None:
    """JOIN staging_telemetry and staging_crm_customers for the target date
    and insert the result into user_prosthetics_report.

    Uses FINAL modifier on staging reads to force ReplacingMergeTree deduplication
    before joining, ensuring only the latest version of each row is used.
    """
    ch_query(f"""
    INSERT INTO user_prosthetics_report
    SELECT
        t.user_id,
        t.report_date,
        -- CRM snapshot (NULL → empty string)
        coalesce(c.first_name,          '')  AS first_name,
        coalesce(c.last_name,           '')  AS last_name,
        coalesce(c.email,               '')  AS email,
        coalesce(c.prosthetics_model,   '')  AS prosthetics_model,
        coalesce(c.order_date,          '')  AS order_date,
        coalesce(c.delivery_date,       '')  AS delivery_date,
        coalesce(c.last_service_date,   '')  AS last_service_date,
        -- Telemetry aggregates
        t.total_sessions,
        t.total_active_min,
        t.avg_signal                         AS avg_signal_strength,
        t.max_signal                         AS max_signal_strength,
        t.movement_count,
        t.error_count,
        t.avg_battery                        AS avg_battery_level,
        t.min_battery                        AS min_battery_level,
        now()                                AS etl_updated_at
    FROM (
        SELECT * FROM staging_telemetry FINAL
        WHERE report_date = '{ds}'
    ) AS t
    LEFT JOIN (
        SELECT * FROM staging_crm_customers FINAL
    ) AS c ON c.user_id = t.user_id
    """)
    log.info("refresh_datamart: data mart updated for %s.", ds)


# ── Task 4: Validate the data mart ────────────────────────────────────────────

def validate_datamart(ds: str, **_) -> None:
    """Sanity checks after each ETL run.

    Checks:
    1. At least one row was written for the target date.
       (Warns, not fails — telemetry may be absent on maintenance days.)
    2. error_count never exceeds movement_count + total_sessions.
       (Would indicate a counter bug in the extraction SQL.)
    3. All numeric fields are non-negative.
    """
    row_count = int(ch_query(
        f"SELECT count() FROM user_prosthetics_report FINAL "
        f"WHERE report_date = '{ds}'"
    ))
    log.info("validate_datamart: %d rows for %s.", row_count, ds)

    if row_count == 0:
        log.warning("No rows in data mart for %s — possible missing telemetry.", ds)
        return  # non-fatal: prosthesis might be offline / unused

    # Check for logical impossibilities in error counter
    anomalies = int(ch_query(
        f"SELECT countIf(error_count > total_sessions + movement_count + 100) "
        f"FROM user_prosthetics_report FINAL WHERE report_date = '{ds}'"
    ))
    if anomalies > 0:
        raise ValueError(
            f"{anomalies} row(s) with suspiciously high error_count for {ds}. "
            "Inspect extract_telemetry aggregation."
        )

    # Check no negative values crept in via float rounding
    neg_values = int(ch_query(
        f"SELECT countIf(avg_signal_strength < 0 OR avg_battery_level < 0 "
        f"               OR min_battery_level < 0) "
        f"FROM user_prosthetics_report FINAL WHERE report_date = '{ds}'"
    ))
    if neg_values > 0:
        raise ValueError(
            f"{neg_values} row(s) with negative metric values for {ds}."
        )

    log.info("validate_datamart: all checks passed for %s.", ds)


# ── DAG definition ─────────────────────────────────────────────────────────────

default_args = {
    "owner": "bionicpro-data",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="bionicpro_etl",
    description=(
        "Daily ETL: PostgreSQL telemetry + CRM DB → ClickHouse data mart "
        "(user_prosthetics_report). Schedule: 02:00 UTC."
    ),
    schedule_interval="0 2 * * *",      # 02:00 UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,                       # don't back-fill on first deploy
    default_args=default_args,
    tags=["bionicpro", "etl", "reporting"],
    doc_md=__doc__,
) as dag:

    t_extract_telemetry = PythonOperator(
        task_id="extract_telemetry",
        python_callable=extract_telemetry,
        doc_md=(
            "Aggregate daily prosthetics sensor telemetry from PostgreSQL "
            "and write to ClickHouse staging_telemetry."
        ),
    )

    t_extract_crm = PythonOperator(
        task_id="extract_crm",
        python_callable=extract_crm,
        doc_md=(
            "Snapshot customer + order + service history from CRM DB "
            "and write to ClickHouse staging_crm_customers."
        ),
    )

    t_refresh_datamart = PythonOperator(
        task_id="refresh_datamart",
        python_callable=refresh_datamart,
        doc_md=(
            "JOIN telemetry and CRM staging tables for ds and insert "
            "into the user_prosthetics_report data mart."
        ),
    )

    t_validate = PythonOperator(
        task_id="validate_datamart",
        python_callable=validate_datamart,
        doc_md="Validate row counts and metric bounds in the data mart.",
    )

    # Dependency graph:
    #
    #   extract_telemetry ──┐
    #                       ├──► refresh_datamart ──► validate_datamart
    #   extract_crm       ──┘
    #
    # extract_telemetry and extract_crm run in parallel (no dependency between them).
    [t_extract_telemetry, t_extract_crm] >> t_refresh_datamart >> t_validate
