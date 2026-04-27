#!/usr/bin/env python3
"""
Seed the BionicPRO reporting data mart on first startup.

Steps:
  1. If user_prosthetics_report already has >= 3 rows → skip (already seeded).
  2. Wait for ClickHouse to respond.
  3. Wait for CDC snapshot (raw_crm_customers >= 3 rows).
  4. Unpause the bionicpro_etl Airflow DAG.
  5. Delete any stale DAG run for yesterday (avoids exec-date unique constraint).
  6. Trigger one DAG run for yesterday's date.
  7. Wait up to 10 minutes for the run to succeed.
"""
import os, sys, time, subprocess
from datetime import date, datetime, timedelta, timezone

import requests
import psycopg2

CH_URL     = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123/")
AIRFLOW_DB = "host=airflow_db dbname=airflow user=airflow password=airflow"


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def ch(sql: str) -> str:
    r = requests.post(CH_URL, data=sql.encode(), timeout=60)
    r.raise_for_status()
    return r.text.strip()


def wait_for(label: str, check_fn, retries: int = 60, interval: int = 5) -> bool:
    for i in range(retries):
        try:
            if check_fn():
                log(f"  {label}: ready")
                return True
        except Exception:
            pass
        log(f"  {label}: attempt {i + 1}/{retries}…")
        time.sleep(interval)
    return False


# ── 0. Skip if already seeded ─────────────────────────────────────────────────
try:
    if int(ch("SELECT count() FROM user_prosthetics_report FINAL")) >= 3:
        log("Data mart already seeded — skipping.")
        sys.exit(0)
except Exception:
    pass

log("=== BionicPRO data-seed starting ===")

# ── 1. Wait for ClickHouse ─────────────────────────────────────────────────────
wait_for("ClickHouse", lambda: ch("SELECT 1") == "1", retries=30, interval=3)

# ── 2. Wait for CDC snapshot ───────────────────────────────────────────────────
log("Waiting for CDC snapshot (raw_crm_customers >= 3)…")
if not wait_for("raw_crm_customers",
                lambda: int(ch("SELECT count() FROM raw_crm_customers FINAL")) >= 3,
                retries=72, interval=5):
    log("WARN: CDC snapshot timed out after 6 min; proceeding anyway.")

# ── 3. Unpause DAG ─────────────────────────────────────────────────────────────
log("Unpausing bionicpro_etl…")
subprocess.run(["airflow", "dags", "unpause", "bionicpro_etl"],
               capture_output=True)

# ── 4. Clean up any stale run for yesterday ────────────────────────────────────
yesterday = (date.today() - timedelta(days=1)).isoformat()
run_id    = f"seed_{yesterday}"

try:
    conn = psycopg2.connect(AIRFLOW_DB)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM dag_run "
            "WHERE dag_id = 'bionicpro_etl' AND execution_date::text LIKE %s",
            (f"{yesterday}%",),
        )
    conn.close()
    log(f"Cleaned up stale DAG runs for {yesterday}.")
except Exception as e:
    log(f"DB cleanup skipped: {e}")

# ── 5. Trigger DAG run (retry until scheduler serializes the DAG) ──────────────
# dags list uses the dag table (written early); dags trigger needs serialized_dag
# (written slightly later). Retry on DagNotFound until the DAG is fully ready.
log(f"Triggering bionicpro_etl for {yesterday} (run-id: {run_id})…")
for attempt in range(60):   # up to 5 minutes
    result = subprocess.run(
        ["airflow", "dags", "trigger", "bionicpro_etl",
         "--run-id", run_id,
         "--conf", "{}",
         "--exec-date", f"{yesterday}T02:00:00+00:00"],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        log("  Triggered successfully.")
        # Force-unpause via DB — the CLI unpause may have run before the scheduler
        # created the DagModel entry (which defaults to is_paused=True).
        try:
            conn2 = psycopg2.connect(AIRFLOW_DB)
            conn2.autocommit = True
            with conn2.cursor() as cur2:
                cur2.execute("UPDATE dag SET is_paused = FALSE WHERE dag_id = 'bionicpro_etl'")
            conn2.close()
            log("  DAG unpaused (DB).")
        except Exception as e:
            log(f"  WARN: could not force-unpause via DB: {e}")
        break
    combined = result.stderr + result.stdout
    if "DagNotFound" in combined or "initialize the database" in combined or "db init" in combined:
        log(f"  Airflow not ready yet ({attempt+1}/60), retrying in 5s…")
        time.sleep(5)
        continue
    if "already exists" in combined:
        log("  Run already exists — continuing.")
        break
    log(f"WARN: trigger exit {result.returncode}: {combined[-400:]}")
    break
else:
    log("ERROR: Could not trigger DAG after 5 minutes — check scheduler.")
    sys.exit(1)

# ── 6. Wait for the run to succeed (up to 10 minutes) ─────────────────────────
log(f"Waiting for {run_id} to complete…")
for i in range(120):
    out = subprocess.run(
        ["airflow", "dags", "list-runs", "-d", "bionicpro_etl", "--output", "plain"],
        capture_output=True, text=True,
    ).stdout
    for line in out.splitlines():
        if run_id in line:
            parts = line.split()
            state = parts[2] if len(parts) > 2 else ""
            log(f"  state={state or '?'}")
            if state == "success":
                break
            if state in ("failed", "upstream_failed"):
                log(f"DAG run {state}. Check Airflow UI at http://localhost:8082.")
                sys.exit(1)
    else:
        time.sleep(5)
        continue
    break

# ── 7. Verify ──────────────────────────────────────────────────────────────────
try:
    rows = int(ch("SELECT count() FROM user_prosthetics_report FINAL"))
    log(f"=== Seed complete: user_prosthetics_report has {rows} row(s). ===")
except Exception as e:
    log(f"WARN: could not verify data mart: {e}")
