#!/usr/bin/env bash
# reseed.sh — Reseed telemetry data and rebuild the ClickHouse data mart.
#
# Run this after a fresh "docker compose up" or whenever you need to reload
# the last 7 days of telemetry into the reporting pipeline.
#
# Steps:
#   0. Wait for all required services to be healthy.
#   1. Register Debezium connector if not running; wait for CDC snapshot.
#   2. Truncate and re-seed prosthetics_telemetry in app_db (today-7..today-1).
#   3. Truncate ClickHouse staging + data-mart tables.
#   4. Unpause bionicpro_etl and trigger one DAG run per day (sequential).
#   5. Poll until user_prosthetics_report has >= 18 rows (3 users × 6+ days).
#
# Usage (from repo root):
#   bash scripts/reseed.sh

set -euo pipefail

COMPOSE="docker compose"
CH_URL="http://localhost:8123"
CONNECT_URL="http://localhost:8083"
APP_DB_SERVICE="app_db"
AIRFLOW_SERVICE="airflow-webserver"
CONNECTOR_NAME="crm-postgres-connector"
CONNECTOR_FILE="debezium/crm-connector.json"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}[reseed]${NC} $*"; }
warn()  { echo -e "${YELLOW}[reseed]${NC} $*"; }
die()   { echo -e "${RED}[reseed] ERROR:${NC} $*" >&2; exit 1; }

# ── 0a. Wait for Kafka Connect ────────────────────────────────────────────────
info "Waiting for Kafka Connect..."
for i in $(seq 1 30); do
  if curl -sf "$CONNECT_URL/connectors" > /dev/null 2>&1; then
    info "Kafka Connect is ready."
    break
  fi
  [[ $i -eq 30 ]] && die "Kafka Connect did not become ready in time."
  echo "  ... attempt $i/30"
  sleep 5
done

# ── 0b. Wait for ClickHouse ───────────────────────────────────────────────────
info "Waiting for ClickHouse..."
for i in $(seq 1 20); do
  if curl -sf "$CH_URL" --data "SELECT 1" > /dev/null 2>&1; then
    info "ClickHouse is ready."
    break
  fi
  [[ $i -eq 20 ]] && die "ClickHouse did not become ready in time."
  sleep 3
done

# ── 0c. Wait for Airflow webserver ────────────────────────────────────────────
info "Waiting for Airflow..."
for i in $(seq 1 20); do
  if $COMPOSE exec -T "$AIRFLOW_SERVICE" airflow dags list > /dev/null 2>&1; then
    info "Airflow is ready."
    break
  fi
  [[ $i -eq 20 ]] && die "Airflow did not become ready in time."
  sleep 5
done

# ── 1. Debezium connector: register if missing, wait for RUNNING ──────────────
EXISTING=$(curl -sf "$CONNECT_URL/connectors" 2>/dev/null | tr -d '[]" ' | tr ',' '\n' | grep -c "$CONNECTOR_NAME" || true)

if [[ "$EXISTING" -eq 0 ]]; then
  info "Registering Debezium connector..."
  # Retry until Kafka Connect accepts the registration (worker may still be
  # reading internal topics on a fresh start).
  for i in $(seq 1 20); do
    CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$CONNECT_URL/connectors" \
      -H "Content-Type: application/json" \
      -d @"$CONNECTOR_FILE" 2>/dev/null)
    if [[ "$CODE" == "201" || "$CODE" == "409" ]]; then
      info "Connector registered (HTTP $CODE)."
      break
    fi
    [[ $i -eq 20 ]] && die "Failed to register connector after $i attempts (last HTTP $CODE)."
    echo "  ... HTTP $CODE, retrying in 8s (attempt $i/20)"
    sleep 8
  done
else
  info "Connector '$CONNECTOR_NAME' already registered."
fi

# Wait for connector to reach RUNNING state
info "Waiting for connector to be RUNNING..."
for i in $(seq 1 30); do
  STATE=$(curl -sf "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" 2>/dev/null \
    | grep -o '"state":"[^"]*"' | head -1 | cut -d'"' -f4)
  if [[ "$STATE" == "RUNNING" ]]; then
    info "Connector is RUNNING."
    break
  fi
  [[ $i -eq 30 ]] && die "Connector did not reach RUNNING state (last state: $STATE)."
  echo "  ... state='$STATE', waiting 5s (attempt $i/30)"
  sleep 5
done

# Wait for CDC snapshot: raw_crm_customers should have 3 rows
info "Waiting for CDC snapshot (raw_crm_customers >= 3 rows)..."
for i in $(seq 1 30); do
  COUNT=$(curl -sf "$CH_URL" --data "SELECT count() FROM raw_crm_customers FINAL" 2>/dev/null | tr -d '[:space:]')
  if [[ "${COUNT:-0}" -ge 3 ]]; then
    info "CDC snapshot complete: raw_crm_customers has ${COUNT} rows."
    break
  fi
  [[ $i -eq 30 ]] && warn "CDC snapshot timed out (${COUNT:-0} rows). CRM data may be incomplete."
  echo "  ... ${COUNT:-0}/3 rows, waiting 5s (attempt $i/30)"
  sleep 5
done

# ── 2. Re-seed PostgreSQL telemetry ──────────────────────────────────────────
info "Truncating prosthetics_telemetry in app_db..."
$COMPOSE exec -T "$APP_DB_SERVICE" \
  psql -U bionicpro_user -d bionicpro -c "TRUNCATE prosthetics_telemetry RESTART IDENTITY;"

info "Inserting 7 days of fresh telemetry (relative to today)..."
$COMPOSE exec -T "$APP_DB_SERVICE" \
  psql -U bionicpro_user -d bionicpro <<'EOSQL'
INSERT INTO prosthetics_telemetry
    (user_id, timestamp, signal_strength, movement_type,
     battery_level, is_error, error_code, session_duration_seconds)
SELECT
    users.uid                                                           AS user_id,
    (days.day + (n.n * INTERVAL '30 minutes') + INTERVAL '8 hours')
        AT TIME ZONE 'UTC'                                              AS timestamp,
    ROUND((50 + RANDOM() * 50 + users.base_signal_offset)::numeric, 2) AS signal_strength,
    (ARRAY['grasp','pinch','point','open',NULL,NULL])[
        floor(RANDOM() * 6 + 1)::int
    ]                                                                   AS movement_type,
    ROUND(GREATEST(0, 100 - n.n * 1.5 - RANDOM() * 5)::numeric, 1)    AS battery_level,
    (RANDOM() < 0.02)                                                   AS is_error,
    CASE WHEN RANDOM() < 0.02 THEN 'ERR_SIGNAL_WEAK' ELSE NULL END     AS error_code,
    (600 + (RANDOM() * 1800)::int)                                      AS session_duration_seconds
FROM
    (VALUES
        ('3ca00e3e-b8d4-40b6-842f-4a111542e13d', 0.0),
        ('f992e346-c37e-4a8a-a5be-6920ade928f0', 5.0),
        ('31b18f98-2395-4d00-a492-0b3fa5900b99', -3.0)
    ) AS users(uid, base_signal_offset),
    generate_series(
        CURRENT_DATE - INTERVAL '7 days',
        CURRENT_DATE - INTERVAL '1 day',
        INTERVAL '1 day'
    ) AS days(day),
    generate_series(0, 29) AS n(n);
EOSQL

SEEDED=$($COMPOSE exec -T "$APP_DB_SERVICE" \
  psql -U bionicpro_user -d bionicpro -tAc "SELECT COUNT(*) FROM prosthetics_telemetry;")
info "app_db: ${SEEDED} telemetry rows."

# ── 3. Truncate ClickHouse staging + data-mart ────────────────────────────────
for TBL in staging_telemetry staging_crm_customers user_prosthetics_report; do
  curl -sf "$CH_URL" --data "TRUNCATE TABLE IF EXISTS $TBL" \
    || warn "Could not truncate $TBL (may not exist yet — OK on first run)."
  info "Truncated $TBL."
done

# ── 4. Compute dates, unpause DAG, trigger runs ───────────────────────────────
mapfile -t DATES < <(
  $COMPOSE exec -T "$AIRFLOW_SERVICE" python3 -c "
from datetime import date, timedelta
today = date.today()
for i in range(7, 0, -1):
    print((today - timedelta(days=i)).isoformat())
"
)
info "Dates to process: ${DATES[*]}"

info "Unpausing bionicpro_etl..."
$COMPOSE exec -T "$AIRFLOW_SERVICE" airflow dags unpause bionicpro_etl

RUN_TS=$(date +%s)

for DS in "${DATES[@]}"; do
  RUN_ID="reseed_${DS}_${RUN_TS}"
  # Remove any existing DAG run with this exec-date so we can re-trigger.
  $COMPOSE exec -T airflow_db \
    psql -U airflow airflow -c \
    "DELETE FROM dag_run WHERE dag_id='bionicpro_etl' AND execution_date='${DS}T02:00:00+00:00';" \
    > /dev/null 2>&1 || true

  info "Triggering DAG for $DS (run-id: $RUN_ID)..."
  $COMPOSE exec -T "$AIRFLOW_SERVICE" \
    airflow dags trigger bionicpro_etl \
      --run-id "$RUN_ID" \
      --conf "{}" \
      --exec-date "${DS}T02:00:00+00:00"

  info "Waiting for ${RUN_ID}..."
  for i in $(seq 1 60); do
    STATE=$(
      $COMPOSE exec -T "$AIRFLOW_SERVICE" \
        airflow dags list-runs -d bionicpro_etl --output plain 2>/dev/null \
        | awk -v rid="$RUN_ID" '$0 ~ rid { print $4 }' | head -1
    )
    if [[ "$STATE" == "success" ]]; then
      info "  ${RUN_ID} succeeded."
      break
    elif [[ "$STATE" == "failed" ]]; then
      warn "  ${RUN_ID} FAILED — continuing."
      break
    fi
    [[ $i -eq 60 ]] && warn "  Timed out waiting for ${RUN_ID} (state='${STATE}')."
    sleep 5
  done
done

# ── 5. Poll until data mart has data ─────────────────────────────────────────
info "Polling user_prosthetics_report..."
for i in $(seq 1 30); do
  COUNT=$(curl -sf "$CH_URL" \
    --data "SELECT count() FROM user_prosthetics_report FINAL" 2>/dev/null | tr -d '[:space:]')
  if [[ "${COUNT:-0}" -ge 18 ]]; then
    info "Done! user_prosthetics_report has ${COUNT} rows."
    break
  fi
  [[ $i -eq 30 ]] && { warn "Timed out. Row count: ${COUNT:-0}. Check: docker compose logs airflow-scheduler"; exit 1; }
  echo "  ... ${COUNT:-0} rows, waiting 10s (attempt $i/30)"
  sleep 10
done

info "Reseed complete."
