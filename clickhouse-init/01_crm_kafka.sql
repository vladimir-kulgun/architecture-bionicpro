-- ─────────────────────────────────────────────────────────────────────────────
-- 01_crm_kafka.sql — ClickHouse CDC pipeline for CRM data via Kafka / Debezium.
--
-- Three-layer architecture per CRM table:
--
--   Kafka Engine table     ← stateless consumer; reads from a Kafka topic
--         │
--         ▼  (Materialized View — fires on every batch consumed)
--   raw_crm_*              ← ReplacingMergeTree; stores current CDC state
--
-- Message format: Debezium ExtractNewRecordState SMT, JsonConverter, no schema.
--   • PostgreSQL DATE  → integer (days since 1970-01-01)
--   • __deleted = 1    → row was deleted in the source DB
--   • __ts_ms          → source-event timestamp (ms); used as ReplacingMergeTree
--                        version so out-of-order events resolve correctly
--
-- These tables are created on ClickHouse first-start (docker-entrypoint-initdb.d).
-- Airflow staging tables (staging_crm_customers, etc.) are created by ch_ensure_tables().
-- ─────────────────────────────────────────────────────────────────────────────


-- ═══════════════════════════════════════════════════════════════════════════════
-- 1. Kafka Engine consumers
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS kafka_crm_customers
(
    id          Int32,
    keycloak_id String,
    first_name  String,
    last_name   String,
    email       String,
    phone       String,
    created_at  String,
    __deleted   UInt8,
    __op        String,
    __ts_ms     Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:9092',
    kafka_topic_list           = 'crm.public.crm_customers',
    kafka_group_name           = 'ch-crm-customers',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 10;

-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS kafka_crm_orders
(
    id                Int32,
    customer_id       Int32,
    prosthetics_model String,
    serial_number     String,
    order_date        Int32,
    delivery_date     Nullable(Int32),
    status            String,
    __deleted         UInt8,
    __op              String,
    __ts_ms           Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:9092',
    kafka_topic_list           = 'crm.public.crm_orders',
    kafka_group_name           = 'ch-crm-orders',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 10;

-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS kafka_crm_service_history
(
    id            Int32,
    order_id      Int32,
    service_date  Int32,
    service_type  String,
    technician    String,
    notes         String,
    __deleted     UInt8,
    __op          String,
    __ts_ms       Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:9092',
    kafka_topic_list           = 'crm.public.crm_service_history',
    kafka_group_name           = 'ch-crm-service-history',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 10;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 2. Target tables  (ReplacingMergeTree)
-- ═══════════════════════════════════════════════════════════════════════════════
-- ReplacingMergeTree(__ts_ms) keeps the row with the highest __ts_ms per ORDER BY key.
-- Query with FINAL to force deduplication; filter WHERE __deleted = 0 for live rows.

CREATE TABLE IF NOT EXISTS raw_crm_customers
(
    id          Int32,
    keycloak_id String,
    first_name  String  DEFAULT '',
    last_name   String  DEFAULT '',
    email       String  DEFAULT '',
    phone       String  DEFAULT '',
    created_at  String  DEFAULT '',
    __deleted   UInt8   DEFAULT 0,
    __ts_ms     Int64   DEFAULT 0
)
ENGINE = ReplacingMergeTree(__ts_ms)
ORDER BY id;

-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS raw_crm_orders
(
    id                Int32,
    customer_id       Int32,
    prosthetics_model String          DEFAULT '',
    serial_number     String          DEFAULT '',
    order_date        Int32           DEFAULT 0,
    delivery_date     Nullable(Int32),
    status            String          DEFAULT '',
    __deleted         UInt8           DEFAULT 0,
    __ts_ms           Int64           DEFAULT 0
)
ENGINE = ReplacingMergeTree(__ts_ms)
ORDER BY id;

-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS raw_crm_service_history
(
    id            Int32,
    order_id      Int32,
    service_date  Int32   DEFAULT 0,
    service_type  String  DEFAULT '',
    technician    String  DEFAULT '',
    notes         String  DEFAULT '',
    __deleted     UInt8   DEFAULT 0,
    __ts_ms       Int64   DEFAULT 0
)
ENGINE = ReplacingMergeTree(__ts_ms)
ORDER BY id;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 3. Materialized Views  (Kafka Engine → ReplacingMergeTree)
-- ═══════════════════════════════════════════════════════════════════════════════
-- Each MV fires automatically whenever ClickHouse polls the Kafka topic.
-- coalesce() guards against unexpected NULLs in non-nullable target columns.

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_crm_customers
TO raw_crm_customers AS
SELECT
    id,
    keycloak_id,
    coalesce(first_name, '') AS first_name,
    coalesce(last_name,  '') AS last_name,
    coalesce(email,      '') AS email,
    coalesce(phone,      '') AS phone,
    coalesce(created_at, '') AS created_at,
    __deleted,
    __ts_ms
FROM kafka_crm_customers;

-- ─────────────────────────────────────────────────────────────────────────────

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_crm_orders
TO raw_crm_orders AS
SELECT
    id,
    customer_id,
    coalesce(prosthetics_model, '') AS prosthetics_model,
    coalesce(serial_number,     '') AS serial_number,
    order_date,
    delivery_date,
    coalesce(status, '')            AS status,
    __deleted,
    __ts_ms
FROM kafka_crm_orders;

-- ─────────────────────────────────────────────────────────────────────────────

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_crm_service_history
TO raw_crm_service_history AS
SELECT
    id,
    order_id,
    service_date,
    coalesce(service_type, '') AS service_type,
    coalesce(technician,   '') AS technician,
    coalesce(notes,        '') AS notes,
    __deleted,
    __ts_ms
FROM kafka_crm_service_history;
