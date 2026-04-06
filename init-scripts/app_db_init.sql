-- ──────────────────────────────────────────────────────────────────────────────
-- app_db_init.sql — Initialise app_db (PostgreSQL) for BionicPRO.
-- Runs automatically when the postgres container starts for the first time.
--
-- Tables:
--   user_profiles        — created by bionicpro-auth BFF service (Yandex OAuth)
--   prosthetics_telemetry — time-series data written by the API service
-- ──────────────────────────────────────────────────────────────────────────────

-- ── Telemetry table (source for ETL DAG 1) ────────────────────────────────────

CREATE TABLE IF NOT EXISTS prosthetics_telemetry (
    id                     BIGSERIAL       PRIMARY KEY,
    user_id                VARCHAR(255)    NOT NULL,           -- Keycloak user ID
    timestamp              TIMESTAMPTZ     NOT NULL DEFAULT now(),
    signal_strength        FLOAT,                             -- EMG signal (mV)
    movement_type          VARCHAR(50),                       -- 'grasp','pinch','point','open'
    battery_level          FLOAT,                             -- %
    is_error               BOOLEAN         NOT NULL DEFAULT FALSE,
    error_code             VARCHAR(50),                       -- e.g. 'ERR_SIGNAL_WEAK'
    session_duration_seconds INT
);

-- Index used by the ETL query (filters on user_id + date range)
CREATE INDEX IF NOT EXISTS idx_telemetry_user_ts
    ON prosthetics_telemetry (user_id, timestamp);

-- Partial index for error analysis
CREATE INDEX IF NOT EXISTS idx_telemetry_errors
    ON prosthetics_telemetry (user_id, timestamp)
    WHERE is_error = TRUE;

-- ── Seed data ─────────────────────────────────────────────────────────────────
-- Three prothetic users, 7 days of synthetic telemetry (30 readings / day / user).
-- user_id values match the keycloak_id used in crm_db_init.sql.

INSERT INTO prosthetics_telemetry
    (user_id, timestamp, signal_strength, movement_type,
     battery_level, is_error, error_code, session_duration_seconds)
SELECT
    users.uid                                                           AS user_id,
    -- Spread 30 readings across each day at 30-minute intervals from 08:00
    (days.day + (n.n * INTERVAL '30 minutes') + INTERVAL '8 hours')
        AT TIME ZONE 'UTC'                                              AS timestamp,
    -- EMG signal: 50–100 mV with per-user variability
    ROUND((50 + RANDOM() * 50 + users.base_signal_offset)::numeric, 2) AS signal_strength,
    -- Movement type: weighted random; NULL ≈ passive wear (no gesture)
    (ARRAY['grasp','pinch','point','open',NULL,NULL])[
        floor(RANDOM() * 6 + 1)::int
    ]                                                                   AS movement_type,
    -- Battery: starts at 95–100 %, drops ~1 % per reading
    ROUND(GREATEST(0, 100 - n.n * 1.5 - RANDOM() * 5)::numeric, 1)    AS battery_level,
    -- ~2 % error rate
    (RANDOM() < 0.02)                                                   AS is_error,
    CASE WHEN RANDOM() < 0.02 THEN 'ERR_SIGNAL_WEAK' ELSE NULL END     AS error_code,
    -- Session duration: 10–40 minutes
    (600 + (RANDOM() * 1800)::int)                                      AS session_duration_seconds
FROM
    -- Three simulated prothetic users
    (VALUES
        ('kc-prothetic-001', 0.0),
        ('kc-prothetic-002', 5.0),
        ('kc-prothetic-003', -3.0)
    ) AS users(uid, base_signal_offset),
    -- Last 7 days
    generate_series(
        CURRENT_DATE - INTERVAL '7 days',
        CURRENT_DATE - INTERVAL '1 day',
        INTERVAL '1 day'
    ) AS days(day),
    -- 30 readings per day
    generate_series(0, 29) AS n(n);
