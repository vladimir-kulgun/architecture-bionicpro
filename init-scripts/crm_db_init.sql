-- ──────────────────────────────────────────────────────────────────────────────
-- crm_db_init.sql — Initialise crm_db (PostgreSQL, simulating CRM Oracle DB).
-- Runs automatically when the crm_db container starts for the first time.
--
-- Tables:
--   crm_customers         — customer registry (keycloak_id links to Keycloak)
--   crm_orders            — prosthetics orders per customer
--   crm_service_history   — maintenance / repair events per order
-- ──────────────────────────────────────────────────────────────────────────────

-- ── Customers ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS crm_customers (
    id           SERIAL       PRIMARY KEY,
    keycloak_id  VARCHAR(255) UNIQUE NOT NULL,  -- FK to Keycloak user ID
    first_name   VARCHAR(255),
    last_name    VARCHAR(255),
    email        VARCHAR(255),
    phone        VARCHAR(50),
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- ── Orders ────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS crm_orders (
    id                 SERIAL       PRIMARY KEY,
    customer_id        INT          NOT NULL REFERENCES crm_customers(id),
    prosthetics_model  VARCHAR(255) NOT NULL,   -- e.g. 'BionicPRO Hand v3'
    serial_number      VARCHAR(255) UNIQUE,
    order_date         DATE         NOT NULL,
    delivery_date      DATE,
    status             VARCHAR(50)  NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending','delivered','service','returned'))
);

CREATE INDEX IF NOT EXISTS idx_orders_customer
    ON crm_orders (customer_id, order_date DESC);

-- ── Service history ───────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS crm_service_history (
    id            SERIAL       PRIMARY KEY,
    order_id      INT          NOT NULL REFERENCES crm_orders(id),
    service_date  DATE         NOT NULL,
    service_type  VARCHAR(100) NOT NULL
                  CHECK (service_type IN
                      ('calibration','repair','software_update','inspection','replacement')),
    technician    VARCHAR(255),
    notes         TEXT
);

CREATE INDEX IF NOT EXISTS idx_service_order
    ON crm_service_history (order_id, service_date DESC);

-- ── Seed data ─────────────────────────────────────────────────────────────────
-- keycloak_id values must match those used in app_db prosthetics_telemetry.

INSERT INTO crm_customers (keycloak_id, first_name, last_name, email, phone) VALUES
    ('kc-prothetic-001', 'Александр', 'Иванов',   'prothetic1@example.com', '+7-900-001-01-01'),
    ('kc-prothetic-002', 'Мария',     'Петрова',  'prothetic2@example.com', '+7-900-002-02-02'),
    ('kc-prothetic-003', 'Дмитрий',   'Сидоров',  'prothetic3@example.com', '+7-900-003-03-03');

INSERT INTO crm_orders
    (customer_id, prosthetics_model, serial_number, order_date, delivery_date, status)
VALUES
    (1, 'BionicPRO Hand v3',  'BPH3-001-2023', '2023-01-15', '2023-02-01', 'delivered'),
    (2, 'BionicPRO Arm v2',   'BPA2-002-2023', '2023-03-10', '2023-04-15', 'delivered'),
    (3, 'BionicPRO Hand v4',  'BPH4-003-2023', '2023-06-01', '2023-07-10', 'delivered');

INSERT INTO crm_service_history (order_id, service_date, service_type, technician, notes)
VALUES
    (1, '2023-08-15', 'calibration',    'Техник А. Смирнов',
        'Плановая калибровка ЭМГ-датчиков через 6 месяцев эксплуатации.'),
    (1, '2024-01-10', 'repair',         'Техник А. Смирнов',
        'Замена модуля ЭМГ-сенсора — снижение чувствительности сигнала ниже порога.'),
    (2, '2023-10-20', 'calibration',    'Техник О. Козлова',
        'Плановая калибровка и проверка механики локтевого сустава.'),
    (2, '2024-03-05', 'inspection',     'Техник О. Козлова',
        'Внеплановая проверка после жалоб на вибрацию при нагрузке.'),
    (3, '2024-02-15', 'software_update','Инженер П. Новиков',
        'Обновление прошивки до v2.3.1 — улучшенный алгоритм распознавания жестов.'),
    (3, '2024-06-01', 'calibration',    'Техник О. Козлова',
        'Плановая годовая калибровка.');
