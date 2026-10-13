-- ============================================================
-- E-Commerce Data Pipeline | Source Database Setup
-- Datenbank: ecom_db
-- Zielobjekt: ecommerce.customers
-- Ausführen mit: psql -h localhost -U ecom_user -d ecom_db -f ...
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- 1. Schema (Trennung von Airflow-Metadaten und Business-Daten)
-- ------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS ecommerce;

-- ------------------------------------------------------------
-- 2. Kundentabelle
--    IDENTITY ist der moderne Nachfolger von SERIAL (SQL-Standard)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ecommerce.customers (
    customer_id         INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name                VARCHAR(100)  NOT NULL,
    email               VARCHAR(150)  NOT NULL,
    phone               VARCHAR(20),
    registration_source VARCHAR(50)   NOT NULL DEFAULT 'webshop',
    status              VARCHAR(20)   NOT NULL DEFAULT 'active',
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMPTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Data Quality direkt in der Quelle erzwingen
    CONSTRAINT uq_customers_email UNIQUE (email),
    CONSTRAINT chk_customers_status
        CHECK (status IN ('active', 'inactive', 'blocked')),
    CONSTRAINT chk_customers_source
        CHECK (registration_source IN ('webshop', 'app', 'instagram', 'affiliate'))
);

COMMENT ON TABLE ecommerce.customers IS
    'Quelle der Kundendaten. Wird von AWS Lambda extrahiert und als JSON nach S3 geschrieben.';

-- ------------------------------------------------------------
-- 3. Index für den Incremental Load
--    Lambda filtert: WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
--    Ohne diesen Index = Full Table Scan
-- ------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_customers_created_at
    ON ecommerce.customers (created_at);

-- Index für Joins / lookups per Customer (PK-Index existiert bereits,
-- dieser hier unterstützt Filterungen auf status + created_at)
CREATE INDEX IF NOT EXISTS idx_customers_status_created
    ON ecommerce.customers (status, created_at);

-- ------------------------------------------------------------
-- 4. Trigger: updated_at automatisch pflegen
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION ecommerce.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_customers_updated_at ON ecommerce.customers;

CREATE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON ecommerce.customers
    FOR EACH ROW
    EXECUTE FUNCTION ecommerce.set_updated_at();

-- ------------------------------------------------------------
-- 5. Testdaten
--    NOW() - INTERVAL sorgt dafür, dass IMMER Daten im
--    Incremental-Fenster liegen (wichtig für den Demo-DAG!)
--    ON CONFLICT macht das Skript idempotent.
-- ------------------------------------------------------------
INSERT INTO ecommerce.customers
    (name, email, phone, registration_source, status, created_at)
VALUES
    ('Max Mustermann',  'max.mustermann@example.com',   '+4915112345678', 'webshop',   'active',   NOW() - INTERVAL '5 days'),
    ('Erika Musterfrau','erika.musterfrau@example.de',  '+4917698765432', 'app',       'active',   NOW() - INTERVAL '3 days'),
    ('Thomas Müller',   'thomas.mueller@example.com',   '+4916011223344', 'instagram', 'active',   NOW() - INTERVAL '2 days'),
    ('Sabine Schmidt',  'sabine.schmidt@example.net',   NULL,             'webshop',   'inactive', NOW() - INTERVAL '10 days'),
    ('Klaus Fischer',   'klaus.fischer@example.org',    '+4915222334455', 'webshop',   'active',   NOW() - INTERVAL '1 day'),
    ('Julia Weber',     'julia.weber@example.com',      '+491711234567',  'app',       'active',   NOW() - INTERVAL '12 hours'),
    ('Michael Wagner',  'michael.wagner@example.de',    '+491629876543',  'webshop',   'blocked',  NOW() - INTERVAL '6 hours'),
    ('Anna Becker',     'anna.becker@example.com',      '+4915155667788', 'webshop',   'active',   NOW() - INTERVAL '2 hours'),
    ('David Hoffmann',  'david.hoffmann@example.com',   '+4917099887766', 'affiliate', 'active',   NOW() - INTERVAL '30 minutes'),
    ('Laura Klein',     'laura.klein@example.de',       NULL,             'app',       'active',   NOW() - INTERVAL '10 minutes')
ON CONFLICT (email) DO NOTHING;

-- ------------------------------------------------------------
-- 6. Berechtigungen
--    Schema gehört ggf. postgres -> USAGE für ecom_user nötig
-- ------------------------------------------------------------
GRANT USAGE ON SCHEMA ecommerce TO ecom_user;
GRANT SELECT, INSERT, UPDATE, DELETE
    ON ALL TABLES IN SCHEMA ecommerce TO ecom_user;
GRANT USAGE, SELECT
    ON ALL SEQUENCES IN SCHEMA ecommerce TO ecom_user;

-- ------------------------------------------------------------
-- 7. OPTIONAL: Least-Privilege-User NUR für AWS Lambda
--    Entferne die Kommentare, wenn du Lambda mit einem
--    dedizierten Read-Only-User betreiben willst (Empfehlung!)
-- ------------------------------------------------------------
-- CREATE USER lambda_reader WITH PASSWORD 'EinAnderesSicheresPasswort!';
-- GRANT CONNECT ON DATABASE ecom_db TO lambda_reader;
-- GRANT USAGE ON SCHEMA ecommerce TO lambda_reader;
-- GRANT SELECT ON ecommerce.customers TO lambda_reader;

COMMIT;
