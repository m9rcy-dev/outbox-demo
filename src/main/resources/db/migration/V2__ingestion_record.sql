-- ============================================================
-- Business table: ingestion_record
-- Represents an inbound data-ingestion record driven through the
-- DATA_INGESTION_PIPELINE (detokenise -> mastercard search -> internal
-- notes -> sms -> mq publish)
-- ============================================================
CREATE TABLE ingestion_record (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token                VARCHAR(200) NOT NULL,
    cardholder_phone     VARCHAR(30),
    status               VARCHAR(30)  NOT NULL DEFAULT 'RECEIVED',
    -- populated progressively as the pipeline runs
    pan_last4            VARCHAR(4),
    mastercard_match_id  VARCHAR(100),
    created_at           TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMP NOT NULL DEFAULT NOW()
);
