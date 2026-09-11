-- H2-compatible version of V2 (no gen_random_uuid)
CREATE TABLE IF NOT EXISTS ingestion_record (
    id                   UUID          DEFAULT RANDOM_UUID() PRIMARY KEY,
    token                VARCHAR(200)  NOT NULL,
    cardholder_phone     VARCHAR(30),
    status               VARCHAR(30)   NOT NULL DEFAULT 'RECEIVED',
    pan_last4            VARCHAR(4),
    mastercard_match_id  VARCHAR(100),
    created_at           TIMESTAMP     NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMP     NOT NULL DEFAULT NOW()
);
