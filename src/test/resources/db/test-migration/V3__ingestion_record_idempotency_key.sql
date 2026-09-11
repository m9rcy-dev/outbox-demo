-- H2-compatible version of V3
ALTER TABLE ingestion_record ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(200);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ingestion_record_idempotency_key
    ON ingestion_record (idempotency_key);
