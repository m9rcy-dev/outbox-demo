-- Caller-supplied de-duplication key for DATA_INGESTION_PIPELINE submissions.
-- Plain UNIQUE is sufficient: standard SQL treats NULL as distinct from any
-- other NULL, so callers that don't supply a key (idempotency_key IS NULL)
-- never collide with each other. This unique constraint is the hard guard
-- against two pods both accepting the same duplicate request concurrently.
ALTER TABLE ingestion_record ADD COLUMN idempotency_key VARCHAR(200);

CREATE UNIQUE INDEX idx_ingestion_record_idempotency_key
    ON ingestion_record (idempotency_key);
