-- ============================================================
-- Business table: card_application
-- Represents a credit card application submitted by a customer
-- ============================================================
CREATE TABLE card_application (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    applicant_name  VARCHAR(200)  NOT NULL,
    email           VARCHAR(200)  NOT NULL,
    annual_income   NUMERIC(15,2) NOT NULL,
    status          VARCHAR(30)   NOT NULL DEFAULT 'SUBMITTED',
    -- populated after pipeline completes
    credit_score    INTEGER,
    provider_ref    VARCHAR(100),
    notification_id VARCHAR(100),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ============================================================
-- Outbox table: outbox_event
-- Stores pipeline execution state atomically with business data
-- ============================================================
CREATE TABLE outbox_event (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    correlation_id   UUID         NOT NULL,
    pipeline_type    VARCHAR(100) NOT NULL,
    current_step     INTEGER      NOT NULL DEFAULT 0,
    total_steps      INTEGER      NOT NULL,
    status           VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
    -- JSON context shared and mutated across pipeline steps
    payload          TEXT         NOT NULL,
    retry_count      INTEGER      NOT NULL DEFAULT 0,
    last_error       TEXT,
    -- JPA optimistic lock column; prevents concurrent pods from silently overwriting each other
    version          BIGINT       NOT NULL DEFAULT 0,
    created_at       TIMESTAMP    NOT NULL DEFAULT NOW(),
    processed_at     TIMESTAMP
);

CREATE INDEX idx_outbox_pending
    ON outbox_event (status, created_at)
    WHERE status = 'PENDING';
