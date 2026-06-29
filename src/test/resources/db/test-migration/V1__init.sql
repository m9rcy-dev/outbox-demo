-- H2-compatible migration (PostgreSQL mode)
CREATE TABLE IF NOT EXISTS card_application (
    id              UUID          DEFAULT RANDOM_UUID() PRIMARY KEY,
    applicant_name  VARCHAR(200)  NOT NULL,
    email           VARCHAR(200)  NOT NULL,
    annual_income   NUMERIC(15,2) NOT NULL,
    status          VARCHAR(30)   NOT NULL DEFAULT 'SUBMITTED',
    credit_score    INTEGER,
    provider_ref    VARCHAR(100),
    notification_id VARCHAR(100),
    created_at      TIMESTAMP     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox_event (
    id               UUID          DEFAULT RANDOM_UUID() PRIMARY KEY,
    correlation_id   UUID          NOT NULL,
    pipeline_type    VARCHAR(100)  NOT NULL,
    current_step     INTEGER       NOT NULL DEFAULT 0,
    total_steps      INTEGER       NOT NULL,
    status           VARCHAR(20)   NOT NULL DEFAULT 'PENDING',
    payload          TEXT          NOT NULL,
    retry_count      INTEGER       NOT NULL DEFAULT 0,
    last_error       TEXT,
    version          BIGINT        NOT NULL DEFAULT 0,
    created_at       TIMESTAMP     NOT NULL DEFAULT NOW(),
    processed_at     TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_event (status, created_at);
