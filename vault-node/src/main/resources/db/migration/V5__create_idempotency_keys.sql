-- =============================================
-- TABLE: idempotency_keys
-- "Has this exact request already been processed?"
-- Prevents duplicate writes when a client retries after a timeout.
-- =============================================
CREATE TABLE idempotency_keys (
    key               VARCHAR(128) PRIMARY KEY,
    status            VARCHAR(16) NOT NULL DEFAULT 'IN_PROGRESS',
    response_code     INT,
    response_payload  JSONB,                         -- cached response for retries
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    expires_at        TIMESTAMPTZ DEFAULT NOW() + INTERVAL '24 hours',

    CHECK (status IN ('IN_PROGRESS', 'DONE'))
);

CREATE INDEX idx_idempotency_expires ON idempotency_keys(expires_at);
