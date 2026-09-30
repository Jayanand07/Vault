-- =============================================
-- TABLE: nodes
-- "Which computers are in our cluster?"
-- =============================================
CREATE TABLE nodes (
    node_id           UUID PRIMARY KEY,             -- unique ID for this computer
    address           VARCHAR(255) NOT NULL UNIQUE,  -- how to reach it: "vault-node-1:8080"
    status            VARCHAR(20) NOT NULL DEFAULT 'UP',
    pfd_status        VARCHAR(30) DEFAULT 'HEALTHY',
    disk_total_bytes  BIGINT DEFAULT 0,
    disk_used_bytes   BIGINT DEFAULT 0,
    object_count      BIGINT DEFAULT 0,
    last_heartbeat    TIMESTAMPTZ,
    joined_at         TIMESTAMPTZ DEFAULT NOW(),
    updated_at        TIMESTAMPTZ DEFAULT NOW(),

    CHECK (status IN ('UP', 'SUSPECT', 'DOWN', 'DECOMMISSIONED')),
    CHECK (pfd_status IN ('HEALTHY', 'WATCH', 'PREDICTED_FAILURE', 'DRAINING'))
);
