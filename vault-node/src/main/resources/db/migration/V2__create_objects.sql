-- =============================================
-- TABLE: objects
-- "What files exist and what version are they at?"
--
-- IMPORTANT: This is APPEND-ONLY. Each write creates a NEW row.
-- Old versions stay in the table until garbage collection.
-- This means a failed write NEVER destroys the previous good version.
-- =============================================
CREATE TABLE objects (
    object_id         VARCHAR(512) NOT NULL,
    version           BIGINT NOT NULL,
    size_bytes        BIGINT NOT NULL,
    checksum_sha256   CHAR(64) NOT NULL,
    content_type      VARCHAR(255) DEFAULT 'application/octet-stream',
    durability_tier   VARCHAR(20) DEFAULT 'STANDARD',
    term              BIGINT NOT NULL,               -- which leader's reign
    status            VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    created_at        TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (object_id, version),                -- composite key!
    CHECK (status IN ('PENDING', 'ACTIVE', 'FAILED', 'TOMBSTONED'))
);

-- Fast lookup: "give me the latest working version of this file"
CREATE INDEX idx_objects_latest ON objects(object_id, version DESC)
    WHERE status IN ('ACTIVE', 'TOMBSTONED');

-- Fast cleanup: find old FAILED and TOMBSTONED rows
CREATE INDEX idx_objects_gc ON objects(status, created_at)
    WHERE status IN ('FAILED', 'TOMBSTONED');
