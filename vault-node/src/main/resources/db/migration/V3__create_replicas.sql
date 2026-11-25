-- =============================================
-- TABLE: replicas
-- "Which computers have a copy of which file?"
-- =============================================
CREATE TABLE replicas (
    object_id         VARCHAR(512) NOT NULL,
    version           BIGINT NOT NULL,
    node_id           UUID NOT NULL REFERENCES nodes(node_id),
    checksum_sha256   CHAR(64) NOT NULL,
    status            VARCHAR(20) DEFAULT 'HEALTHY',
    verified_at       TIMESTAMPTZ,
    created_at        TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (object_id, version, node_id),
    -- FOREIGN KEY requires objects to have (object_id, version) as unique/PK which it does
    FOREIGN KEY (object_id, version) REFERENCES objects(object_id, version),
    CHECK (status IN ('HEALTHY', 'CORRUPT', 'MISSING', 'REPAIRING'))
);

CREATE INDEX idx_replicas_node ON replicas(node_id);
