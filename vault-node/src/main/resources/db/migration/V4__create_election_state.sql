-- =============================================
-- TABLE: election_state
-- "Who is the leader right now?"
-- Only ONE row ever exists in this table (the CHECK enforces it).
-- =============================================
CREATE TABLE election_state (
    id                INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),  -- singleton
    current_term      BIGINT NOT NULL DEFAULT 0,     -- increments with each election
    leader_id         UUID REFERENCES nodes(node_id),
    lease_expires     TIMESTAMPTZ,                   -- leader must renew before this
    updated_at        TIMESTAMPTZ DEFAULT NOW()
);

-- Seed the singleton row
INSERT INTO election_state (id, current_term) VALUES (1, 0);
