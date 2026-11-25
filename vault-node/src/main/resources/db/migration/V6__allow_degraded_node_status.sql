-- Allow FailurePredictionEngine's DEGRADED state in nodes.status CHECK constraint
DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    SELECT con.conname INTO constraint_name
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    WHERE rel.relname = 'nodes'
      AND con.contype = 'c'
      AND pg_get_constraintdef(con.oid) ILIKE '%status%'
      AND pg_get_constraintdef(con.oid) NOT ILIKE '%pfd_status%'
    LIMIT 1;

    IF constraint_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE nodes DROP CONSTRAINT %I', constraint_name);
    END IF;
END $$;

ALTER TABLE nodes
    ADD CONSTRAINT nodes_status_check
    CHECK (status IN ('UP', 'DEGRADED', 'SUSPECT', 'DOWN', 'DECOMMISSIONED'));
