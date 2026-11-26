-- Normalize CHAR(64) (bpchar) checksum columns to VARCHAR(64) to match JPA/Hibernate validation.
-- btrim preserves 64-hex SHA-256 digests and strips CHAR(64) trailing padding from literal 'TOMBSTONE' markers.
ALTER TABLE objects
    ALTER COLUMN checksum_sha256 TYPE VARCHAR(64) USING btrim(checksum_sha256);

ALTER TABLE replicas
    ALTER COLUMN checksum_sha256 TYPE VARCHAR(64) USING btrim(checksum_sha256);
