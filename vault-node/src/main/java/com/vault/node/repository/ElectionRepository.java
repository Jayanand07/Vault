package com.vault.node.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public class ElectionRepository {

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Value("${vault.election.lease-duration-ms:5000}")
    private long leaseDurationMs = 5000L;

    /**
     * CLAIM: "The old leader's lease expired. I want to be the new leader."
     * Only succeeds if the lease has actually expired or is null.
     */
    public Long tryClaimLeadership(UUID myNodeId) {
        String sql = """
                UPDATE election_state
                SET leader_id = :myNodeId,
                    current_term = current_term + 1,
                    lease_expires = now() + (:leaseDurationMs * interval '1 millisecond'),
                    updated_at = now()
                WHERE (lease_expires < now() OR lease_expires IS NULL)
                  AND id = 1
                RETURNING current_term
                """;

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("myNodeId", myNodeId)
                .addValue("leaseDurationMs", leaseDurationMs);

        try {
            return jdbcTemplate.queryForObject(sql, params, Long.class);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            // No row returned -> someone else already claimed it or lease not expired
            return null;
        }
    }

    /**
     * RENEW: "I'm already the leader. Extend my lease."
     * Only succeeds if I'm still the leader and lease hasn't expired.
     */
    public Long renewLease(UUID myNodeId) {
        String sql = """
                UPDATE election_state
                SET lease_expires = now() + (:leaseDurationMs * interval '1 millisecond'),
                    updated_at = now()
                WHERE leader_id = :myNodeId
                  AND lease_expires >= now()
                  AND id = 1
                RETURNING current_term
                """;

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("myNodeId", myNodeId)
                .addValue("leaseDurationMs", leaseDurationMs);

        try {
            return jdbcTemplate.queryForObject(sql, params, Long.class);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            // Lost the lease!
            return null;
        }
    }

    /**
     * FENCING CHECK: Acquires the election_state singleton row lock in the enclosing
     * transaction and verifies that {@code expectedLeaderId} still owns an unexpired
     * lease for {@code expectedTerm}.
     */
    public boolean verifyAndFenceLeaderLease(UUID expectedLeaderId, long expectedTerm) {
        if (expectedLeaderId == null) {
            return false;
        }
        String sql = """
                UPDATE election_state
                SET updated_at = CURRENT_TIMESTAMP
                WHERE id = 1
                  AND leader_id = :leaderId
                  AND current_term = :term
                  AND lease_expires >= CURRENT_TIMESTAMP
                """;
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("leaderId", expectedLeaderId)
                .addValue("term", expectedTerm);
        return jdbcTemplate.update(sql, params) == 1;
    }

    /**
     * READ current state (used on startup or to see who the leader is)
     */
    public ElectionStateRow getElectionState() {
        String sql = "SELECT leader_id, current_term, lease_expires FROM election_state WHERE id = 1";
        try {
            return jdbcTemplate.queryForObject(sql, new MapSqlParameterSource(), (rs, rowNum) -> {
                ElectionStateRow row = new ElectionStateRow();
                String leaderIdStr = rs.getString("leader_id");
                if (leaderIdStr != null) {
                    row.leaderId = UUID.fromString(leaderIdStr);
                }
                row.currentTerm = rs.getLong("current_term");
                row.leaseExpires = rs.getTimestamp("lease_expires");
                return row;
            });
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }

    public static class ElectionStateRow {
        public UUID leaderId;
        public long currentTerm;
        public java.sql.Timestamp leaseExpires;
    }
}
