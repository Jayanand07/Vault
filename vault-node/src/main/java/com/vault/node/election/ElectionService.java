package com.vault.node.election;

import com.vault.node.repository.ElectionRepository;
import com.vault.node.service.NodeRegistryService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
@Slf4j
public class ElectionService {

    private final ElectionRepository electionRepository;
    private final NodeRegistryService nodeRegistryService;
    private final NodeState nodeState;
    private final com.vault.node.metrics.VaultMetrics vaultMetrics;

    @Value("${vault.election.election-timeout-min-ms:7000}")
    private int electionTimeoutMinMs = 7000;

    @Value("${vault.election.election-timeout-max-ms:12000}")
    private int electionTimeoutMaxMs = 12000;

    private final AtomicLong lastHeartbeatNanos = new AtomicLong(System.nanoTime());
    private volatile int currentElectionTimeoutMs = generateTimeout();

    @PostConstruct
    public void init() {
        currentElectionTimeoutMs = generateTimeout();
        // Fetch current state from DB on startup
        ElectionRepository.ElectionStateRow state = electionRepository.getElectionState();
        if (state != null && state.leaderId != null) {
            nodeState.setLeaderId(state.leaderId, state.currentTerm);
        }
    }

    public void recordHeartbeat(long term, java.util.UUID leaderId) {
        if (term >= nodeState.getCurrentTerm()) {
            nodeState.setLeaderId(leaderId, term);
            lastHeartbeatNanos.set(System.nanoTime());
            currentElectionTimeoutMs = generateTimeout(); // reset and randomize
        }
    }

    // Runs every 500ms to check if election timer expired
    @Scheduled(fixedDelay = 500)
    public void checkElectionTimer() {
        if (nodeState.isLeader()) {
            return; // Leaders don't run election timers
        }

        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastHeartbeatNanos.get());
        if (elapsedMs > currentElectionTimeoutMs) {

            com.vault.common.model.NodeStatus status = nodeRegistryService.getSelfStatus();
            if (status == com.vault.common.model.NodeStatus.DEGRADED
                    || status == com.vault.common.model.NodeStatus.SUSPECT) {
                // DEGRADED / SUSPECT nodes do not volunteer for leadership
                log.debug("Skipping election volunteer because node is {}", status);
                return;
            }

            log.warn("Election timeout ({} ms elapsed). Attempting to claim leadership...", elapsedMs);
            vaultMetrics.incrementElections();

            Long newTerm = null;
            try {
                newTerm = electionRepository.tryClaimLeadership(nodeRegistryService.getSelfNodeId());
            } catch (org.springframework.dao.DataAccessException e) {
                log.warn("Database unavailable during election timeout.");
            }

            if (newTerm != null) {
                // We won the election!
                nodeState.setLeaderId(nodeRegistryService.getSelfNodeId(), newTerm);
                log.info("Successfully claimed leadership for term {}!", newTerm);
            } else {
                // Someone else claimed it, or Postgres is down. Reset timer and wait.
                log.info("Failed to claim leadership. Will retry later.");
            }

            // Reset timer whether we won or lost (if we won, we are leader so this check is
            // skipped)
            lastHeartbeatNanos.set(System.nanoTime());
            currentElectionTimeoutMs = generateTimeout();
        }
    }

    private int generateTimeout() {
        int min = Math.max(500, electionTimeoutMinMs);
        int span = Math.max(1, electionTimeoutMaxMs - min);
        return min + java.util.concurrent.ThreadLocalRandom.current().nextInt(span);
    }
}
