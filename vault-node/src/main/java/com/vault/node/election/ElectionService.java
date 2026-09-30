package com.vault.node.election;

import com.vault.node.repository.ElectionRepository;
import com.vault.node.service.NodeRegistryService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
@Slf4j
public class ElectionService {

    private final ElectionRepository electionRepository;
    private final NodeRegistryService nodeRegistryService;
    private final NodeState nodeState;
    private final com.vault.node.metrics.VaultMetrics vaultMetrics;

    private final AtomicLong lastHeartbeatReceived = new AtomicLong(System.currentTimeMillis());
    private final Random random = new Random();
    private int currentElectionTimeoutMs = generateTimeout();

    @PostConstruct
    public void init() {
        // Fetch current state from DB on startup
        ElectionRepository.ElectionStateRow state = electionRepository.getElectionState();
        if (state != null && state.leaderId != null) {
            nodeState.setLeaderId(state.leaderId, state.currentTerm);
        }
    }

    public void recordHeartbeat(long term, java.util.UUID leaderId) {
        if (term >= nodeState.getCurrentTerm()) {
            nodeState.setLeaderId(leaderId, term);
            lastHeartbeatReceived.set(System.currentTimeMillis());
            currentElectionTimeoutMs = generateTimeout(); // reset and randomize
        }
    }

    // Runs every 500ms to check if election timer expired
    @Scheduled(fixedDelay = 500)
    public void checkElectionTimer() {
        if (nodeState.isLeader()) {
            return; // Leaders don't run election timers
        }

        long elapsed = System.currentTimeMillis() - lastHeartbeatReceived.get();
        if (elapsed > currentElectionTimeoutMs) {
            
            com.vault.common.model.NodeStatus status = nodeRegistryService.getSelfStatus();
            if (status == com.vault.common.model.NodeStatus.DEGRADED || status == com.vault.common.model.NodeStatus.SUSPECT) {
                // Phase 8: DEGRADED nodes don't receive new leader responsibilities
                log.debug("Skipping election volunteer because node is {}", status);
                return;
            }

            log.warn("Election timeout ({} ms elapsed). Attempting to claim leadership...", elapsed);
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
            
            // Reset timer whether we won or lost (if we won, we are leader so this check is skipped)
            lastHeartbeatReceived.set(System.currentTimeMillis());
            currentElectionTimeoutMs = generateTimeout();
        }
    }

    private int generateTimeout() {
        // 7 to 12 seconds
        return 7000 + random.nextInt(5000);
    }
}
