package com.vault.node.election;

import com.vault.node.model.Node;
import com.vault.node.repository.NodeRepository;
import com.vault.node.service.NodeRegistryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

@Component
@RequiredArgsConstructor
@Slf4j
public class NodeState {

    public record LeaderTerm(UUID leaderId, long term) {}

    private final NodeRegistryService nodeRegistryService;
    private final NodeRepository nodeRepository;

    private final AtomicReference<LeaderTerm> leaderTerm = new AtomicReference<>(new LeaderTerm(null, 0L));

    public boolean isLeader() {
        UUID currentLeader = leaderTerm.get().leaderId();
        return currentLeader != null && currentLeader.equals(nodeRegistryService.getSelfNodeId());
    }
    
    public UUID getLeaderId() {
        return leaderTerm.get().leaderId();
    }

    public void setLeaderId(UUID newLeaderId, long newTerm) {
        LeaderTerm updated = leaderTerm.accumulateAndGet(
                new LeaderTerm(newLeaderId, newTerm),
                (curr, next) -> next.term() >= curr.term() ? next : curr
        );
        if (updated.term() == newTerm && (updated.leaderId() == null || updated.leaderId().equals(newLeaderId))) {
            log.info("Term {}: Leader is {}", newTerm, newLeaderId);
        }
    }

    public String getLeaderAddress() {
        UUID currentLeader = leaderTerm.get().leaderId();
        if (currentLeader == null) return null;
        
        return nodeRepository.findById(currentLeader)
                .map(Node::getAddress)
                .orElse(null);
    }
    
    public long getCurrentTerm() {
        return leaderTerm.get().term();
    }
}
