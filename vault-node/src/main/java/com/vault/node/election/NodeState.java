package com.vault.node.election;

import com.vault.node.model.Node;
import com.vault.node.repository.NodeRepository;
import com.vault.node.service.NodeRegistryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Component
@RequiredArgsConstructor
@Slf4j
public class NodeState {

    private final NodeRegistryService nodeRegistryService;
    private final NodeRepository nodeRepository;

    private final AtomicReference<UUID> leaderId = new AtomicReference<>(null);
    private final AtomicLong currentTerm = new AtomicLong(0);

    public boolean isLeader() {
        UUID currentLeader = leaderId.get();
        return currentLeader != null && currentLeader.equals(nodeRegistryService.getSelfNodeId());
    }
    
    public UUID getLeaderId() {
        return leaderId.get();
    }

    public void setLeaderId(UUID newLeaderId, long newTerm) {
        if (currentTerm.get() <= newTerm) {
            UUID oldLeader = leaderId.getAndSet(newLeaderId);
            currentTerm.set(newTerm);
            
            if (oldLeader == null || !oldLeader.equals(newLeaderId)) {
                log.info("Term {}: Leader changed from {} to {}", newTerm, oldLeader, newLeaderId);
            }
        }
    }

    public String getLeaderAddress() {
        UUID currentLeader = leaderId.get();
        if (currentLeader == null) return null;
        
        return nodeRepository.findById(currentLeader)
                .map(Node::getAddress)
                .orElse(null);
    }
    
    public long getCurrentTerm() {
        return currentTerm.get();
    }
}
