package com.vault.node.service;

import com.vault.common.model.NodeStatus;
import com.vault.node.model.Node;
import com.vault.node.placement.ConsistentHashRing;
import com.vault.node.repository.NodeRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class NodeRegistryService {

    private final NodeRepository nodeRepository;
    private final ConsistentHashRing ring;

    @Value("${vault.node.id}")
    private String selfNodeIdStr;

    @Value("${vault.node.address}")
    private String selfAddress;

    private UUID selfNodeId;
    private List<Node> lastKnownRingNodes = null;

    @PostConstruct
    @Transactional
    public void init() {
        this.selfNodeId = UUID.fromString(selfNodeIdStr);
        
        // Register or update self in DB
        Node self = nodeRepository.findById(selfNodeId).orElseGet(Node::new);
        self.setNodeId(selfNodeId);
        self.setAddress(selfAddress);
        self.setStatus(NodeStatus.UP);
        nodeRepository.save(self);
        
        refreshRing();
    }

    public UUID getSelfNodeId() {
        return selfNodeId;
    }

    public String getSelfAddress() {
        return selfAddress;
    }

    public com.vault.common.model.NodeStatus getSelfStatus() {
        return nodeRepository.findById(selfNodeId)
            .map(Node::getStatus)
            .orElse(com.vault.common.model.NodeStatus.DOWN);
    }

    @Scheduled(fixedDelayString = "${vault.ring.refresh-interval-ms:5000}")
    @Transactional(readOnly = true)
    public void refreshRing() {
        // Find nodes that are UP or SUSPECT
        List<Node> activeNodes = getActiveNodes();

        // If ring changed, rebuild
        if (lastKnownRingNodes == null || !activeNodes.equals(lastKnownRingNodes)) {
            ring.rebuild(activeNodes);
            lastKnownRingNodes = activeNodes;
            log.info("Ring rebuilt with {} nodes", activeNodes.size());
        }
    }
    
    public List<Node> getActiveNodes() {
        return nodeRepository.findAll().stream()
                .filter(n -> n.getStatus() == NodeStatus.UP || n.getStatus() == NodeStatus.DEGRADED)
                .toList();
    }

    @Transactional
    public void markNodeSuspect(UUID nodeId) {
        nodeRepository.findById(nodeId).ifPresent(n -> {
            if (n.getStatus() == NodeStatus.UP) {
                n.setStatus(NodeStatus.SUSPECT);
                nodeRepository.save(n);
                log.warn("Marked node {} as SUSPECT", nodeId);
            }
        });
    }

    @Transactional
    public void markNodeUp(UUID nodeId) {
        nodeRepository.findById(nodeId).ifPresent(n -> {
            if (n.getStatus() == NodeStatus.SUSPECT || n.getStatus() == NodeStatus.DOWN || n.getStatus() == NodeStatus.DEGRADED) {
                n.setStatus(NodeStatus.UP);
                nodeRepository.save(n);
                log.info("Marked node {} as UP", nodeId);
            }
        });
    }

    @Transactional
    public void markNodeDegraded(String nodeIdStr) {
        UUID nodeId = UUID.fromString(nodeIdStr);
        nodeRepository.findById(nodeId).ifPresent(n -> {
            if (n.getStatus() == NodeStatus.UP) {
                n.setStatus(NodeStatus.DEGRADED);
                nodeRepository.save(n);
                log.warn("Marked node {} as DEGRADED", nodeId);
            }
        });
    }
}
