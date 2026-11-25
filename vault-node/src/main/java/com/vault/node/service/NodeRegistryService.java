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
    private volatile List<Node> lastKnownRingNodes = null;

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

    public java.util.Optional<Node> findNode(UUID nodeId) {
        return nodeRepository.findById(nodeId);
    }

    public com.vault.common.model.NodeStatus getSelfStatus() {
        return nodeRepository.findById(selfNodeId)
            .map(Node::getStatus)
            .orElse(com.vault.common.model.NodeStatus.DOWN);
    }

    @Scheduled(fixedDelayString = "${vault.ring.refresh-interval-ms:5000}")
    @Transactional(readOnly = true)
    public void refreshRing() {
        rebuildRing(getActiveNodes());
    }

    private void rebuildRing(List<Node> activeNodes) {
        if (lastKnownRingNodes == null || !activeNodes.equals(lastKnownRingNodes)) {
            ring.rebuild(activeNodes);
            lastKnownRingNodes = activeNodes;
            log.info("Ring rebuilt with {} nodes", activeNodes.size());
        }
    }

    /**
     * Returns all nodes eligible to store/serve replicas and receive client traffic.
     * Excludes DOWN and DECOMMISSIONED nodes.
     */
    public List<Node> getActiveNodes() {
        return nodeRepository.findAll().stream()
                .filter(n -> n.getStatus() == NodeStatus.UP || n.getStatus() == NodeStatus.DEGRADED || n.getStatus() == NodeStatus.SUSPECT)
                .toList();
    }

    /**
     * Returns all non-decommissioned nodes (including DOWN) so HeartbeatService can probe recovery.
     */
    public List<Node> getMonitoredNodes() {
        return nodeRepository.findAll().stream()
                .filter(n -> n.getStatus() != NodeStatus.DECOMMISSIONED)
                .toList();
    }

    @Transactional
    public void markNodeSuspect(UUID nodeId) {
        nodeRepository.findById(nodeId).ifPresent(n -> {
            if (n.getStatus() == NodeStatus.UP) {
                n.setStatus(NodeStatus.SUSPECT);
                nodeRepository.saveAndFlush(n);
                log.warn("Marked node {} as SUSPECT", nodeId);
            }
        });
    }

    @Transactional
    public void markNodeDown(UUID nodeId) {
        nodeRepository.findById(nodeId).ifPresent(n -> {
            if (n.getStatus() == NodeStatus.UP || n.getStatus() == NodeStatus.SUSPECT || n.getStatus() == NodeStatus.DEGRADED) {
                n.setStatus(NodeStatus.DOWN);
                nodeRepository.saveAndFlush(n);
                log.error("Marked node {} as DOWN after repeated heartbeat failures; removing from active ring", nodeId);
                rebuildRing(getActiveNodes());
            }
        });
    }

    @Transactional
    public void markNodeUp(UUID nodeId) {
        nodeRepository.findById(nodeId).ifPresent(n -> {
            if (n.getStatus() == NodeStatus.SUSPECT || n.getStatus() == NodeStatus.DOWN || n.getStatus() == NodeStatus.DEGRADED) {
                n.setStatus(NodeStatus.UP);
                nodeRepository.saveAndFlush(n);
                log.info("Marked node {} as UP", nodeId);
                rebuildRing(getActiveNodes());
            }
        });
    }

    @Transactional
    public void markNodeDegraded(String nodeIdStr) {
        UUID nodeId = UUID.fromString(nodeIdStr);
        nodeRepository.findById(nodeId).ifPresent(n -> {
            if (n.getStatus() == NodeStatus.UP) {
                n.setStatus(NodeStatus.DEGRADED);
                nodeRepository.saveAndFlush(n);
                log.warn("Marked node {} as DEGRADED", nodeId);
                rebuildRing(getActiveNodes());
            }
        });
    }
}
