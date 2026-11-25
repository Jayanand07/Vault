package com.vault.node.placement;

import com.google.common.hash.Hashing;
import com.vault.node.model.Node;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

@Component
public class ConsistentHashRing {

    private final int vnodesPerNode;
    private final TreeMap<Long, Node> ring = new TreeMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public ConsistentHashRing(@Value("${vault.replication.vnodes-per-node:150}") int vnodesPerNode) {
        this.vnodesPerNode = vnodesPerNode;
    }

    public void rebuild(List<Node> activeNodes) {
        lock.writeLock().lock();
        try {
            ring.clear();
            for (Node node : activeNodes) {
                for (int i = 0; i < vnodesPerNode; i++) {
                    long hash = Hashing.murmur3_128()
                            .hashString(node.getNodeId() + "#" + i, UTF_8)
                            .asLong();
                    ring.put(hash, node);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<Node> getTargetNodes(String objectId, int count) {
        return getTargetNodes(objectId, count, com.vault.common.model.DurabilityTier.STANDARD);
    }

    public List<Node> getTargetNodes(String objectId, int count, com.vault.common.model.DurabilityTier tier) {
        lock.readLock().lock();
        try {
            if (ring.isEmpty()) {
                return Collections.emptyList();
            }

            long hash = Hashing.murmur3_128()
                    .hashString(objectId, UTF_8).asLong();

            List<Node> targets = new ArrayList<>();
            Set<UUID> seen = new HashSet<>();

            // Walk clockwise
            for (Map.Entry<Long, Node> entry : ring.tailMap(hash).entrySet()) {
                Node node = entry.getValue();
                if (tier == com.vault.common.model.DurabilityTier.CRITICAL && node.getStatus() == com.vault.common.model.NodeStatus.DEGRADED) {
                    continue; // Skip DEGRADED nodes for CRITICAL writes
                }
                if (seen.add(node.getNodeId())) {
                    targets.add(node);
                    if (targets.size() == count) return targets;
                }
            }
            // Wrap around
            for (Map.Entry<Long, Node> entry : ring.entrySet()) {
                Node node = entry.getValue();
                if (tier == com.vault.common.model.DurabilityTier.CRITICAL && node.getStatus() == com.vault.common.model.NodeStatus.DEGRADED) {
                    continue; // Skip DEGRADED nodes for CRITICAL writes
                }
                if (seen.add(node.getNodeId())) {
                    targets.add(node);
                    if (targets.size() == count) return targets;
                }
            }
            return targets;
        } finally {
            lock.readLock().unlock();
        }
    }
}
