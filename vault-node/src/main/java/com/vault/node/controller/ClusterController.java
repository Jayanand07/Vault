package com.vault.node.controller;

import com.vault.node.election.NodeState;
import com.vault.node.model.Node;
import com.vault.node.service.NodeRegistryService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/cluster")
@RequiredArgsConstructor
public class ClusterController {

    private final NodeState nodeState;
    private final NodeRegistryService nodeRegistryService;

    @GetMapping("/status")
    public ResponseEntity<?> getClusterStatus() {
        List<Node> activeNodes = nodeRegistryService.getActiveNodes();
        
        var nodes = activeNodes.stream()
            .map(node -> Map.of(
                "nodeId", node.getNodeId().toString(),
                "address", node.getAddress(),
                "isLeader", nodeState.getLeaderId() != null && nodeState.getLeaderId().equals(node.getNodeId())
            )).collect(Collectors.toList());

        return ResponseEntity.ok(Map.of(
            "leaderId", nodeState.getLeaderId() != null ? nodeState.getLeaderId().toString() : "UNKNOWN",
            "leaderAddress", nodeState.getLeaderAddress() != null ? nodeState.getLeaderAddress() : "UNKNOWN",
            "term", nodeState.getCurrentTerm(),
            "nodes", nodes,
            "activeNodeCount", nodes.size()
        ));
    }

    @GetMapping("/leader")
    public ResponseEntity<?> getLeader() {
        if (nodeState.getLeaderId() == null) {
            return ResponseEntity.status(503).body(Map.of("error", "No leader currently elected"));
        }
        
        return ResponseEntity.ok(Map.of(
            "leaderId", nodeState.getLeaderId().toString(),
            "leaderAddress", nodeState.getLeaderAddress() != null ? nodeState.getLeaderAddress() : "UNKNOWN"
        ));
    }
}
