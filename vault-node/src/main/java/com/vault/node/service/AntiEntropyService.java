package com.vault.node.service;

import com.vault.node.model.Replica;
import com.vault.node.repair.MerkleTree;
import com.vault.node.repository.ReplicaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class AntiEntropyService {

    private final ReplicaRepository replicaRepository;
    private final NodeRegistryService nodeRegistryService;
    private final LocalStorageService localStorageService;

    // Run every hour
    @Scheduled(fixedDelayString = "${vault.repair.anti-entropy-interval-ms:3600000}")
    public void sync() {
        log.info("Starting Anti-Entropy sync towards metadata truth...");
        
        // 1. Build Merkle tree of expected state (from DB)
        List<Replica> expectedReplicas = replicaRepository.findLatestByNodeId(nodeRegistryService.getSelfNodeId());
        List<String> expectedHashes = expectedReplicas.stream()
                .map(Replica::getChecksumSha256)
                .sorted()
                .collect(Collectors.toList());
        
        MerkleTree expectedTree = new MerkleTree(expectedHashes);
        
        // Diagnostic telemetry only: computes expected Merkle root hash vs local file count.
        // Actual repair and cleanup are performed by ReadRepairService, BackgroundScrubber, and RebalanceService.
        List<Path> localFiles = localStorageService.listAllDataFiles();
        
        log.info("Anti-entropy diagnostic check: Expected {} DB replicas, found {} files on disk. (Expected root: {})",
                expectedHashes.size(), localFiles.size(), expectedTree.getRootHash());
    }
}
