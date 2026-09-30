package com.vault.node.repair;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class MerkleTree {

    private final Node root;

    public MerkleTree(List<String> hashes) {
        this.root = buildTree(hashes);
    }

    public String getRootHash() {
        return root != null ? root.hash : null;
    }

    private Node buildTree(List<String> hashes) {
        if (hashes == null || hashes.isEmpty()) return null;
        
        List<Node> nodes = new ArrayList<>();
        for (String hash : hashes) {
            nodes.add(new Node(hash, null, null));
        }

        while (nodes.size() > 1) {
            List<Node> nextLevel = new ArrayList<>();
            for (int i = 0; i < nodes.size(); i += 2) {
                Node left = nodes.get(i);
                Node right = (i + 1 < nodes.size()) ? nodes.get(i + 1) : left; // Duplicate odd node
                
                String combinedHash = combineHashes(left.hash, right.hash);
                nextLevel.add(new Node(combinedHash, left, right));
            }
            nodes = nextLevel;
        }

        return nodes.get(0);
    }

    private String combineHashes(String h1, String h2) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(h1.getBytes());
            digest.update(h2.getBytes());
            byte[] hashBytes = digest.digest();
            
            StringBuilder sb = new StringBuilder();
            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private static class Node {
        String hash;
        Node left;
        Node right;

        Node(String hash, Node left, Node right) {
            this.hash = hash;
            this.left = left;
            this.right = right;
        }
    }
}
