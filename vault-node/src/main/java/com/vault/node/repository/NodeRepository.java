package com.vault.node.repository;

import com.vault.node.model.Node;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface NodeRepository extends JpaRepository<Node, UUID> {
}
