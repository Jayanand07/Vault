package com.vault.node.repository;

import com.vault.node.model.ElectionState;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ElectionStateRepository extends JpaRepository<ElectionState, Integer> {
}
