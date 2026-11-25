# Vault Consistency Model

Vault uses a quorum-based replication system governed by tunable Durability Tiers. This document explains the consistency guarantees and associated risks for each tier.

## Overview

Vault replication is coordinated by a Leader node, managing writes and ensuring strong or eventual consistency based on the Durability Tier specified per object.

Vault provides three default Durability Tiers:
- **Critical:** 5 replicas, wait for 3 ACKs ($N=5, W=3, R=2$). Strong consistency ($W + R > N$). Requires $\ge 5$ eligible non-degraded nodes for full redundancy; fails fast (`507`) if fewer than 3 non-degraded targets exist.
- **Standard:** 3 replicas, wait for 2 ACKs ($N=3, W=2, R=1$). Balanced latency and quorum durability (default).
- **Temporary:** 1 replica, wait for 1 ACK ($N=1, W=1, R=1$). Weak/single-node durability, minimum latency.

## Consistency Guarantees

For **Critical** and **Standard** tiers, Vault synchronously waits until a write quorum ($W$) of nodes has persisted and acknowledged the payload (via early-quorum completion without blocking on slower stragglers beyond $W$), AND verifies an active PostgreSQL-coordinated leader lease with row lock before committing `ACTIVE`. If quorum fails or the lease lapses, the write rolls back to `FAILED`, idempotency claims are released, and stranded `.dat`/`.partial`/failed rows are cleaned up by `BackgroundScrubber` and `GarbageCollector`.

## Risks of Reduced Consistency (Temporary Tier)

The **Temporary Tier (N=1, W=1)** prioritizes minimum write latency by storing a single replica on the primary ring target without synchronous peer replication, carrying significant durability risk:

In this tier, a write is considered successful as soon as the **single target node** persists the data locally and acknowledges it.

**Risk of Data Loss:**
If that node's disk fails or the node crashes permanently, **that object has no redundant peer replica and may be lost**. 

When a new Leader is elected, it will have no knowledge of the object because the only node that had it (the old leader) is dead. If the old leader's storage is completely destroyed, the object cannot be recovered.

**Recommendation:**
Use the Temporary tier only for ephemeral data, caching, or easily reproducible assets where data loss is acceptable in edge case scenarios. For user data or critical assets, always use at least the Standard tier.
