# Vault Consistency Model

Vault uses a quorum-based replication system governed by tunable Durability Tiers. This document explains the consistency guarantees and associated risks for each tier.

## Overview

Vault replication is coordinated by a Leader node, managing writes and ensuring strong or eventual consistency based on the Durability Tier specified per object.

Vault provides three default Durability Tiers:
- **Critical:** 5 replicas, wait for 3 ACKs (W=3). Strong consistency, high fault tolerance.
- **Standard:** 3 replicas, wait for 2 ACKs (W=2). Strong consistency, balanced latency.
- **Temporary:** 2 replicas, wait for 1 ACK (W=1). Weak consistency, low latency.

## Consistency Guarantees

For **Critical** and **Standard** tiers (W > N/2), Vault guarantees strong consistency (assuming R + W > N). The system will block until a quorum of nodes has acknowledged the write. If the leader fails mid-write before quorum is achieved, the write fails and the client is notified. Partial writes are subsequently cleaned up by background anti-entropy processes.

## Risks of Reduced Consistency (Temporary Tier)

The **Temporary Tier (W=1)** provides high performance and low latency but carries significant durability risks. 

In this tier, a write is considered successful as soon as the **Leader (1 node)** persists the data locally and acknowledges it. Background replication handles syncing the data to the second replica.

**Risk of Data Loss:**
If the Leader node experiences a sudden, catastrophic failure (e.g., hard crash, power loss) immediately after acknowledging a W=1 write, but *before* background replication completes, **that data is permanently lost**. 

When a new Leader is elected, it will have no knowledge of the object because the only node that had it (the old leader) is dead. If the old leader's storage is completely destroyed, the object cannot be recovered.

**Recommendation:**
Use the Temporary tier only for ephemeral data, caching, or easily reproducible assets where data loss is acceptable in edge case scenarios. For user data or critical assets, always use at least the Standard tier.
