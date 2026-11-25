# Vault: System Architecture & Technical Specifications

## 1. Architectural Philosophy: The Brain & The Muscle

Vault rejects the conventional approaches to file storage in web applications:
*   **Anti-Pattern 1 (Postgres `bytea`)**: Storing binary data in database rows causes massive table bloat, slow checkpoints, oversized write-ahead logs, and memory exhaustion.
*   **Anti-Pattern 2 (Cloud S3 Egress Lock-in)**: Storing files in proprietary clouds isolates file metadata from application databases, making SQL joins impossible and incurring high egress bills.

Vault decouples metadata management from binary persistence:

```
+-----------------------------------------------------------+
|                      VAULT ARCHITECTURE                    |
+-----------------------------------------------------------+
|                                                           |
|    CLIENT INTERFACES                                      |
|    - S3 Compatible API (/s3/{bucket}/{key})               |
|    - Native REST API   (/api/v1/objects/{objectId})       |
|    - Vault Web Console (/console.html)                    |
|    - Python CLI Tool   (scripts/vault_cli.py)             |
|                                                           |
+-----------------------------+-----------------------------+
                              |
+-----------------------------v-----------------------------+
|    STORAGE NODES (Java 17 / Spring Boot)                  |
|    - ObjectService (Guava Striped Locks, Atomic Version)  |
|    - ReplicationService (Parallel Quorum Coordinator)     |
|    - LocalStorageService (Directory Sharding, fsync)      |
|    - ReadRepairService & BackgroundScrubber               |
|                                                           |
+--------------+-----------------------------+--------------+
               |                             |
+--------------v---------------+ +-----------v--------------+
|   THE BRAIN (PostgreSQL)     | |   THE MUSCLE (Local Disk)|
|   - objects                  | |   - ObjectKey SHA-256    |
|   - replicas                 | |   - Pre-verified reads   |
|   - election_state           | |   - Atomic .partial move |
+------------------------------+ +--------------------------+
```

---

## 2. Core Subsystems

### A. Consensus: PostgreSQL-Coordinated Leader Lease with Fencing
Vault implements a PostgreSQL-coordinated leader lease mechanism (not Raft):
*   **Heartbeat Mechanism**: Storage nodes update `election_state` every 1,000 ms.
*   **Lease Expiry**: A leader's lease is valid for 5,000 ms.
*   **Fencing Tokens**: Every successful election increments a monotonic `term` number. Requests sent by a stale leader carrying an outdated term are rejected by peer nodes (`409 Conflict`).
*   **Randomized Election Timeout**: Followers observe heartbeats; if no heartbeat arrives within a randomized window (7,000 ms – 12,000 ms), the follower attempts atomic lease acquisition.

### B. Distributed Hash Ring (Virtual Nodes)
To distribute objects evenly across physical hardware:
*   **MurmurHash3 (32-bit)**: Maps both node identities and object keys onto a circular hash space $[0, 2^{32}-1]$.
*   **Virtual Nodes (150 vnodes per physical node)**: Prevents hot spots and ensures smooth rebalancing when nodes join or leave.

### C. Local Storage Sharding & In-Flight Isolation
*   **Collision-Free Sharding**: Objects are keyed by `SHA-256(objectId)` under `/data/vault/objects/<h0..2>/<h2..4>/<sha256(key)>.v<ver>.dat` (`ObjectKey`), preventing slash/underscore collisions and OS filename limits.
*   **Atomic Move + Fsync**: Payloads stream into `tmp/<hash>.v<ver>.<uuid>.partial`, sync with `FileChannel.force(true)`, and atomically rename via `Files.move(..., StandardCopyOption.ATOMIC_MOVE)`.

---

## 3. Memory & I/O Optimization Profile

| Operation | Implementation | Memory Profile |
| :--- | :--- | :--- |
| **Client Upload** | Raw binary body spooled via bounded `UploadSpooler` (max 128 MiB; multipart `415`) | Bounded 8 KB chunk loop |
| **Client Download** | Pre-verified pinned `FileChannel` + `InputStreamResource` (plus temp-staged `Flux<DataBuffer>` peer fallback) | Bounded chunk I/O; never serves unverified bytes |
| **Replication Push** | Spring WebClient streaming `FileSystemResource` with `X-Vault-Term` fencing | Bounded streaming via Netty byte buffers |
| **Checksum Validation** | Pre-read `FileChannel` SHA-256 verify + rewind; single-pass `DigestInputStream` on write | No full-object JVM heap array allocation |

---

## 4. Component Implementation & Heuristic Disclosures

| Component | Status | Notes |
| :--- | :--- | :--- |
| **PostgreSQL Lease & Fencing** | Enforced | `ElectionRepository` 5s leases (`vault.election.lease-duration-ms`), `election_state` row lock at claim & commit (`409` on stale leader) |
| **Pre-Verified Reads & Stage-Verify Repair** | Enforced | Local `FileChannel` and temp-staged peer reads verify SHA-256 & byte size before `200 OK` |
| **Early-Quorum Replication** | Enforced | `ReplicationService` completes immediately upon `W` acks (`vault.replication.timeout-ms`) |
| **Experimental S3 Adapter (`/s3/{bucket}/{key}`)** | Partial / Auth via `X-API-Key` | Shares native PUT/GET/DELETE core; AWS SigV4 and multipart uploads are **not** implemented |
| **`AdaptiveConsistencyEngine`** | Heuristic | Reads live active-node ratio (`healthScore`); latency & access pattern terms are fixed constants (`0.9`, `0.8`) |
| **`FailurePredictionEngine`** | Heuristic | Tracks rolling heartbeat latency statistics to flag `DEGRADED` nodes (not ML-based prediction) |
| **`AntiEntropyService`** | Diagnostic | Logs expected `MerkleTree` root hashes vs local disk file count; actual healing runs via `ReadRepairService`, `BackgroundScrubber`, and `RebalanceService` |
