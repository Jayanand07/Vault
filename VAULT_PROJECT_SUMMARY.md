# Vault: The Postgres-Native Object Store
**Complete Project Summary & Architecture Report**

Vault is a production-grade, distributed object storage engine designed specifically for developers who want the convenience of Amazon S3, but prefer to keep their file metadata inside their existing PostgreSQL databases. 

By separating the **Metadata (PostgreSQL)** from the **Binary Data (Local NVMe/Disk)**, Vault allows developers to perform standard SQL `JOIN`s between their application data and file storage, while preventing database bloat and maintaining extreme read/write speeds.

---

## 1. Core Architecture & Infrastructure

### PostgreSQL-Backed Source of Truth
Instead of relying on complex gossip protocols or external consensus systems like ZooKeeper/etcd, Vault uses PostgreSQL as the absolute source of truth for both file metadata and cluster leadership. 
*   **Metadata Table (`object_meta`):** Stores file sizes, SHA-256 checksums, version numbers, and durability tiers.
*   **Replica Table (`replica_locations`):** Tracks exactly which physical node holds which piece of data.
*   **Lease Table (`cluster_leases`):** Manages Raft-inspired cluster leader elections using atomic database locks.

### Multi-Node Clustering
Vault is designed to run as a cluster of discrete nodes. 
*   **Leader Election:** Nodes ping the database every 2 seconds. If the active leader dies, a follower automatically promotes itself.
*   **Consistent Hashing:** Object keys are mapped to a hash ring to determine which nodes should store the replicas, ensuring even data distribution.

---

## 2. Key Features Implemented

### 🔌 Drop-In S3 Compatibility
Vault features a dedicated `S3CompatibleController` that mimics the standard AWS S3 API. Developers can point the AWS CLI, `boto3`, or AWS JS SDK directly to `http://vault-cluster/s3/` to upload and download files without modifying a single line of their existing application code.

### ⚖️ Adaptive Consistency
Clients can specify `X-Vault-Durability` headers to dynamically balance speed vs. safety on a per-file basis:
*   **STANDARD (Strong Consistency):** Writes synchronously to 3 nodes. The write blocks until a quorum (2/3) acknowledges the save. Best for critical documents.
*   **REDUCED (Eventual Consistency):** Writes synchronously only to the Leader. The leader asynchronously replicates the file to followers in the background. Best for high-throughput, non-critical data like logs or temporary cache.

### 🔄 Self-Healing & Read Repair
Vault constantly protects against "bit-rot" and corrupted disks. 
*   When a file is read, Vault computes the SHA-256 hash on-the-fly.
*   If the hash doesn't match the database, Vault throws out the bad data, instantly requests a healthy copy from a peer node, streams it to the user, and repairs its local disk in the background.

### 🛡️ Idempotency Layer
To protect against network blips (where a client uploads a file, loses internet, and retries), Vault implements an Idempotency system. Using atomic Postgres locks, Vault guarantees that duplicate upload requests for the same `X-Idempotency-Key` are rejected or safely ignored, preventing cluster corruption.

---

## 3. Extreme Memory Optimization (Zero-Copy Streaming)

A massive engineering focus was placed on preventing Out-Of-Memory (OOM) crashes, a common flaw in junior storage projects.

*   **Zero-Copy Downloads:** Files are never loaded into the JVM heap. When a user requests a file, Vault uses Spring's `StreamingResponseBody` to pipe the bytes directly from the local file system to the HTTP socket in tiny 8KB chunks.
*   **Spooling Uploads:** When a massive file is uploaded via the S3 adapter or native API, it is *not* buffered into a `byte[]` array. Instead, it is spooled directly to a temporary file on disk using `Files.copy()`.
*   **Efficient Internal Replication:** When nodes sync data with each other, they pass `FileSystemResource` objects and `InputStream`s, meaning a 100GB file could be replicated across the cluster using less than 10MB of RAM.
*   **Hard Limits:** The application is configured at the Tomcat level (`application.yml`) to outright reject single files larger than 128MB as a final safety net against malicious DoS attacks.

---

## 4. Production Readiness & Operations

### Marketing & Documentation
*   **Responsive Landing Page (`website/index.html`):** A modern, dark-mode, glassmorphism UI designed to explain the value proposition to developers.
*   **Technical Docs (`website/docs.html`):** Detailed REST API specifications and S3 CLI examples to onboard users quickly.

### Deployment & CI/CD
*   **Dockerized Cluster:** A complete `docker-compose.yml` spins up a 3-node Vault cluster alongside a PostgreSQL instance and Prometheus/Grafana monitoring stack with a single command.
*   **Deep Health Checks:** Exposes `/actuator/health` endpoints that report not just CPU usage, but Quorum health and Leadership status.
*   **GitHub Actions:** Included a `.github/workflows/build.yml` file to automatically compile and test the Java codebase upon every commit.
*   **Open Source Standard:** Licensed under the Apache 2.0 License, making it legally safe for enterprise adoption.

---

## Summary
Vault was successfully pivoted from a generic distributed storage engine into a highly-specialized, highly-resilient tool tailored for PostgreSQL developers. It solves complex concurrency, networking, and memory-management problems using robust, industry-standard patterns. 
