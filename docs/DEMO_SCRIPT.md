# Vault: Capstone Presentation & Live Demo Script

> **Your 5-minute showcase script and viva defense cheat sheet to demonstrate Vault as a 10/10 distributed systems project.**

---

## 1. The 30-Second Elevator Pitch
> *"Most web applications either store files in PostgreSQL as `bytea`—which bloats the database and destroys query performance—or store them in AWS S3, where cloud egress fees add up and you lose the ability to write SQL joins between your user accounts and their files.*  
> *Vault is a **Postgres-Native Distributed Object Storage Engine**. It uses PostgreSQL as the 'Brain' for metadata, versioning, and PostgreSQL-coordinated leader leases, while streaming binary payloads in bounded 8 KB chunks to local disk with quorum replication, SHA-256 pre-verified reads, and automatic peer fallback / read-repair."*

---

## 2. Live Demo: Step-by-Step Walkthrough

### Part 1: Cluster Health & Topology (1 Minute)
1. Open the **Vault Web Console** in your browser:
   `http://localhost:8080/console.html` (or `website/console.html`).
2. Point out the **Live Telemetry Banner**:
   - Show the **Active Ring Nodes** (e.g., `Node-1`, `Node-2`, `Node-3`).
   - Point out the **Cluster Leader** and current **Election Term**.
   - Explain: *"Nodes coordinate via PostgreSQL heartbeats using atomic leases and fencing tokens to prevent split-brain conditions."*

---

### Part 2: Upload & The Object Health Inspector (2 Minutes)
1. Switch to the **Object Explorer (S3)** tab.
2. Drag and drop a sample file (e.g., `avatar.png` or `report.pdf`).
3. Select the Durability Tier:
   - **`STANDARD`**: Writes synchronously to 2 of 3 nodes (Quorum $W=2, N=3$).
4. Click Upload. Once uploaded, click the **🔬 Health Inspector** button next to the file.
5. Highlight what appears:
   - **Cryptographic SHA-256 Hash**: Computed on-the-fly during a single streaming pass without buffering into JVM memory.
   - **Distributed Replica Placement**: Shows the file exists on `vault-node-1`, `vault-node-2`, and `vault-node-3`.
   - **Storage Shard Path**: Points out the 2-level directory sharding (e.g. `/data/vault/objects/8f/43/...`) preventing file system inode degradation.
   - **Integrity**: *100% Bit-Rot Free*.

---

### Part 3: Experimental S3 Adapter & Native REST API (1 Minute)
1. Switch to the **Code Generator & Playground** tab.
2. Explain that Vault exposes an **experimental S3-style adapter** at `/s3/{bucket}/{key}` alongside the canonical `/api/v1/objects` REST API (both authenticated via `X-API-Key`; AWS Signature V4 and multipart uploads are deferred):
   - **Native REST / Experimental S3 via cURL**:
     ```bash
     curl -X PUT -L "http://localhost:8081/api/v1/objects?key=users/avatar.png" \
       -H "X-API-Key: $VAULT_API_KEY" \
       -H "X-Vault-Durability: STANDARD" \
       -H "Content-Type: application/octet-stream" \
       --data-binary @avatar.png
     ```
   - **Python Verifier (`scripts/verify_s3_boto3.py`)**: Exercises `PUT`, `GET`, and `DELETE` against `/s3/{bucket}/{key}` using `X-API-Key`.

---

### Part 4: Self-Repair & Bit-Rot Simulation (1 Minute)
1. Switch to the **Self-Repair Simulator** tab.
2. Click **Simulate Bit-Rot & Repair Scenario**.
3. Explain the 4 steps that appear in real-time:
   - *Step 1*: A cosmic ray or drive degradation flips a bit on Node 2.
   - *Step 2*: Client requests the file. Node 2 pre-verifies its local `FileChannel` SHA-256 **before** sending HTTP `200` and blocks the corrupt file.
   - *Step 3*: Node 2 streams a verified replica from peer Node 1 to a staged temp file (`Flux<DataBuffer>`).
   - *Step 4*: Node 2 returns `200 OK` with verified bytes and repairs its local disk via stage-verify-atomic-install (*Read Repair*).

---

## 3. Viva / Interview Defense Cheat Sheet (The 5 Tough Questions)

### Q1: "Why not just use Raft or Paxos instead of PostgreSQL leases?"
> **Answer**: *"Real Raft requires managing dedicated log consensus daemons and quorum state machines. Since web applications already depend on a relational database like PostgreSQL for ACID user transactions, Vault leverages PostgreSQL as the single source of truth for metadata and leader leases with fencing tokens. This dramatically simplifies operational overhead while ensuring absolute consistency for file pointers."*

### Q2: "Isn't PostgreSQL a Single Point of Failure (SPOF)?"
> **Answer**: *"If PostgreSQL goes offline, storage nodes fail closed—new writes and metadata reads reject with HTTP 503 to prevent data divergence. However, binary payloads on local NVMe disks remain completely unharmed. In production enterprise deployments, PostgreSQL is run in High Availability mode (such as Amazon RDS Multi-AZ or Patroni with physical streaming replication) to guarantee 99.99% uptime."*

### Q3: "How do you prevent Out-Of-Memory (OOM) errors during large uploads?"
> **Answer**: *"Vault avoids loading file bytes into JVM heap memory. Raw PUT uploads stream through bounded 8 KB buffers up to `max-object-size-bytes` into `.partial` temp files, and reads pre-verify `FileChannel` SHA-256 checksums before returning HTTP `200` (`InputStreamResource` / staged `Flux<DataBuffer>` peer fallback)."*

### Q4: "What happens if a node crashes halfway through an upload?"
> **Answer**: *"The upload write occurs on an isolated `.partial` temporary file. If the network drops or the node dies, the write quorum ($W=2$) is not satisfied, the uncommitted version in PostgreSQL is rolled back to `FAILED`, and the node's startup routine cleans up any lingering `.partial` files. Only when quorum confirms data is flushed to disk does the version transition to `ACTIVE`."*

### Q5: "How does Vault compare to MinIO?"
> **Answer**: *"MinIO is a full standalone S3-compatible store, whereas Vault stores object metadata in standard PostgreSQL tables (`objects`, `replicas`) alongside an experimental S3-style path adapter. This lets applications join object metadata with relational rows in SQL."*
