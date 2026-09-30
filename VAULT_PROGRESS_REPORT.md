# Vault Project Progress Report

This document outlines the detailed progress of the Vault distributed object storage project, specifically focusing on the recent fixes and overall architecture readiness.

## 1. Bug Fixes & Stabilization (Completed)

### a. Delete/Resurrection Bug Fixed
*   **Issue:** Objects that were deleted (marked `TOMBSTONED`) were still being returned by the latest version queries because the queries only looked for `ACTIVE` rows. This caused files to "resurrect" on the next read.
*   **Resolution:** 
    *   Updated `ObjectMetaRepository` to query across both `ACTIVE` and `TOMBSTONED` statuses when determining the latest version.
    *   Updated `MetadataService` and `AntiEntropyService` to explicitly branch on the status of the latest version—serving the file if `ACTIVE` and returning a 404 if `TOMBSTONED`.
    *   Adjusted the partial index (`idx_objects_latest`) logic to ensure tombstoned rows are respected as the latest state.

### b. Compilation Errors on `@Scheduled` Fixed
*   **Issue:** The `@Scheduled` annotations across background services (`GarbageCollector`, `BackgroundScrubber`, `AntiEntropyService`) had an invalid `scheduler = "backgroundExecutor"` attribute causing compilation failures.
*   **Resolution:** 
    *   Removed the invalid `scheduler` attribute.
    *   Configured a standard Spring task scheduling pool (`spring.task.scheduling.pool.size: 5`) in `application.yml` to prevent heartbeat thread starvation.
    *   Removed the redundant `AsyncConfig.java` to rely on standard Spring Boot task execution.

### c. Version-Claim Race Condition Mitigated
*   **Issue:** During a leadership handoff, `claimVersion` could compute the same next version on two different nodes simultaneously, causing a `DataIntegrityViolationException` on insert due to duplicate primary keys.
*   **Resolution:** 
    *   Wrapped the `claimVersion` calls (for both standard writes and tombstone writes) in a try-catch block inside `ObjectService.java`.
    *   Upon catching `DataIntegrityViolationException`, the system safely retries claiming the version with `version + 1`, successfully handling the collision.

### d. Single-Pass Streaming Checksum Verified
*   **Issue:** Needed to confirm internal replication computes the SHA-256 checksum dynamically while streaming data to disk without buffering the entire payload into memory.
*   **Resolution:** 
    *   Refactored `InternalController.java` to use `java.security.DigestInputStream`.
    *   The stream is now read, hashed in chunks, and written directly to a temporary file via `Files.copy` in a single pass, completely eliminating large in-memory byte arrays.

### e. Consistency Model Risks Documented
*   **Issue:** The risks associated with the Temporary Durability Tier (W=1) needed clear documentation regarding data loss.
*   **Resolution:** 
    *   Created `docs/consistency-model.md` detailing the strong consistency guarantees of Critical/Standard tiers.
    *   Explicitly outlined how a W=1 write can lead to permanent data loss if the leader crashes before background replication kicks in.

## 2. Core Features (Implemented & Ready)

*   **Raft-lite Leader Election:** Node discovery and dynamic leader election using a PostgreSQL backend.
*   **Quorum-based Replication:** Configurable durability tiers (Critical, Standard, Temporary) dictating how many nodes must acknowledge a write before it is committed.
*   **Zero-Copy Streaming:** Object uploads and downloads are streamed directly through controllers to/from the local file system using `StreamingResponseBody`, minimizing memory overhead.
*   **S3-Compatible API:** `S3CompatibleController` provides a translation layer for S3 clients to interface with Vault's native object API.
*   **Background Maintenance:** 
    *   Anti-Entropy: Re-syncs replicas that fall out of sync with the metadata truth.
    *   Scrubber: Checks for bit-rot/corruption on disk.
    *   Garbage Collector: Cleans up orphaned or tombstoned files after their TTL expires.

## 3. What is Left (Next Steps)

1.  **Tier-1 Definition of Done Validation:** Run the distributed tests using the actual `docker-compose.yml` to provide hard evidence of system resilience. This requires an environment with Docker installed.
    *   3-Node Cluster Startup
    *   Checksum validation during normal Upload/Download
    *   Simulated Leader Failover timing (< 15 seconds)
    *   Network partition testing via `tc netem`
    *   Replica corruption and self-repair validation
2.  **Performance Profiling:** Conduct load tests to measure throughput and memory overhead under heavy concurrent zero-copy streaming.
3.  **Comprehensive Client SDKs:** Develop SDKs (Python, Go) for native interactions, bypassing the S3 layer if users want to use Vault's native features directly.
