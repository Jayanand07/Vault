# Vault: API Reference & S3 Compatibility Matrix

> **Comprehensive interface documentation for Amazon S3 SDKs, cURL, and native HTTP microservices.**

---

## 1. S3 Compatibility Matrix (Experimental Adapter)

Vault provides an experimental S3-style adapter at `/s3/{bucket}/{key}`. Authentication requires `X-API-Key`; AWS Signature V4 (`Authorization: AWS4-HMAC-SHA256`) and S3 Multipart Uploads are **not** implemented, so unmodified AWS CLI / Boto3 requests without `X-API-Key` will receive `401 Unauthorized`.

| Operation / Feature | S3 Protocol Command | Vault Support | Notes |
| :--- | :--- | :---: | :--- |
| **Put Object** | `PUT /{bucket}/{key}` | ✅ Supported | Raw binary stream up to 128 MiB (`413` over limit, `415` on multipart) |
| **Get Object** | `GET /{bucket}/{key}` | ✅ Supported | Streaming download |
| **Delete Object** | `DELETE /{bucket}/{key}`| ✅ Supported | Atomic tombstone version creation |
| **List Objects** | `GET /api/v1/objects` | ⚠️ Native only | Prefix-escaped (`_`, `%`, `\`) listing on native REST endpoint |
| **Head Object** | `HEAD /api/v1/objects?...` | ⚠️ Native only | Returns headers (`X-Vault-Version`, `X-Vault-Checksum`, `Content-Length`) |
| **AWS Signature V4** | Auth Header | 📋 Deferred | Uses `X-API-Key` filter |
| **Multipart Upload** | `InitiateMultipartUpload`| 📋 Deferred | Raw binary PUT up to `vault.storage.max-object-size-bytes` (128 MiB) |
| **Presigned URLs** | Query Param Auth | 📋 Deferred | Future work |

---

## 2. Native REST API (`/api/v1/...`)

Slash-containing object keys (e.g., `reports/q3-summary.pdf`) are supported both via the canonical query form (`/api/v1/objects?key=reports/q3-summary.pdf`, `/api/v1/objects/inspect?key=reports/q3-summary.pdf`) and unencoded multi-segment paths (`/api/v1/objects/reports/q3-summary.pdf`). Avoid percent-encoded slashes (`%2F`), which Tomcat rejects.

### A. Objects API

#### 1. Upload Object
*   **Method**: `PUT /api/v1/objects?key={objectId}` or `PUT /api/v1/objects/{objectId...}`
*   **Headers**:
    *   `X-API-Key`: Required (configured via `VAULT_API_KEYS`, or explicit `dev` profile)
    *   `X-Vault-Durability`: `STANDARD` | `CRITICAL` | `TEMPORARY` (default: `STANDARD`)
    *   `X-Idempotency-Key`: Optional UUID/string for safe retries (24h TTL; leader atomically claims after `307` redirect checks, replays cached `201` responses, and releases transient `409`/`507`/`500` claims so retries succeed)
    *   `Content-Type`: Binary media type (e.g., `application/octet-stream`); `multipart/form-data` and `application/x-www-form-urlencoded` are rejected with `415 Unsupported Media Type`
*   **Body**: Raw binary payload up to 128 MiB (`vault.storage.max-object-size-bytes`; oversized bodies return `413 Payload Too Large`)
*   **Response (201 Created)**:
    *   JSON body with `objectId`, `checksum`, `size` (`409 Conflict` if the leader lease expires or is superseded before commit, `507` on insufficient replication quorum)

#### 2. Download Object
*   **Method**: `GET /api/v1/objects?key={objectId}` or `GET /api/v1/objects/{objectId...}`
*   **Headers**: `X-API-Key`
*   **Response (200 OK)**: Raw binary file stream with `X-Vault-Checksum` and `X-Vault-Version` headers.

#### 3. Inspect Object (Health & Replica Placement)
*   **Method**: `GET /api/v1/objects/inspect?key={objectId}` or `GET /api/v1/objects/{objectId...}/inspect`
*   **Headers**: `X-API-Key`
*   **Response (200 OK)**:
```json
{
  "objectId": "reports/q3-summary.pdf",
  "version": 1,
  "sizeBytes": 204800,
  "checksumSha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "durabilityTier": "STANDARD",
  "expectedReplicas": 3,
  "actualReplicas": 3,
  "status": "ACTIVE",
  "replicaLocations": [
    { "nodeId": "11111111-1111-1111-1111-111111111111", "address": "vault-node-1:8080", "verified": true },
    { "nodeId": "22222222-2222-2222-2222-222222222222", "address": "vault-node-2:8080", "verified": true },
    { "nodeId": "33333333-3333-3333-3333-333333333333", "address": "vault-node-3:8080", "verified": true }
  ]
}
```

#### 4. List Objects
*   **Method**: `GET /api/v1/objects?prefix={prefix}&limit={limit}`
*   **Headers**: `X-API-Key`

#### 5. Delete Object
*   **Method**: `DELETE /api/v1/objects?key={objectId}` or `DELETE /api/v1/objects/{objectId...}`
*   **Headers**: `X-API-Key`
*   **Response**: `204 No Content`

---

### B. Cluster Coordination API

#### 1. Cluster Status & Topology
*   **Method**: `GET /api/v1/cluster/status`
*   **Response**:
```json
{
  "leaderId": "11111111-1111-1111-1111-111111111111",
  "leaderAddress": "vault-node-1:8080",
  "term": 1,
  "activeNodeCount": 3,
  "nodes": [
    { "nodeId": "11111111-1111-1111-1111-111111111111", "address": "vault-node-1:8080", "isLeader": true },
    { "nodeId": "22222222-2222-2222-2222-222222222222", "address": "vault-node-2:8080", "isLeader": false },
    { "nodeId": "33333333-3333-3333-3333-333333333333", "address": "vault-node-3:8080", "isLeader": false }
  ]
}
```
