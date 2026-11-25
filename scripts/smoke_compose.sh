#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

CREATED_ENV=0
CREATED_SECRET=0

cleanup() {
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  if [ "$CREATED_ENV" -eq 1 ]; then
    rm -f .env
  fi
  if [ "$CREATED_SECRET" -eq 1 ]; then
    rm -f secrets/db_password.txt
  fi
}
trap cleanup EXIT

rand_token() {
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 16
  else
    od -An -tx1 -N16 /dev/urandom | tr -d ' \n'
  fi
}

mkdir -p secrets
if [ ! -f secrets/db_password.txt ]; then
  rand_token > secrets/db_password.txt
  CREATED_SECRET=1
fi

SMOKE_API_KEY="$(rand_token)"
SMOKE_NODE_SECRET="$(rand_token)"
SMOKE_GRAFANA_VAL="$(rand_token)"

if [ ! -f .env ]; then
  cat > .env <<EOF
VAULT_NODE_SECRET=${SMOKE_NODE_SECRET}
VAULT_API_KEYS=${SMOKE_API_KEY}
GF_SECURITY_ADMIN_PASSWORD=${SMOKE_GRAFANA_VAL}
EOF
  CREATED_ENV=1
else
  # Read first configured key if .env already exists locally
  SMOKE_API_KEY="$(grep -E '^VAULT_API_KEYS=' .env | head -n1 | cut -d= -f2- | cut -d, -f1 | tr -d '\r')"
fi

echo "[1/6] Starting PostgreSQL + 3 Vault nodes..."
docker compose up -d --build postgres vault-node-1 vault-node-2 vault-node-3

echo "[2/6] Waiting for all 3 nodes to pass /actuator/health..."
for port in 8081 8082 8083; do
  ready=0
  for _ in $(seq 1 45); do
    if curl -fsS "http://localhost:${port}/actuator/health" | grep -q '"status":"UP"'; then
      ready=1
      break
    fi
    sleep 2
  done
  if [ "$ready" -ne 1 ]; then
    echo "ERROR: Node on port ${port} failed to become healthy"
    docker compose logs
    exit 1
  fi
done

echo "[3/6] Waiting for 3 active nodes and an elected leader..."
elected=0
for _ in $(seq 1 30); do
  STATUS_JSON="$(curl -fsS -H "X-API-Key: ${SMOKE_API_KEY}" http://localhost:8081/api/v1/cluster/status || true)"
  if echo "$STATUS_JSON" | grep -q '"activeNodeCount":3' && ! echo "$STATUS_JSON" | grep -q '"leaderId":"UNKNOWN"'; then
    elected=1
    echo "Cluster status: ${STATUS_JSON}"
    break
  fi
  sleep 2
done
if [ "$elected" -ne 1 ]; then
  echo "ERROR: Cluster failed to elect a leader with 3 active nodes"
  docker compose logs
  exit 1
fi

echo "[4/6] Executing PUT, idempotency replay, GET, and /inspect..."
TMP_IN="$(mktemp)"
TMP_OUT="$(mktemp)"
printf "vault-compose-smoke-payload-%s\n" "$(date +%s)" > "$TMP_IN"
EXPECTED_SHA="$(sha256sum "$TMP_IN" | awk '{print $1}')"
IDEM_KEY="smoke-idem-$(date +%s)-$$"

PUT_CODE="$(curl -sS -L -o /dev/null -w "%{http_code}" -X PUT \
  "http://localhost:8081/api/v1/objects?key=smoke/hello.txt" \
  -H "X-API-Key: ${SMOKE_API_KEY}" \
  -H "X-Idempotency-Key: ${IDEM_KEY}" \
  -H "X-Vault-Durability: STANDARD" \
  -H "Content-Type: application/octet-stream" \
  --data-binary "@${TMP_IN}")"
[ "$PUT_CODE" -eq 201 ] || { echo "Expected 201 PUT, got ${PUT_CODE}"; exit 1; }

REPLAY_HEADERS="$(mktemp)"
REPLAY_CODE="$(curl -sS -L -D "$REPLAY_HEADERS" -o /dev/null -w "%{http_code}" -X PUT \
  "http://localhost:8081/api/v1/objects?key=smoke/hello.txt" \
  -H "X-API-Key: ${SMOKE_API_KEY}" \
  -H "X-Idempotency-Key: ${IDEM_KEY}" \
  -H "X-Vault-Durability: STANDARD" \
  -H "Content-Type: application/octet-stream" \
  --data-binary "@${TMP_IN}")"
[ "$REPLAY_CODE" -eq 201 ] || { echo "Expected 201 replay, got ${REPLAY_CODE}"; exit 1; }
grep -qi "X-Idempotency-Replayed: true" "$REPLAY_HEADERS" || { echo "Missing X-Idempotency-Replayed header"; exit 1; }
rm -f "$REPLAY_HEADERS"

curl -fsS -H "X-API-Key: ${SMOKE_API_KEY}" "http://localhost:8081/api/v1/objects/smoke/hello.txt" -o "$TMP_OUT"
ACTUAL_SHA="$(sha256sum "$TMP_OUT" | awk '{print $1}')"
[ "$EXPECTED_SHA" = "$ACTUAL_SHA" ] || { echo "SHA mismatch: $EXPECTED_SHA != $ACTUAL_SHA"; exit 1; }

INSPECT_JSON="$(curl -fsS -H "X-API-Key: ${SMOKE_API_KEY}" "http://localhost:8081/api/v1/objects/inspect?key=smoke/hello.txt")"
echo "Inspect: ${INSPECT_JSON}"
echo "$INSPECT_JSON" | grep -q '"status":"ACTIVE"' || { echo "Inspect missing ACTIVE status"; exit 1; }

echo "[5/6] Simulating on-disk bit-rot on vault-node-1 and verifying peer fallback read..."
docker exec vault-node-1 sh -c 'find /data/vault/objects -name "*.dat" -exec sh -c "dd if=/dev/zero of={} bs=1 count=4 conv=notrunc 2>/dev/null" \;'
curl -fsS -H "X-API-Key: ${SMOKE_API_KEY}" "http://localhost:8081/api/v1/objects/smoke/hello.txt" -o "$TMP_OUT"
FALLBACK_SHA="$(sha256sum "$TMP_OUT" | awk '{print $1}')"
[ "$EXPECTED_SHA" = "$FALLBACK_SHA" ] || { echo "Bit-rot fallback served corrupted bytes!"; exit 1; }

echo "[6/6] Deleting object and verifying 404..."
DEL_CODE="$(curl -sS -L -o /dev/null -w "%{http_code}" -X DELETE \
  "http://localhost:8081/api/v1/objects?key=smoke/hello.txt" \
  -H "X-API-Key: ${SMOKE_API_KEY}")"
[ "$DEL_CODE" -eq 204 ] || { echo "Expected 204 DELETE, got ${DEL_CODE}"; exit 1; }

GET_AFTER_DEL="$(curl -sS -o /dev/null -w "%{http_code}" \
  -H "X-API-Key: ${SMOKE_API_KEY}" \
  "http://localhost:8081/api/v1/objects?key=smoke/hello.txt")"
[ "$GET_AFTER_DEL" -eq 404 ] || { echo "Expected 404 after delete, got ${GET_AFTER_DEL}"; exit 1; }

rm -f "$TMP_IN" "$TMP_OUT"
echo "[SUCCESS] 3-node Compose smoke test passed!"
