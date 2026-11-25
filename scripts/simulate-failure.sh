#!/usr/bin/env bash
set -euo pipefail

echo "======================================"
echo " Vault Cluster Failure Simulation"
echo "======================================"

echo ""
echo "[1/4] Stopping vault-node-2 (simulating node failure)..."
docker compose stop vault-node-2

echo ""
echo "[2/4] Waiting 15s for lease failover / node DOWN detection..."
sleep 15

echo ""
echo "[3/4] Query cluster status with your API key:"
echo "  curl -H \"X-API-Key: \$VAULT_API_KEY\" http://localhost:8081/api/v1/cluster/status"

echo ""
echo "[4/4] Restarting vault-node-2 (simulating recovery)..."
docker compose start vault-node-2

echo ""
echo "Done. Node recovery runs via HeartbeatService, ReadRepairService, BackgroundScrubber, and RebalanceService."
