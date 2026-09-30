#!/bin/bash

echo "======================================"
echo " Vault Cluster Failure Simulation"
echo "======================================"

echo ""
echo "[1/4] Stopping vault-node-2 (Simulating a crash)..."
docker-compose stop vault-node-2

echo ""
echo "[2/4] Waiting for 15 seconds to allow the cluster to elect a new leader (if necessary) and mark the node as DEAD..."
sleep 15

echo ""
echo "[3/4] Check the logs of node-1 or node-3 to see the ring rebalance."
echo "You can query the cluster status:"
echo "  curl http://localhost:8081/api/v1/cluster/status"

echo ""
echo "[4/4] Restarting vault-node-2 (Simulating recovery)..."
docker-compose start vault-node-2

echo ""
echo "Done. The node will rejoin the cluster, sync its data via Merkle Trees, and be healthy again!"
