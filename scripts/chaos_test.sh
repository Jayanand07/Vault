#!/bin/bash
# Vault Chaos Test Script
API_KEY="${VAULT_API_KEY:?Set VAULT_API_KEY before running chaos_test.sh}"

echo "Starting Vault Chaos Test..."

# 1. Upload a file
echo "Uploading test file..."
curl -s -L -X PUT "http://localhost:8081/api/v1/objects?key=chaos/chaos-test.txt" \
  -H "X-API-Key: ${API_KEY}" \
  -H "X-Vault-Durability: CRITICAL" \
  -H "Content-Type: application/octet-stream" \
  --data-binary "This is a chaos engineering test."

# 2. Verify file is available
echo -e "\nVerifying file is available..."
curl -s -H "X-API-Key: ${API_KEY}" "http://localhost:8081/api/v1/objects?key=chaos/chaos-test.txt"

# 3. Find leader
echo -e "\nFinding leader..."
LEADER=$(curl -s -H "X-API-Key: ${API_KEY}" http://localhost:8081/api/v1/cluster/leader | grep -o 'vault-node-[0-9]')
echo "Leader is: $LEADER"

# 4. Kill the leader
echo "Killing leader container: $LEADER..."
docker kill $LEADER

# 5. Wait for failover
echo "Waiting 15 seconds for failover..."
sleep 15

# 6. Verify file is still available from remaining nodes
echo "Verifying file is still available..."
# We try all ports in case the leader was the one we were using
curl -s -H "X-API-Key: ${API_KEY}" "http://localhost:8081/api/v1/objects?key=chaos/chaos-test.txt" || \
curl -s -H "X-API-Key: ${API_KEY}" "http://localhost:8082/api/v1/objects?key=chaos/chaos-test.txt" || \
curl -s -H "X-API-Key: ${API_KEY}" "http://localhost:8083/api/v1/objects?key=chaos/chaos-test.txt"

# 7. Bring leader back
echo -e "\nBringing old leader back..."
docker start $LEADER
sleep 10

# 8. Check cluster status
echo "Cluster status:"
curl -s -H "X-API-Key: ${API_KEY}" http://localhost:8081/api/v1/cluster/status

echo -e "\nChaos Test Complete."
