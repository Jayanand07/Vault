#!/bin/bash
# Vault Chaos Test Script

echo "Starting Vault Chaos Test..."

# 1. Upload a file
echo "Uploading test file..."
curl -s -X PUT http://localhost:8081/api/v1/objects/chaos-test.txt \
  -H "X-Vault-Durability: CRITICAL" \
  -d "This is a chaos engineering test."

# 2. Verify file is available
echo -e "\nVerifying file is available..."
curl -s http://localhost:8081/api/v1/objects/chaos-test.txt

# 3. Find leader
echo -e "\nFinding leader..."
LEADER=$(curl -s http://localhost:8081/api/v1/cluster/leader | grep -o 'vault-node-[0-9]')
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
curl -s http://localhost:8081/api/v1/objects/chaos-test.txt || \
curl -s http://localhost:8082/api/v1/objects/chaos-test.txt || \
curl -s http://localhost:8083/api/v1/objects/chaos-test.txt

# 7. Bring leader back
echo -e "\nBringing old leader back..."
docker start $LEADER
sleep 10

# 8. Check cluster status
echo "Cluster status:"
curl -s http://localhost:8081/api/v1/cluster/status

echo -e "\nChaos Test Complete."
