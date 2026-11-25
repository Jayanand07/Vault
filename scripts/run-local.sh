#!/usr/bin/env bash
set -e

MODE="${1:-single}"
NODES="${2:-3}"

echo "=================================================="
echo "         VAULT DISTRIBUTED OBJECT STORAGE         "
echo "=================================================="
echo "Target Mode: $MODE"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

if [ "$MODE" = "cluster" ]; then
    if command -v docker >/dev/null 2>&1 && docker ps >/dev/null 2>&1; then
        echo "[*] Starting 3-node cluster via Docker Compose..."
        docker compose up -d --build
        echo ""
        echo "[SUCCESS] Cluster is running!"
        echo "  - Web Console: http://localhost:8081/console.html"
        echo "  - Node 1:      http://localhost:8081"
        echo "  - Node 2:      http://localhost:8082"
        echo "  - Node 3:      http://localhost:8083"
        echo "  - Grafana:     http://localhost:3000 (use GF_SECURITY_ADMIN_PASSWORD from .env)"
        exit 0
    else
        echo "[!] Docker daemon not reachable. Falling back to single-node mode."
        MODE="single"
    fi
fi

echo "[*] Building project with Maven..."
if [ -f "./apache-maven-3.9.6/bin/mvn" ]; then
    ./apache-maven-3.9.6/bin/mvn package -DskipTests -q
else
    mvn package -DskipTests -q
fi

DATA_DIR="$PROJECT_ROOT/data/node-1"
mkdir -p "$DATA_DIR"

export VAULT_NODE_ID="11111111-1111-1111-1111-111111111111"
export VAULT_NODE_ADDRESS="localhost:8080"
export VAULT_DATA_DIR="$DATA_DIR"
export SPRING_PROFILES_ACTIVE="dev"

echo ""
echo "=================================================="
echo " Starting Vault Node (dev profile) on http://localhost:8080"
echo " Web Console: http://localhost:8080/console.html"
echo "=================================================="
java -jar "$PROJECT_ROOT/vault-node/target/vault-node-1.0-SNAPSHOT.jar"
