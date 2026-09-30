#!/bin/bash
# Vault Benchmark Script

echo "Generating random payload (1MB)..."
dd if=/dev/urandom of=test_1mb.dat bs=1M count=1 2>/dev/null

echo "Running sequential write benchmark (100 objects)..."
START_TIME=$(date +%s)
for i in {1..100}; do
    curl -s -X PUT http://localhost:8081/api/v1/objects/bench-test-$i.dat \
        -H "X-Vault-Durability: STANDARD" \
        --data-binary @test_1mb.dat > /dev/null
    echo -n "."
done
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo -e "\nSequential write 100x 1MB completed in ${DURATION}s."
if [ $DURATION -gt 0 ]; then
    echo "Throughput: $((100 / DURATION)) objects/sec"
fi

echo "Running sequential read benchmark (100 objects)..."
START_TIME=$(date +%s)
for i in {1..100}; do
    curl -s http://localhost:8081/api/v1/objects/bench-test-$i.dat > /dev/null
    echo -n "."
done
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo -e "\nSequential read 100x 1MB completed in ${DURATION}s."
if [ $DURATION -gt 0 ]; then
    echo "Throughput: $((100 / DURATION)) objects/sec"
fi

rm test_1mb.dat
echo "Benchmark Complete."
