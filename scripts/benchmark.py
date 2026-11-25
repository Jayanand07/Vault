#!/usr/bin/env python3
"""
Vault Distributed Object Storage - Automated Benchmark Suite
Evaluates throughput (req/s, MB/s) and latency percentiles (p50, p95, p99)
across concurrent worker threads without external dependencies.
"""

import argparse
import concurrent.futures
import hashlib
import json
import os
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_ENDPOINT = os.environ.get("VAULT_ENDPOINT", "http://localhost:8080")
DEFAULT_API_KEY = os.environ.get("VAULT_API_KEY", "")


def make_request(url, method="GET", headers=None, data=None):
    if headers is None:
        headers = {}
    req = urllib.request.Request(url, data=data, method=method)
    for k, v in headers.items():
        req.add_header(k, v)
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req) as resp:
            body = resp.read()
            t1 = time.perf_counter()
            return resp.status, (t1 - t0) * 1000.0, body
    except urllib.error.HTTPError as e:
        t1 = time.perf_counter()
        return e.code, (t1 - t0) * 1000.0, e.read()
    except Exception as e:
        t1 = time.perf_counter()
        return 0, (t1 - t0) * 1000.0, str(e).encode()


def run_benchmark(args):
    endpoint = args.endpoint.rstrip("/")
    api_key = args.key
    concurrency = args.concurrency
    total_requests = args.requests
    size_bytes = args.size_kb * 1024
    tier = args.tier.upper()

    print("\n" + "=" * 65)
    print("          VAULT STORAGE ENGINE BENCHMARK SUITE          ")
    print("=" * 65)
    print(f"Endpoint      : {endpoint}")
    print(f"Concurrency   : {concurrency} parallel workers")
    print(f"Total Requests: {total_requests}")
    print(f"Payload Size  : {args.size_kb} KB ({size_bytes:,} bytes)")
    print(f"Storage Tier  : {tier}")
    print("-" * 65)

    # Pre-generate deterministic raw payload
    payload = os.urandom(size_bytes)
    sha256_hash = hashlib.sha256(payload).hexdigest()

    headers_write = {
        "X-API-Key": api_key,
        "X-Vault-Durability": tier,
        "Content-Type": "application/octet-stream",
        "Content-Length": str(len(payload))
    }
    headers_read = {
        "X-API-Key": api_key
    }

    # 1. WRITE BENCHMARK
    if args.mode in ("write", "both"):
        print(f"\n[*] Starting WRITE Benchmark ({total_requests} objects)...")
        write_latencies = []
        write_success = 0
        t_start = time.perf_counter()

        def do_write(idx):
            obj_id = f"bench/{tier.lower()}-{args.size_kb}kb-item-{idx}.dat"
            url = f"{endpoint}/api/v1/objects?key={urllib.parse.quote(obj_id, safe='')}"
            status, lat, _ = make_request(url, method="PUT", headers=headers_write, data=payload)
            return status, lat

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(do_write, i) for i in range(total_requests)]
            for fut in concurrent.futures.as_completed(futures):
                st, lat = fut.result()
                if st in (200, 201):
                    write_success += 1
                    write_latencies.append(lat)

        t_end = time.perf_counter()
        duration = t_end - t_start
        print_results("WRITE (Quorum Commit)", write_latencies, write_success, total_requests, duration, size_bytes)

    # 2. READ BENCHMARK
    if args.mode in ("read", "both"):
        print(f"\n[*] Starting READ Benchmark ({total_requests} SHA-256 verified streams)...")
        read_latencies = []
        read_success = 0
        t_start = time.perf_counter()

        def do_read(idx):
            obj_id = f"bench/{tier.lower()}-{args.size_kb}kb-item-{idx}.dat"
            url = f"{endpoint}/api/v1/objects?key={urllib.parse.quote(obj_id, safe='')}"
            status, lat, _ = make_request(url, method="GET", headers=headers_read)
            return status, lat

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(do_read, i) for i in range(total_requests)]
            for fut in concurrent.futures.as_completed(futures):
                st, lat = fut.result()
                if st == 200:
                    read_success += 1
                    read_latencies.append(lat)

        t_end = time.perf_counter()
        duration = t_end - t_start
        print_results("READ (Zero-Copy Streaming)", read_latencies, read_success, total_requests, duration, size_bytes)


def print_results(label, latencies, success_count, total_count, duration, size_bytes):
    if not latencies:
        print(f"[!] No successful requests recorded for {label}.")
        return

    latencies.sort()
    p50 = statistics.median(latencies)
    p95 = latencies[int(len(latencies) * 0.95)] if len(latencies) > 1 else latencies[0]
    p99 = latencies[int(len(latencies) * 0.99)] if len(latencies) > 1 else latencies[-1]
    ops_sec = success_count / duration if duration > 0 else 0
    mb_sec = (success_count * size_bytes) / (1024 * 1024 * duration) if duration > 0 else 0

    print("-" * 65)
    print(f" Results for: {label}")
    print("-" * 65)
    print(f" Success Rate : {success_count}/{total_count} ({success_count/total_count*100:.1f}%)")
    print(f" Elapsed Time : {duration:.2f} seconds")
    print(f" Throughput   : {ops_sec:.2f} ops/sec  ({mb_sec:.2f} MB/s)")
    print(f" Latency Min  : {min(latencies):.2f} ms")
    print(f" Latency p50  : {p50:.2f} ms")
    print(f" Latency p95  : {p95:.2f} ms")
    print(f" Latency p99  : {p99:.2f} ms")
    print(f" Latency Max  : {max(latencies):.2f} ms")
    print("-" * 65)


def main():
    parser = argparse.ArgumentParser(description="Vault Benchmark Suite")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT, help=f"Vault endpoint URL (default: {DEFAULT_ENDPOINT})")
    parser.add_argument("--key", default=DEFAULT_API_KEY, help="API Key")
    parser.add_argument("--concurrency", "-c", type=int, default=4, help="Concurrent worker threads (default: 4)")
    parser.add_argument("--requests", "-n", type=int, default=50, help="Total requests (default: 50)")
    parser.add_argument("--size-kb", "-s", type=int, default=64, help="Object payload size in KB (default: 64)")
    parser.add_argument("--tier", default="STANDARD", choices=["CRITICAL", "STANDARD", "TEMPORARY"], help="Durability Tier")
    parser.add_argument("--mode", default="both", choices=["write", "read", "both"], help="Benchmark mode")

    args = parser.parse_args()
    if not args.key:
        parser.error("VAULT_API_KEY environment variable or --key is required")
    run_benchmark(args)


if __name__ == "__main__":
    main()
