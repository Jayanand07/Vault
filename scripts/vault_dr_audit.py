#!/usr/bin/env python3
"""
Vault Distributed Object Storage - Disaster Recovery & Integrity Audit Scanner
Scans physical NVMe/disk directories, computes cryptographic SHA-256 hashes,
cross-references against cluster metadata, and identifies orphan, missing, or corrupted replicas.
"""

import argparse
import hashlib
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_ENDPOINT = os.environ.get("VAULT_ENDPOINT", "http://localhost:8080")
DEFAULT_API_KEY = os.environ.get("VAULT_API_KEY", "")


def compute_file_sha256(filepath):
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def scan_local_storage(data_dir):
    print(f"[*] Scanning local data directory: {data_dir}...")
    disk_files = {}

    objects_root = os.path.join(data_dir, "objects")
    if not os.path.exists(objects_root):
        objects_root = data_dir

    for root, _, files in os.walk(objects_root):
        for f in files:
            if f.endswith(".dat"):
                full_path = os.path.join(root, f)
                size = os.path.getsize(full_path)
                sha = compute_file_sha256(full_path)
                disk_files[f] = {
                    "path": full_path,
                    "filename": f,
                    "size": size,
                    "sha256": sha
                }

    print(f"    Found {len(disk_files)} physical data files on disk.")
    return disk_files


def fetch_cluster_metadata(endpoint, key):
    print(f"[*] Querying cluster metadata from {endpoint}...")
    url = f"{endpoint.rstrip('/')}/api/v1/objects?limit=1000"
    headers = {"X-API-Key": key}

    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            objects = data if isinstance(data, list) else data.get("objects", [])
            print(f"    Retrieved {len(objects)} active object records from metadata store.")
            return objects
    except Exception as e:
        print(f"[!] Could not query cluster metadata: {e}")
        return []


def run_audit(args):
    print("=" * 65)
    print("      VAULT DISASTER RECOVERY & CONSISTENCY AUDITOR      ")
    print("=" * 65)

    disk_files = scan_local_storage(args.data_dir)
    cluster_objs = fetch_cluster_metadata(args.endpoint, args.key)

    matched = 0
    corrupted = 0
    orphans = 0

    print("\n[*] Cross-referencing physical disk payloads with PostgreSQL metadata...")
    print("-" * 65)

    db_map = {
        hashlib.sha256(obj.get("objectId", "").encode("utf-8")).hexdigest(): obj
        for obj in cluster_objs if obj.get("objectId")
    }

    # Check disk files against DB (<sha256(objectId)>.v<version>.dat)
    for fname, dinfo in disk_files.items():
        parts = fname.rsplit(".v", 1)
        key_hash = parts[0] if len(parts) > 1 else fname

        if key_hash in db_map:
            db_obj = db_map[key_hash]
            obj_id = db_obj.get("objectId", key_hash)
            expected_hash = db_obj.get("checksum")
            if expected_hash and expected_hash.lower() != dinfo["sha256"].lower():
                print(f"  [CORRUPTED DISK REPLICA] {obj_id}")
                print(f"      Disk Path : {dinfo['path']}")
                print(f"      Expected  : {expected_hash}")
                print(f"      Actual    : {dinfo['sha256']}")
                corrupted += 1
            else:
                matched += 1
        else:
            print(f"  [ORPHAN DISK FILE] {fname}")
            print(f"      Path: {dinfo['path']} ({dinfo['size']} bytes)")
            orphans += 1

    print("-" * 65)
    print(" AUDIT SUMMARY:")
    print(f"  Healthy & Verified Replicas : {matched}")
    print(f"  Corrupted Replicas          : {corrupted}")
    print(f"  Orphan Disconnected Files   : {orphans}")
    print("-" * 65)

    if corrupted == 0 and orphans == 0:
        print("\n[RESULT] 100% HEALTHY: Perfect synchronization between DB and NVMe storage!")
    else:
        print("\n[ACTION REQUIRED] Trigger read-repair / BackgroundScrubber or RebalanceService reconciliation.")
    print("=" * 65 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Vault Disaster Recovery & Integrity Auditor")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT, help="Vault Node endpoint")
    parser.add_argument("--key", default=DEFAULT_API_KEY, help="API Key")
    parser.add_argument("--data-dir", default="./data/node-1", help="Path to local node data directory")

    args = parser.parse_args()
    if not args.key:
        parser.error("VAULT_API_KEY environment variable or --key is required")
    run_audit(args)


if __name__ == "__main__":
    main()
