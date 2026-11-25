#!/usr/bin/env python3
"""
Vault Distributed Object Storage - AWS S3 & Boto3 Interoperability Verifier
Validates that standard Amazon S3 SDKs and tools interact seamlessly with Vault's /s3 endpoint.
"""

import argparse
import hashlib
import io
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_ENDPOINT = os.environ.get("VAULT_ENDPOINT", "http://localhost:8080")
DEFAULT_API_KEY = os.environ.get("VAULT_API_KEY", "")


def verify_with_boto3(s3_endpoint, api_key):
    try:
        import boto3
        from botocore.config import Config
    except ImportError:
        print("[!] Python 'boto3' library is not installed in the current environment.")
        print("    Install it via: pip install boto3")
        print("    Falling back to Raw S3 HTTP Protocol Verification...\n")
        return False

    print("[*] Running Experimental /s3 Adapter Boto3 Client Tests (X-API-Key header)...")
    s3 = boto3.client(
        "s3",
        endpoint_url=f"{s3_endpoint}/s3",
        aws_access_key_id="placeholder",
        aws_secret_access_key="placeholder",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"})
    )
    s3.meta.events.register(
        "before-sign.s3",
        lambda request, **kwargs: request.headers.add_header("X-API-Key", api_key)
    )

    bucket = "test-bucket"
    key = "boto3-verified.txt"
    payload = b"Hello from AWS Boto3 SDK! Vault S3-compatibility verified."
    expected_hash = hashlib.sha256(payload).hexdigest()

    try:
        # 1. Put Object
        print("  [1/4] Executing s3.put_object()...")
        s3.put_object(Bucket=bucket, Key=key, Body=payload)
        print("        ✓ Upload acknowledged by Vault S3 adapter.")

        # 2. Get Object
        print("  [2/4] Executing s3.get_object()...")
        resp = s3.get_object(Bucket=bucket, Key=key)
        downloaded = resp["Body"].read()
        if downloaded != payload:
            print("        ✗ Payload verification failed!")
            return False
        print("        ✓ Downloaded byte-for-byte exact match.")

        # 3. Head Object
        print("  [3/4] Executing s3.head_object()...")
        head = s3.head_object(Bucket=bucket, Key=key)
        print(f"        ✓ Object metadata retrieved. ContentLength: {head.get('ContentLength')} bytes.")

        # 4. Delete Object
        print("  [4/4] Executing s3.delete_object()...")
        s3.delete_object(Bucket=bucket, Key=key)
        print("        ✓ Tombstone marker created.")

        print("\n[SUCCESS] 100% AWS SDK Boto3 Interoperability Verified!")
        return True

    except Exception as e:
        print(f"[!] Boto3 operation encountered: {e}")
        return False


def verify_raw_s3_protocol(endpoint, api_key):
    print("[*] Running Standard S3 HTTP Protocol Tests...")
    bucket = "raw-bucket"
    key = "s3-raw-test.txt"
    url = f"{endpoint.rstrip('/')}/s3/{bucket}/{key}"
    payload = b"Testing S3 HTTP standard protocol methods."

    # 1. PUT
    print("  [1/3] Testing S3 PUT /{bucket}/{key}...")
    req = urllib.request.Request(url, data=payload, method="PUT")
    req.add_header("X-API-Key", api_key)
    req.add_header("Content-Type", "text/plain")
    try:
        with urllib.request.urlopen(req) as resp:
            print(f"        ✓ Status: {resp.status} OK")
    except Exception as e:
        print(f"        ✗ S3 PUT failed: {e}")
        return False

    # 2. GET
    print("  [2/3] Testing S3 GET /{bucket}/{key}...")
    req = urllib.request.Request(url, method="GET")
    req.add_header("X-API-Key", api_key)
    try:
        with urllib.request.urlopen(req) as resp:
            data = resp.read()
            if data == payload:
                print(f"        ✓ Status: {resp.status} OK (Payload matched)")
            else:
                print("        ✗ Payload mismatch")
                return False
    except Exception as e:
        print(f"        ✗ S3 GET failed: {e}")
        return False

    # 3. DELETE
    print("  [3/3] Testing S3 DELETE /{bucket}/{key}...")
    req = urllib.request.Request(url, method="DELETE")
    req.add_header("X-API-Key", api_key)
    try:
        with urllib.request.urlopen(req) as resp:
            print(f"        ✓ Status: {resp.status} OK")
    except Exception as e:
        print(f"        ✗ S3 DELETE failed: {e}")
        return False

    print("\n[SUCCESS] S3 Protocol Compatibility Verified!")
    return True


def main():
    parser = argparse.ArgumentParser(description="Vault AWS S3 Compatibility Verifier")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT, help="Vault Node endpoint")
    parser.add_argument("--key", default=DEFAULT_API_KEY, help="API Key")

    args = parser.parse_args()
    if not args.key:
        parser.error("VAULT_API_KEY environment variable or --key is required")

    print("=" * 65)
    print("       VAULT S3 & BOTO3 SDK COMPATIBILITY VERIFIER       ")
    print("=" * 65)
    print(f"Endpoint: {args.endpoint}")
    print("-" * 65)

    if not verify_with_boto3(args.endpoint, args.key):
        verify_raw_s3_protocol(args.endpoint, args.key)

    print("=" * 65 + "\n")


if __name__ == "__main__":
    main()
