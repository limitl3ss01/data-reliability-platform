#!/usr/bin/env bash
set -euo pipefail

docker compose exec -T pipeline python - <<'PY'
import boto3
import os

endpoint = os.getenv("OBJECT_STORE_ENDPOINT_URL")
bucket = os.getenv("OBJECT_STORE_BUCKET")
access_key = os.getenv("OBJECT_STORE_ACCESS_KEY_ID")
secret_key = os.getenv("OBJECT_STORE_SECRET_ACCESS_KEY")
secure = os.getenv("OBJECT_STORE_SECURE", "false").lower() == "true"
region = os.getenv("OBJECT_STORE_REGION", "us-east-1")

client = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    use_ssl=secure,
    region_name=region,
)

resp = client.list_objects_v2(Bucket=bucket)
for obj in resp.get("Contents", []):
    print(f'{obj["LastModified"]}  {obj["Size"]:>8}  {obj["Key"]}')
PY
