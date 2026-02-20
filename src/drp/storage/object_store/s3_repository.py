import json
from pathlib import Path
from typing import Any

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from drp.config.settings import Settings
from drp.core.exceptions import StorageError


class S3Repository:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = self._build_client()

    def _build_client(self) -> BaseClient:
        session = boto3.session.Session()
        return session.client(
            "s3",
            endpoint_url=self._settings.object_store_endpoint_url,
            region_name=self._settings.object_store_region,
            aws_access_key_id=self._settings.object_store_access_key_id,
            aws_secret_access_key=self._settings.object_store_secret_access_key,
            use_ssl=self._settings.object_store_secure,
        )

    def ensure_bucket(self) -> None:
        bucket = self._settings.object_store_bucket
        try:
            self._client.head_bucket(Bucket=bucket)
            return
        except ClientError as exc:
            error_code = str(exc.response.get("Error", {}).get("Code", ""))
            if error_code not in {"404", "NoSuchBucket"}:
                raise StorageError(f"Unable to access object store bucket '{bucket}': {exc}") from exc

        try:
            self._client.create_bucket(Bucket=bucket)
        except ClientError as exc:
            raise StorageError(f"Unable to create object store bucket '{bucket}': {exc}") from exc

    def put_json(self, key: str, payload: dict[str, Any]) -> str:
        body = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
        return self.put_bytes(key=key, payload=body, content_type="application/json")

    def put_bytes(self, key: str, payload: bytes, content_type: str = "application/octet-stream") -> str:
        self.ensure_bucket()
        bucket = self._settings.object_store_bucket
        try:
            self._client.put_object(
                Bucket=bucket,
                Key=key,
                Body=payload,
                ContentType=content_type,
            )
        except ClientError as exc:
            raise StorageError(f"Failed uploading object s3://{bucket}/{key}: {exc}") from exc
        return f"s3://{bucket}/{key}"

    def upload_file(self, local_path: str, key: str) -> str:
        self.ensure_bucket()
        bucket = self._settings.object_store_bucket
        path = Path(local_path)
        if not path.exists():
            raise StorageError(f"Cannot upload missing file: {local_path}")

        try:
            self._client.upload_file(str(path), bucket, key)
        except ClientError as exc:
            raise StorageError(f"Failed uploading file to s3://{bucket}/{key}: {exc}") from exc
        return f"s3://{bucket}/{key}"
