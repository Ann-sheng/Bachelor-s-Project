# Provider-agnostic cloud object storage client

from __future__ import annotations
import boto3
import os
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Base interface
# ---------------------------------------------------------------------------

class CloudStorage(ABC):
    """Abstract base: all cloud clients implement this interface."""

    @abstractmethod
    def upload(self, local_path: str, remote_key: str) -> str:
        """Upload local_path → remote_key. Returns remote URI."""

    @abstractmethod
    def download(self, remote_key: str, local_path: str) -> str:
        """Download remote_key → local_path. Returns local_path."""

    @abstractmethod
    def list_files(self, prefix: str = "") -> List[str]:
        """Return list of object keys matching prefix."""

    @abstractmethod
    def exists(self, remote_key: str) -> bool:
        """Return True if object exists."""

    @abstractmethod
    def get_uri(self, remote_key: str) -> str:
        """Return human-readable URI for display / logging."""


# ---------------------------------------------------------------------------
# AWS S3  +  MinIO
# ---------------------------------------------------------------------------

class S3Storage(CloudStorage):

    def __init__(self) -> None:
        provider  = os.environ.get("CLOUD_PROVIDER", "s3").lower()
        is_minio  = (provider == "minio")

        if is_minio:
            endpoint    = os.environ["MINIO_ENDPOINT"]
            access      = os.environ["MINIO_ACCESS_KEY"]
            secret      = os.environ["MINIO_SECRET_KEY"]
            self.bucket = os.environ["MINIO_BUCKET"]

            self._s3 = boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=access,
                aws_secret_access_key=secret,
                config=Config(
                    signature_version="s3v4",
                    s3={"addressing_style": "path"},
                ),
            )

            self._uri_prefix = f"{endpoint}/{self.bucket}"

        else:
            self.bucket = os.environ["S3_BUCKET"]

            self._s3 = boto3.client(
                "s3",
                aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
                region_name=os.environ.get("AWS_REGION", "us-east-1"),
            )

            self._uri_prefix = f"s3://{self.bucket}"

        if is_minio:
            self._ensure_bucket()

    # 🔥 MUST BE INSIDE CLASS
    def _ensure_bucket(self) -> None:
        from botocore.exceptions import ClientError

        try:
            self._s3.list_objects_v2(Bucket=self.bucket, MaxKeys=1)
        except ClientError as e:
            code = e.response["Error"]["Code"]

            if code in ("NoSuchBucket", "404"):
                self._s3.create_bucket(Bucket=self.bucket)
                log.info("Created bucket: %s", self.bucket)
            else:
                raise

    # ---- ALL METHODS BELOW MUST ALSO BE INSIDE CLASS ----

    def upload(self, local_path: str, remote_key: str) -> str:
        size_mb = Path(local_path).stat().st_size / 1e6
        log.info("Uploading %.1f MB → %s", size_mb, remote_key)
        self._s3.upload_file(local_path, self.bucket, remote_key)
        return self.get_uri(remote_key)

    def download(self, remote_key: str, local_path: str) -> str:
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        self._s3.download_file(self.bucket, remote_key, local_path)
        return local_path

    def list_files(self, prefix: str = "") -> List[str]:
        keys = []
        kwargs = {"Bucket": self.bucket, "Prefix": prefix}

        while True:
            resp = self._s3.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []):
                keys.append(obj["Key"])

            if not resp.get("IsTruncated"):
                break

            kwargs["ContinuationToken"] = resp["NextContinuationToken"]

        return keys

    def exists(self, remote_key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self._s3.head_object(Bucket=self.bucket, Key=remote_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return False
            raise

    def get_uri(self, remote_key: str) -> str:
        return f"{self._uri_prefix}/{remote_key}"


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def get_cloud_client() -> CloudStorage:
    provider = os.environ.get("CLOUD_PROVIDER", "minio").lower()
    mapping: dict = {
        "s3":    S3Storage,
        "minio": S3Storage,
    }
    if provider not in mapping:
        raise ValueError(
            f"Unknown CLOUD_PROVIDER='{provider}'. "
            f"Choose from: {list(mapping.keys())}"
        )
    log.info("Cloud provider: %s", provider)
    return mapping[provider]()
