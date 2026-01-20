from typing import Any, Dict, Iterable, List, Optional

import boto3
from botocore.exceptions import ClientError


class S3BucketClient:
    """Small wrapper for common S3 operations in a single bucket."""

    def __init__(
        self,
        bucket_name: str,
        region_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        client: Optional[Any] = None,
    ) -> None:
        self.bucket_name = bucket_name
        self._client = client or boto3.client(
            "s3",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )

    def upload_file(
        self, local_path: str, key: str, extra_args: Optional[Dict[str, Any]] = None
    ) -> None:
        """Upload a local file to S3, replacing the object if it already exists."""
        if extra_args:
            self._client.upload_file(
                local_path, self.bucket_name, key, ExtraArgs=extra_args
            )
            return
        self._client.upload_file(local_path, self.bucket_name, key)

    def upload_bytes(
        self,
        data: bytes,
        key: str,
        content_type: Optional[str] = None,
        extra_args: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Upload bytes to S3, replacing the object if it already exists."""
        params: Dict[str, Any] = {"Bucket": self.bucket_name, "Key": key, "Body": data}
        if content_type:
            params["ContentType"] = content_type
        if extra_args:
            params.update(extra_args)
        self._client.put_object(**params)

    def upload_text(
        self,
        text: str,
        key: str,
        encoding: str = "utf-8",
        extra_args: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Upload text content to S3 using the provided encoding."""
        self.upload_bytes(text.encode(encoding), key, extra_args=extra_args)

    def download_file(self, key: str, local_path: str) -> None:
        """Download an S3 object to a local file path."""
        self._client.download_file(self.bucket_name, key, local_path)

    def get_object_bytes(self, key: str) -> bytes:
        """Fetch an object from S3 and return its raw bytes."""
        response = self._client.get_object(Bucket=self.bucket_name, Key=key)
        return response["Body"].read()

    def get_object_text(self, key: str, encoding: str = "utf-8") -> str:
        """Fetch an object from S3 and decode it as text."""
        return self.get_object_bytes(key).decode(encoding)

    def delete_object(self, key: str) -> None:
        """Delete a single object from the bucket."""
        self._client.delete_object(Bucket=self.bucket_name, Key=key)

    def delete_objects(self, keys: Iterable[str]) -> int:
        """Delete multiple objects and return how many were removed."""
        key_list = [key for key in keys if key]
        if not key_list:
            return 0

        deleted = 0
        for chunk in self._chunk_keys(key_list, 1000):
            response = self._client.delete_objects(
                Bucket=self.bucket_name,
                Delete={"Objects": [{"Key": key} for key in chunk], "Quiet": True},
            )
            deleted += len(response.get("Deleted", []))
        return deleted

    def list_objects(self, prefix: str = "", max_keys: Optional[int] = None) -> List[str]:
        """List object keys in the bucket."""
        keys: List[str] = []
        paginator = self._client.get_paginator("list_objects_v2")
        pagination_config: Dict[str, Any] = {}
        if max_keys is not None:
            pagination_config["MaxItems"] = max_keys

        for page in paginator.paginate(
            Bucket=self.bucket_name, Prefix=prefix, PaginationConfig=pagination_config
        ):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
                if max_keys is not None and len(keys) >= max_keys:
                    return keys
        return keys

    def object_exists(self, key: str) -> bool:
        """Check whether an object exists without downloading it."""
        try:
            self._client.head_object(Bucket=self.bucket_name, Key=key)
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        return True

    def copy_object(
        self,
        source_key: str,
        dest_key: str,
        source_bucket: Optional[str] = None,
        extra_args: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Copy an object inside the bucket or from another bucket."""
        copy_source = {
            "Bucket": source_bucket or self.bucket_name,
            "Key": source_key,
        }
        params: Dict[str, Any] = {
            "Bucket": self.bucket_name,
            "Key": dest_key,
            "CopySource": copy_source,
        }
        if extra_args:
            params.update(extra_args)
        self._client.copy_object(**params)

    def move_object(
        self,
        source_key: str,
        dest_key: str,
        source_bucket: Optional[str] = None,
        extra_args: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Move an object by copying then deleting the source."""
        self.copy_object(source_key, dest_key, source_bucket, extra_args)
        if source_bucket and source_bucket != self.bucket_name:
            self._client.delete_object(Bucket=source_bucket, Key=source_key)
            return
        self.delete_object(source_key)

    def generate_presigned_url(
        self, key: str, expires_in: int = 3600, method: str = "get_object"
    ) -> str:
        """Generate a pre-signed URL for an object."""
        return self._client.generate_presigned_url(
            method,
            Params={"Bucket": self.bucket_name, "Key": key},
            ExpiresIn=expires_in,
        )

    @staticmethod
    def _chunk_keys(keys: List[str], chunk_size: int) -> Iterable[List[str]]:
        for index in range(0, len(keys), chunk_size):
            yield keys[index : index + chunk_size]
