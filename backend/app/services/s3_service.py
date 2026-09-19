"""
S3 Service - JanStream Resilient Ingestion
Handles S3 Bucket management, direct client Presigned Upload URLs, and object fetching.
"""

import os
import uuid
import logging
from typing import Dict, Any, Optional
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger("janstream.s3")


class S3Service:
    def __init__(self, bucket_name: Optional[str] = None, region_name: str = "us-east-1"):
        self.bucket_name = bucket_name or os.environ.get("S3_BUCKET_NAME", "janstream-ingest-bucket")
        self.region_name = os.environ.get("AWS_DEFAULT_REGION", region_name)
        
        session_token = os.environ.get("AWS_SESSION_TOKEN")
        access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        
        # When running in AWS Lambda, pass session token or use default credential provider
        if access_key and access_key != "test_access_key":
            client_kwargs = {
                "region_name": self.region_name,
                "aws_access_key_id": access_key,
                "aws_secret_access_key": secret_key,
            }
            if session_token:
                client_kwargs["aws_session_token"] = session_token
            self.s3_client = boto3.client("s3", **client_kwargs)
        else:
            try:
                self.s3_client = boto3.client("s3", region_name=self.region_name)
            except Exception:
                self.s3_client = boto3.client(
                    "s3",
                    region_name=self.region_name,
                    aws_access_key_id="test_access_key",
                    aws_secret_access_key="test_secret_key"
                )

    def generate_presigned_upload_url(
        self,
        filename: str,
        content_type: str = "application/octet-stream",
        expires_in_seconds: int = 300,
        max_file_size_bytes: int = 15 * 1024 * 1024  # 15 MB limit
    ) -> Dict[str, Any]:
        """
        Generates a secure S3 presigned POST URL.
        Allows the frontend/client to upload directly to S3 without sending file bytes through our backend server.
        """
        submission_id = str(uuid.uuid4())
        file_ext = filename.split(".")[-1] if "." in filename else "bin"
        s3_key = f"uploads/{submission_id}/{filename}"

        try:
            presigned_post = self.s3_client.generate_presigned_post(
                Bucket=self.bucket_name,
                Key=s3_key,
                Fields={"Content-Type": content_type},
                Conditions=[
                    {"Content-Type": content_type},
                    ["content-length-range", 100, max_file_size_bytes]  # Min 100B, Max 15MB
                ],
                ExpiresIn=expires_in_seconds
            )

            logger.info(f"Generated presigned upload URL for key: {s3_key} (submission_id={submission_id})")
            return {
                "submission_id": submission_id,
                "s3_key": s3_key,
                "bucket": self.bucket_name,
                "upload_url": presigned_post["url"],
                "fields": presigned_post["fields"],
                "expires_in_seconds": expires_in_seconds
            }
        except ClientError as e:
            logger.error(f"Failed to generate presigned URL: {e}")
            raise RuntimeError(f"S3 Presigned URL error: {str(e)}")

    def generate_presigned_put_url(
        self,
        filename: str,
        content_type: str = "text/plain",
        expires_in_seconds: int = 300
    ) -> Dict[str, Any]:
        """
        Generates a direct S3 Presigned PUT URL.
        Includes STS temporary session token in the query string automatically.
        """
        submission_id = str(uuid.uuid4())
        s3_key = f"uploads/{submission_id}/{filename}"

        try:
            url = self.s3_client.generate_presigned_url(
                ClientMethod="put_object",
                Params={
                    "Bucket": self.bucket_name,
                    "Key": s3_key,
                    "ContentType": content_type
                },
                ExpiresIn=expires_in_seconds
            )
            logger.info(f"Generated presigned PUT URL for key: {s3_key}")
            return {
                "submission_id": submission_id,
                "s3_key": s3_key,
                "bucket": self.bucket_name,
                "upload_url": url,
                "method": "PUT",
                "content_type": content_type,
                "expires_in_seconds": expires_in_seconds
            }
        except ClientError as e:
            logger.error(f"Failed to generate presigned PUT URL: {e}")
            raise RuntimeError(f"S3 Presigned PUT error: {str(e)}")

    def put_object(self, s3_key: str, data: bytes, content_type: str = "text/plain") -> bool:
        """Saves object bytes directly into the S3 bucket."""
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=data,
                ContentType=content_type
            )
            logger.info(f"Successfully saved object to s3://{self.bucket_name}/{s3_key}")
            return True
        except Exception as e:
            logger.error(f"Failed to put object {s3_key} into bucket {self.bucket_name}: {e}")
            return False

    def get_object_bytes(self, s3_key: str) -> bytes:
        """Fetches object bytes from S3 for worker processing."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            return response["Body"].read()
        except ClientError as e:
            logger.error(f"Failed to fetch object {s3_key} from bucket {self.bucket_name}: {e}")
            raise
