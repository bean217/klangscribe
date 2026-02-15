import os

# Required before importing boto3:
os.environ['AWS_REQUEST_CHECKSUM_CALCULATION'] = 'when_required'
os.environ['AWS_RESPONSE_CHECKSUM_VALIDATION'] = 'when_required'
import boto3

from botocore.exceptions import ClientError
from typing import Optional

import dagster as dg
    

class S3Resource(dg.ConfigurableResource):
    """Resource for interacting with an S3 object storage server."""

    endpoint: Optional[str] = None
    access_key: str
    secret_key: str
    region: str = "us-east-1"

    def get_client(self):
        """Get boto3 S3 client"""
        return boto3.client(
            's3',
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
        )
    
    def _validate_bucket(self, s3_client, bucket_name: str) -> None:
        # Create bucket if it doesn't exist
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                # Bucket doesn't exist, create it
                try:
                    if self.region == 'us-east-1':
                        s3_client.create_bucket(Bucket=bucket_name)
                    else:
                        s3_client.create_bucket(
                            Bucket=bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': self.region}
                        )
                except ClientError as create_error:
                    raise Exception(f"Failed to create bucket {bucket_name}: {create_error}")
            else:
                raise Exception(f"Failed to check bucket {bucket_name}: {e}")

    def upload_file(self, bucket_name: str, object_key: str, file_path: str) -> str:
        """Uploads a file to S3-compatible storage"""
        s3_client = self.get_client()

        # Ensure bucket exists
        self._validate_bucket(s3_client, bucket_name)

        # Upload the file
        s3_client.upload_file(file_path, bucket_name, object_key)

        return f"{bucket_name}/{object_key}"
    
    def put_bytes(self, bucket_name: str, obj_key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        s3_client = self.get_client()

        # Ensure bucket exists
        self._validate_bucket(s3_client, bucket_name)

        # Write bytes
        s3_client.put_object(Bucket=bucket_name, Key=obj_key, Body=data, ContentType=content_type)
