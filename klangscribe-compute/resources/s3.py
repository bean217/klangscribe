import os
import boto3
import dotenv
from pathlib import Path
dotenv.load_dotenv(Path(__file__).parent.parent / ".env")


class S3Resource:
    """Resource for interacting with an S3 object storage server."""

    def __init__(self, endpoint: str, access_key: str, secret_key: str, region: str = "us-east-1"):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self._client = None

    def get_client(self):
        """Get boto3 S3 client (cached)."""
        if self._client is None:
            self._client = boto3.client(
                's3',
                endpoint_url=self.endpoint,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region,
            )
        return self._client
    
    def download_file(self, bucket_name: str, object_key: str, local_path: str) -> None:
        """Download a file from S3 to a local path."""
        s3_client = self.get_client()
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3_client.download_file(bucket_name, object_key, local_path)

    def get_object(self, bucket_name: str, object_key: str) -> bytes:
        """Get an object from S3 as bytes."""
        s3_client = self.get_client()
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        return response['Body'].read()
    
    def exists(self, bucket_name: str, object_key: str) -> bool:
        """Check if an object exists in S3."""
        s3_client = self.get_client()
        try:
            s3_client.head_object(Bucket=bucket_name, Key=object_key)
            return True
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise


s3_endpoint_url = os.getenv("S3_ENDPOINT_URL")
s3_access_key = os.getenv("S3_ACCESS_KEY")
s3_secret_key = os.getenv("S3_SECRET_KEY")
s3_region = os.getenv("S3_REGION", "us-east-1")

if not all([s3_endpoint_url, s3_access_key, s3_secret_key]):
    raise ValueError("Missing S3 connection parameters. Please set S3_ENDPOINT_URL, S3_ACCESS_KEY, and S3_SECRET_KEY in the environment or .env file.")

s3_resource = S3Resource(
    endpoint=s3_endpoint_url,
    access_key=s3_access_key,
    secret_key=s3_secret_key,
    region=s3_region,
)