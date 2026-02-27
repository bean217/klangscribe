import boto3
import os
import io
import tqdm
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import dotenv
from typing import Optional
from dataclasses import dataclass


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


@dataclass
class OpusRetrievalResult:
    local_path: str = ""
    success: bool = False
    error_message: str = ""


def _download_single_opus(s3_resource: S3Resource, bucket_name: str, object_key: str, output_dir: str) -> None:
    """Helper function to download a single .opus file given its S3 object key."""
    try:
        song_name = os.path.basename(os.path.dirname(object_key))  # extract song name from path
        local_path = os.path.join(output_dir, f"{song_name}.opus")
        s3_resource.download_file(bucket_name, object_key, local_path)
        return OpusRetrievalResult(local_path=local_path, success=True)
    except Exception as e:
        return OpusRetrievalResult(error_message=str(e))

def download_opus(s3_resource: S3Resource, bucket_name: str, prefix: str, output_dir: str) -> None:
    """
    Download all .opus files from a given S3 bucket/prefix to a local directory.

    It is assumed that the S3 prefix contains .opus files within subdirectories with the structure: {prefix}/{song_name}/{song}.opus
        For example: "collected_data/transformed/opus/unique_song_name/song.opus"
    Downloaded songs will be saved to {output_dir}/{song_name}.opus
        For example: "/local/path/unique_song_name.opus"
    """

    # create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # list all .opus files under the given prefix
    client = s3_resource.get_client()
    paginator = client.get_paginator('list_objects_v2')
    opus_keys = [
        obj['Key']
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        for obj in page.get('Contents', [])
        if obj['Key'].endswith('.opus')
    ]

    success: int = 0
    error: int = 0

    with ThreadPoolExecutor() as exec:
        futures = [
            exec.submit(_download_single_opus, s3_resource, bucket_name, opus_key, output_dir)
            for opus_key in opus_keys
        ]

        for future in tqdm.tqdm(as_completed(futures), total=len(futures), desc="Downloading .opus files"):
            result = future.result()
            if result.success:
                success += 1
            else:
                error += 1
                print(f"Error downloading .opus file: {result.error_message}")

    print(f"Downloaded {success} .opus files successfully, {error} errors.")


def main():
    # Load local .env file
    dotenv.load_dotenv(str(Path(__file__).parent / ".env"))

    parser = argparse.ArgumentParser(description="Download .opus files from S3")
    
    # S3 connection parameters
    parser.add_argument("--s3_endpoint", type=str, required=True, help="S3 endpoint URL")
    parser.add_argument("--s3_access_key", type=str, required=True, help="S3 access key")
    parser.add_argument("--s3_secret_key", type=str, required=True, help="S3 secret key")
    parser.add_argument("--s3_region", type=str, default="us-east-1", help="S3 region")
    
    # S3 bucket and prefix to list objects under
    parser.add_argument("--bucket_name", type=str, required=True, help="S3 bucket name")
    parser.add_argument("--prefix", type=str, required=True, help="S3 prefix to list objects under")
    
    # Local output directory
    parser.add_argument("--output_dir", type=str, required=True, help="Local directory to save downloaded files")
    
    args = parser.parse_args()

    s3_resource = S3Resource(
        endpoint=args.s3_endpoint,
        access_key=args.s3_access_key,
        secret_key=args.s3_secret_key,
        region=args.s3_region,
    )

    download_opus(s3_resource, args.bucket_name, args.prefix, args.output_dir)


if __name__ == "__main__":
    main()