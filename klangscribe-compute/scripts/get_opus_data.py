import os
import sys
import tqdm
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

# This script relies on packages in the parent directory, so we need to add it to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from resources import S3Resource, s3_resource



@dataclass
class OpusRetrievalResult:
    local_path: str = ""
    success: bool = False
    error_message: str = ""


def _download_single_opus(s3_resource: S3Resource, bucket_name: str, object_key: str, output_dir: str) -> None:
    """Helper function to download a single .opus file given its S3 object key."""
    try:
        song_name = os.path.basename(os.path.dirname(object_key))  # extract song name from path
        local_path = os.path.join(output_dir, song_name, "song.opus")
        s3_resource.download_file(bucket_name, object_key, local_path)
        return OpusRetrievalResult(local_path=local_path, success=True)
    except Exception as e:
        return OpusRetrievalResult(error_message=str(e))

def download_opus(s3_resource: S3Resource, bucket_name: str, prefix: str, output_dir: str) -> None:
    """
    Download all .opus files from a given S3 bucket/prefix to a local directory.

    It is assumed that the S3 prefix contains .opus files within subdirectories with the structure: {prefix}/{song_name}/{song}.opus
        For example: "collected_data/transformed/opus/unique_song_name/song.opus"
    Downloaded songs will be saved to {output_dir}/unique_song_name/song.opus
        For example: "/local/path/unique_song_name/song.opus"
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
    parser = argparse.ArgumentParser(description="Download .opus files from S3")
    
    # S3 bucket and prefix to list objects under
    parser.add_argument("--bucket_name", type=str, required=True, help="S3 bucket name")
    parser.add_argument("--prefix", type=str, required=True, help="S3 prefix to list objects under")
    
    # Local output directory
    parser.add_argument("--output_dir", type=str, required=True, help="Local directory to save downloaded files")
    
    args = parser.parse_args()

    download_opus(s3_resource, args.bucket_name, args.prefix, args.output_dir)


if __name__ == "__main__":
    main()