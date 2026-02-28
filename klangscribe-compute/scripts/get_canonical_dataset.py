import os
import sys
import argparse
import polars as pl
from tqdm import tqdm
from pathlib import Path
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

# This script relies on packages in the parent directory, so we need to add it to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from resources import S3Resource, s3_resource
from utils import format_exception

@dataclass
class ObjectLocation:
    bucket: str
    prefix: str
    key: str

    def get_s3_path(self) -> str:
        return f"{self.prefix}/{self.key}" if self.prefix else self.key


def retrieve_parquet(s3_resource: S3Resource, bucket_name: str, key: str) -> pl.DataFrame:
    """
    Retrieve manifest parquet file from S3 and load into Polars DataFrame.
    """
    try:
        manifest_bytes = s3_resource.get_object(bucket_name, key)
        manifest_df = pl.read_parquet(manifest_bytes)
        return manifest_df
    except Exception as e:
        print(f"Error retrieving or parsing manifest from S3: {format_exception(e)}")
        raise


def download_single_song(
    s3_resource: S3Resource, 
    song_id: str,
    chart_loc: ObjectLocation,
    full_opus_loc: ObjectLocation,
    instr_opus_loc: ObjectLocation,
    output_dir: str
) -> None:
    """
    Helper function to download a single song given its S3 object key.
    """
    try:
        # (1) Create Output directories if they don't exist
        # create full_songs directory if it doesn't exist
        os.makedirs(os.path.join(output_dir, "full_songs"), exist_ok=True)
        # create instr_songs directory if it doesn't exist
        os.makedirs(os.path.join(output_dir, "instr_songs"), exist_ok=True)
        # create charts directory if it doesn't exist
        os.makedirs(os.path.join(output_dir, "charts"), exist_ok=True)

        # (2) Download song files from S3 to local output directory
        # construct paths
        full_opus_local_path = os.path.join(output_dir, "full_songs", f"sid_{song_id}.opus")
        instr_opus_local_path = os.path.join(output_dir, "instr_songs", f"sid_{song_id}.opus")
        chart_local_path = os.path.join(output_dir, "charts", f"sid_{song_id}.npz")
        # download files
        s3_resource.download_file(full_opus_loc.bucket, full_opus_loc.get_s3_path(), full_opus_local_path)
        # s3_resource.download_file(instr_opus_loc.bucket, instr_opus_loc.get_s3_path(), instr_opus_local_path)
        s3_resource.download_file(chart_loc.bucket, chart_loc.get_s3_path(), chart_local_path)
    except Exception as e:
        print(f"Error downloading a song: {format_exception(e)}")
        # cleanup: remove any partially downloaded files before re-raising
        for path in [full_opus_local_path, instr_opus_local_path, chart_local_path]:
            if os.path.exists(path):
                os.remove(path)
        raise

def download_songs(s3_resource: S3Resource, manifest_df: pl.DataFrame, output_dir: str) -> None:
    """
    Download all songs listed in the manifest DataFrame from S3 to a local directory using multithreading for efficiency.
    """

    # Create Output directories if they don't exist

    success: int = 0
    error: int = 0

    with ThreadPoolExecutor() as exec:
        futures = []
        for row in manifest_df.iter_rows(named=True):
            song_id = row["dir_id"]
            chart_loc = ObjectLocation(
                bucket=row["song_chart_bucket"],
                prefix=row["song_chart_prefix"],
                key=row["song_chart_key"]
            )
            full_opus_loc = ObjectLocation(
                bucket=row["song_opus_bucket"],
                prefix=row["song_opus_prefix"],
                key=row["song_opus_key"]
            )
            # TODO: add instr_opus_loc once we have the data in place
            instr_opus_loc = None
            # instr_opus_loc = ObjectLocation(
            #     bucket=row["instr_opus_bucket"],
            #     prefix=row["instr_opus_prefix"],
            #     key=row["instr_opus_key"]
            # )
            futures.append(
                exec.submit(
                    download_single_song,
                    s3_resource,        # s3 resource for downloading from S3
                    song_id,            # unique song identifier (used for naming local files)
                    chart_loc,          # S3 location of the song's .npz chart file
                    full_opus_loc,      # S3 location of the song's full .opus file
                    instr_opus_loc,     # S3 location of the song's instrument .opus file
                    output_dir          # local output directory to save downloaded files
                )
            )

        with tqdm(as_completed(futures), total=len(futures), desc="Downloading songs") as pbar:
            for future in pbar:
                try:
                    future.result()
                    success += 1
                except Exception as e:
                    print(f"Error downloading a song: {e}")
                    error += 1
                pbar.set_postfix({"success": success, "error": error})


def download_cannonical(
    s3_resource: S3Resource, 
    bucket_name: str, 
    manifest_key: str,
    metadata_key: str,
    output_dir: str
) -> None:
    """
    Download canonical dataset files from a given S3 bucket/prefix to a local directory.
    """
    # create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # fetch manifest parquet file from S3
    manifest_df = retrieve_parquet(s3_resource, bucket_name, manifest_key)

    # retrieve all songs listed in the manifest and save to output_dir
    download_songs(s3_resource, manifest_df, output_dir)

    # download metadata parquet file from S3
    metadata_local_path = os.path.join(output_dir, "song_metadata.parquet")
    s3_resource.download_file(bucket_name, metadata_key, metadata_local_path)
    


def main():
    parser = argparse.ArgumentParser(description="Download canonical dataset files from S3")
    
    # S3 bucket and prefix to list objects under
    parser.add_argument("--bucket_name", type=str, required=True, help="S3 bucket name")
    
    # canonical dataet manifest parquet file key (relative to bucket)
    parser.add_argument("--manifest_key", type=str, required=True, help="S3 key for the canonical dataset manifest parquet file")
    # song metadata parquet file key (relative to bucket)
    parser.add_argument("--metadata_key", type=str, required=True, help="S3 key for the song metadata parquet file")

    # Local output directory
    parser.add_argument("--output_dir", type=str, required=True, help="Local directory to save downloaded files")
    
    args = parser.parse_args()

    download_cannonical(s3_resource, args.bucket_name, args.manifest_key, args.metadata_key, args.output_dir)


if __name__ == "__main__":
    main()
