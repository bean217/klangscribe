import os
import sys
import tqdm
import argparse
import polars as pl
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

# This script relies on packages in the parent directory, so we need to add it to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from resources import S3Resource, s3_resource



def _download_single_chart(song_id, chart_path, s3_resource, output_dir) -> None:
    """Helper function to download a single .chart file given its S3 path."""
    try:
        local_path = os.path.join(output_dir, f"sid_{song_id}.chart")
        bucket, key = chart_path.split("/", 1)  # split "bucket/key" into bucket and key
        s3_resource.download_file(bucket, key, local_path)
        return {"song_id": song_id, "chart_path": chart_path, "status": "success", "error_message": ""}
    except Exception as e:
        return {"song_id": song_id, "chart_path": chart_path, "status": "error", "error_message": str(e)}

def download_charts(s3_resource: S3Resource, chart_df, output_dir: str) -> None:
    """
    Download all .chart files according to the given chart dataframe.

    The chart dataframe contains "dir_id" and "chart_path" columns, 
    where "dir_id" is the unique identifier for each song and "chart_path" is the S3 bucket/path to the .chart file.
    """

    # create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    success: int = 0
    error: int = 0

    with ThreadPoolExecutor() as exec:
        futures = [
            exec.submit(_download_single_chart, song_id, chart_path, s3_resource, output_dir)
            for song_id, chart_path in zip(chart_df["dir_id"], chart_df["chart_path"])
        ]

        for future in tqdm.tqdm(as_completed(futures), total=len(futures), desc="Downloading .chart files"):
            result = future.result()
            if result["status"] == "success":
                success += 1
            else:
                error += 1
                print(f"Error downloading .chart file: (sid={result['song_id']}, chart_path={result['chart_path']}) {result['error_message']}")
    print(f"Downloaded {success} .chart files successfully, {error} errors.")


def main():
    parser = argparse.ArgumentParser(description="Download .chart files from S3")
    
    # metadata parquet containing song IDs
    parser.add_argument("--raw_manifest", type=str, required=True, help="Path to parquet file containing raw collected data manifest used to fetch chart files")
    parser.add_argument("--canonical_manifest", type=str, required=True, help="Path to parquet file containing canonical manifest with song IDs, used to filter out chart files not in the canonical manifest")
    # Local output directory
    parser.add_argument("--output_dir", type=str, required=True, help="Local directory to save downloaded files")
    
    args = parser.parse_args()

    # read in raw manifest to get song IDs and chart paths
    raw_manifest_df = pl.read_parquet(args.raw_manifest).select(["dir_id", "chart_path"])
    canonical_manifest_df = pl.read_parquet(args.canonical_manifest).select("dir_id")
    # filter raw manifest to only include songs in the canonical manifest
    filtered_manifest_df = raw_manifest_df.join(canonical_manifest_df, on="dir_id", how="inner")

    download_charts(s3_resource, filtered_manifest_df, args.output_dir)


if __name__ == "__main__":
    main()