#
# file: separate_audio.py
# desc: Script to remove the vocals from a song .opus file using Demucs, and save the resulting instrumental .wav file.
#       This requires at least one high-end GPU.
#       Before running this file, you should have already downloaded the .opus files locally and have a text file listing the paths to those .opus files (one per line).
# auth: Benjamin Piro (brp8396@rit.edu)
# date: 28 February, 2026
#

import os
import sys
import time
import glob
import argparse
import polars as pl
from tqdm import tqdm
import demucs.separate
from pathlib import Path

# This script relies on packages in the parent directory, so we need to add it to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils import format_exception


def separate(file_paths: list[str], output_dir: str, device: str) -> tuple[pl.DataFrame, int, int]:
    file_statuses = []
    success: int = 0
    error: int = 0

    pbar = tqdm(file_paths, total=len(file_paths), desc="Processing files with Demucs")
    for file_path in pbar:
        try:
            demucs.separate.main([
                "--two-stems", "vocals",
                "-n", "htdemucs_ft",
                "--device", device,
                "--out", output_dir
            ] + [file_path])
            success += 1
            file_statuses.append({"file": file_path, "status": "success", "error_message": ""})
        except Exception as e:
            print(f"Error processing batch: {e}")
            error += 1
            file_statuses.append({"file": file_path, "status": "error", "error_message": format_exception(e)})
        pbar.set_postfix({"success": success, "error": error})

    file_statuses_df = pl.DataFrame(file_statuses)
    return file_statuses_df, success, error


def main():
    parser = argparse.ArgumentParser(description="Separate vocals from instrumental using Demucs.")

    parser.add_argument("--file_list", type=str, required=True, help="Path to a text file containing paths to .opus files to process, one per line.")
    parser.add_argument("--output_dir", type=str, required=True, help="Path to the output directory where separated instrumental .wav files will be saved.")
    parser.add_argument("--device", type=str, default="cuda", help="Device to run Demucs on (e.g., 'cpu', 'cuda', or 'cuda:i' if multiple GPUs are available).")
    parser.add_argument("--status_output", type=str, required=True, help="Path to save the parquet file containing processing statuses for each file.")

    # ensure that the status output path ends with .parquet
    if not parser.parse_args().status_output.endswith(".parquet"):
        parser.error("The --status_output argument must end with .parquet")

    args = parser.parse_args()

    file_paths: list[str] = []
    with open(args.file_list, 'r') as f:
        file_paths = [line.strip() for line in f if line.strip()]
        
    # create the output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)

    # separate(file_paths, args.output_dir, args.device)
    file_statuses, success, error = separate(file_paths, args.output_dir, args.device)

    # save file processing statuses to a parquet file for later analysis
    file_statuses.write_parquet(args.status_output)
    print(f"Demucs processing complete. Success: {success}, Error: {error}. Statuses saved to {args.status_output}")

if __name__== "__main__":
    main()
