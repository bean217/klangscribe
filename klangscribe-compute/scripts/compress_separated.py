#
# file: compress_separated.py
# desc: Script to compress the output of the separate_audio.py script.
#       This involves:
#       1) Deleting the vocals.wav files that Demucs creates (we only care about the instrumental .wav files).
#       2) Converting the instrumental .wav files to .opus format (for storage efficiency) and saving them in a new directory.
# auth: Benjamin Piro (brp8396@rit.edu)
# date: 28 February, 2026
#

import os
import sys
import glob
import ffmpeg
import argparse
import polars as pl
from tqdm import tqdm
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# This script relies on packages in the parent directory, so we need to add it to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils import format_exception

def convert_wav_to_opus(wav_path: str, opus_path: str) -> None:
    try:
        (
            ffmpeg
            .input(wav_path)
            .output(opus_path, ab='96k')
            .overwrite_output()
            .run()
        )
        return {"input_file": wav_path, "output_file": opus_path, "status": "success", "error_message": ""}
    except Exception as e:
        return {"input_file": wav_path, "output_file": opus_path, "status": "error", "error_message": format_exception(e)}


def process_instrumental_files(instrumental_files: list[str], output_dir: str) -> pl.DataFrame:
    file_statuses = []
    success = 0
    error = 0

    # ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    with ThreadPoolExecutor() as exec:
        futures = []
        for wav_path in instrumental_files:
            # construct corresponding .opus output path, using the wav directory name as song name
            song_id = os.path.basename(os.path.dirname(wav_path))
            opus_path = os.path.join(output_dir, f"{song_id}.opus")

            futures.append(exec.submit(convert_wav_to_opus, wav_path, opus_path))
        
        with tqdm(as_completed(futures), total=len(futures), desc="Converting .wav to .opus") as pbar:
            for future in pbar:
                result = future.result()
                file_statuses.append(result)
                if result["status"] == "success":
                    success += 1
                else:
                    error += 1
            pbar.set_postfix({"success": success, "error": error})

    return pl.DataFrame(file_statuses), success, error


def main():
    parser = argparse.ArgumentParser(description="Format the output of the separate_audio.py script by deleting vocals and converting instrumental .wav files to .opus format.")

    parser.add_argument("--input_dir", type=str, required=True, help="Path to the input directory containing the separated audio files from Demucs (should contain subdirectories for each song with vocals.wav and instrumental.wav files).")
    parser.add_argument("--output_dir", type=str, required=True, help="Path to the output directory where formatted .opus files will be saved.")
    parser.add_argument("--status_output", type=str, required=True, help="Path to save the parquet file containing processing statuses for each file.")

    # ensure that the status output path ends with .parquet
    if not parser.parse_args().status_output.endswith(".parquet"):
        parser.error("The --status_output argument must end with .parquet")

    args = parser.parse_args()

    # collect all instrumental .wav files
    instrumental_files = glob.glob(os.path.join(args.input_dir, "*", "no_vocals.wav"))

    file_status, success, error = process_instrumental_files(instrumental_files, args.output_dir)

    # save file processing statuses to a parquet file for later analysis
    file_status.write_parquet(args.status_output)
    print(f"Formatting complete. Success: {success}, Error: {error}")


if __name__ == "__main__":
    main()
