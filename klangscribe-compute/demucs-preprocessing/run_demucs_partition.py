import os
import glob
from argparse import ArgumentParser
import subprocess


def main():
    parser = ArgumentParser(description="Preprocess audio files for source separation using Demucs.")

    parser.add_argument("--input_dir", type=str, required=True, help="Path to the input directory containing audio files to preprocess.")
    parser.add_argument("--output_dir", type=str, required=True, help="Path to the output directory where preprocessed files will be saved.")
    parser.add_argument("--task_id", type=int, required=True)
    parser.add_argument("--num_tasks", type=int, required=True)
    parser.add_argument("--batch_size", type=int, default=16)

    args = parser.parse_args()

    # collect files
    files = sorted(glob.glob(os.path.join(args.input_dir, "*.opus")))
    n = len(files)

    # partition across SLURM tasks
    chunk = (n + args.num_tasks - 1) // args.num_tasks
    start = args.task_id * chunk
    end = min(start + chunk, n)

    subset = files[start:end]

    print(f"Task {args.task_id}: {len(subset)} files to process")

    # process files in batches
    for i in range(0, len(subset), args.batch_size):
        batch = subset[i:i + args.batch_size]
        
        print(f"Running demucs on batch of {len(batch)} files")

        cmd = [
            "python", "-m", "demucs.separate",
            "-n", "htdemucs_ft",
            "--two-stems", "vocals",
            "--device", "cuda",
            "--out", args.output_dir,
        ] + batch

        subprocess.run(cmd, check=True)

        # remove vocals
        for f in batch:
            track = os.path.splitext(os.path.basename(f))[0]
            vocals = os.path.join(
                args.output_dir,
                "htdemucs_ft",
                track,
                "vocals.wav"
            )
            if os.path.exists(vocals):
                os.remove(vocals)



if __name__ == "__main__":
    main()