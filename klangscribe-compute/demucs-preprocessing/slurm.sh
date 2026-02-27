#!/bin/bash -l

#SBATCH --job-name=DemucsPreprocessing
#SBATCH --comment="Source separation preprocessing using Demucs"

#SBATCH --account=brp8396
#SBATCH --partition=tier3

#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err

#SBATCH --mail-user=slack:@brp8396
#SBATCH --mail-type=END,FAIL

#SBATCH --gres=gpu:a100:1
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=24:00:00
#SBATCH --array=0-19

spack load ffmpeg@7.0.2

source /home/brp8396/venvs/demucs/bin/activate

INPUT_DIR=/path/to/opus
OUTPUT_DIR=/path/to/output
NUM_TASKS=20

python run_demucs_partition.py \
    --input_dir $INPUT_DIR \
    --output_dir $OUTPUT_DIR \
    --task_id $SLURM_ARRAY_TASK_ID \
    --num_tasks $NUM_TASKS \
    --batch_size 16