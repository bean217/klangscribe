# KlangScribe Compute Scripts

This Directory contains scripts for
1) retrieving KlangScribe dataset files from S3, 
2) processing them using Demucs (htdemucs_ft) to remove vocal audio, 
3) compressing the resulting instrumental .wav files and moving them into a target directory

The purpose of these scripts is to aid in construction of a canonical dataset for training KlangScribe.

These scripts are intended to be run on remote machines where GPUs and plenty of disk storage are available for (temporary) intermediate data storage.

## Description of Scripts:

### (1) `get_opus_data.py`

**Purpose:** To download `.opus` files from an S3 bucket

**Arguments:**
* `bucket_name`
    * Name of the bucket in the MinIO/S3 endpoint
    * Example: `--bucket_name data-collection`
* `prefix`
    * Path prefix to opus files, relative to the bucket
    * Example: `--prefix transformed/opus`
* `output_dir`
    * The local directory to save downloaded files (relative to where the script is run)
    * Example: `--output_dir ./opus_files`


**Assumptions:**
This script assumes that individual `.opus` files are saved in individual directories, where the directory name is the song name.

Example: `s3://data-collection/transformed/opus/{song_name}/song.opus`


**Side Effects:**
`.opus` files will be downloaded locally to `output_dir` similar to their storage structure in S3,

Example: `{output_dir}/{song_name}/song.opus`


**Example Execution:**
```bash
python get_opus_data.py \
    --bucket_name data-collection \
    --prefix transformed/opus \
    --output_dir ./opus_files
```

---

### (2) `get_canonical_dataset.py`

**Purpose:** To download the canonical dataset from an S3 bucket

**Arguments:**
* `bucket_name`
    * Name of the bucket in the MinIO/S3 endpoint
    * Example: `--bucket_name data-collection`
* `manifest_key`
    * Path to the canonical dataset manifest parquet in the S3 bucket (relative to `bucket_name`)
    * Example: `--manifest_key manifests/manifest_canonical_{uuid}.parquet`
* `metadata_key`
    * path to the canonical data metadata parquet in the S3 bucket (relative to `bucket_name`)
    * Example: `--metadata_key canonical/ini_metadata_{uuid}.parquet`
* `output_dir`
    * The local directory to save downloaded files (relative to where the script is run)
    * Example: `--output_dir ./canonical`

**Assumptions:**
This script assumes that `manifest_key` and `metadata_key` are consistent with each other. In other words, they refer to the same version of the data


**Side Effects:**
All `.opus` and `.npz` files retrieved from S3 will be stored with the filename `sid_{dir_id}.{ext}`.
* `dir_id` refers to its index in the `metadata_key` parquet file. 

`.opus` files from `{bucket_name}/transformed/opus` will be downloaded locally to `{output_dir}/full_songs`

`.opus` files from `{bucket_name}/transformed/instrumentals` will be downloaded locally to `{output_dir}/instr_songs`

`.npz` files from `{bucket_name}/transformed/charts` will be downloaded locally to `{output_dir}/charts`

Example: Song with ID=`514` 
* `{output_dir}/full_songs/sid_514.opus`
* `{output_dir}/instr_songs/sid_514.opus`
* `{output_dir}/charts/sid_514.npz`

The parquet file stored at `metadata_key` is also saved locally at the top level of `{output_dir}`

Example: `ini_metadata_{uuid}.parquet` is saved to `{output_dir}/ini_metadata_{uuid}.parquet`


**Example Execution:**
```bash
python get_canonical_dataset.py \
    --bucket_name data-collection \
    --manifest_key manifests/manifest_canonical_af29d3e7-281f-4954-837b-48a91064f0ae.parquet \
    --metadata_key canonical/ini_metadata_af29d3e7-281f-4954-837b-48a91064f0ae.parquet \
    --output_dir ./canonical
```

---

### (3) `separate_audio.py`

**Purpose:** To separate the vocal and instrumental audio respectivaly from a local directory of full song `.opus` files to 

**Arguments:**
* `file_list`
    * `.txt` file where each line is a path to an `.opus` to process
    * Example: `--file_list file_list.txt`
* `device`
    * The device to run the `htdemucs_ft` de-stemming model on
    * Examples: 
        * `--device cpu` to run on CPU (not recommended)
        * `--device cuda` to run on GPU
        * `--device cuda:i` (`i` is a cuda device ID, e.g., `0`)
* `output_dir`
    * The local directory to save processed files (relative to where the script is run)
    * Example: `--output_dir ./separated`
* `status_output`
    * The location of a local parquet manifest, which includes status information about whether a particular file was successfully processed or failed
    * Example: `--status_output ./sample_sepstats.parquet`


**Assumptions:**
This script assumes that the file paths listed in `file_list` are all valid audio files that can be processed by `htdemucs_ft`. A `file_list` can be created by running:
```bash
ls path/to/opuses/*.opus > file_list.txt
```


**Side Effects:**
All processed files are saved to `{output_path}/htdemucs_ft`.

Demucs produces 2 files, `vocals.wav` and `no_vocals.wav`, which are stored in a directory with the same name as the original `.opus` file.

Example: `canonical/full_songs/sid_514.opus` results are saved to `{output_path}/htdemucs_ft/sid_514`

The `status_output` manifest is also saved locally, as described in the arguments section.


**Example Execution:**
```bash
python separate_audio.py \
    --file_list file_list.txt \
    --output_dir ./separated \
    --device cuda:0 \
    --status_output ./sample_sepstats.parquet
```

---

### (4) `format_separated.py`

**Purpose:** To compress the output of the `separate_audio.py` (3) script.

**Arguments:**
* `input_dir`
    * Path to the input directory containing the separated audio files from Demucs
    * Example: `--input_dir ./separated/htdemuct_ft`
* `output_dir`
    * The local directory to save compressed `.opus` files
    * Example: `--output_dir ./instrumentals`
* `status_output`
    * The location of a local parquet manifest, which includes status information about whether a particular file was successfully compressed or failed
    * Example: `--status_output ./sample_instrstats.parquet`


**Assumptions:**
This script assumes that `inut_dir` contains directories of `vocals.wav` and `no_vocals.wav` files, with the directory name being the song name.


**Side Effects:**
All processed files are saved to `output_path`.

Each file saved in the top-level of `output_path` is an `.opus` file with the name `sid_{dir_id}.opus`.

Example: Instrmental audio of song at `{input_dir}/sid_514` is saved to `{output_dir}/sid_514.opus`

**Example Execution:**
```bash
python compress_separated.py \
    --input_dir ./separated/htdemuct_ft \
    --output_dir ./instrumentals \
    --status_output ./sample_instrstats.parquet
```

---


## Workflow Instructions

**Purpose:** To construct a canonical dataset directory.

Specifics for running each step can be found above.

**Step 1:**
Download the canonical dataset from S3 by running `get_canonical_dataset.py`.

**Step 2:**
De-stem full song opus files by running `separate_audio.py`

**Step 3:**
Compress instrumental audio files by running `compress_separated.py`

After running this step, copy the contents of the `output_dir` to `canonical/instr_songs`:

```bash
cd output_dir
cp -r ./ ../canonical/instr_songs/
```
