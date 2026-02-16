import os
import io
import re
import json
import time
import traceback
from ...utils.audio_processing import opus_to_wav_bytes, merge_wav_bytes
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import dagster as dg
import polars as pl
import pyarrow as pa
from ..resources import PostgresResource, S3Resource


# Acceptable extensions
AUDIO_EXTS = (".opus",)
CHART_EXTS = (".chart",)     # omitting .mid extension, due to chart conversion issues
INI_EXTS = (".ini",)
# Aggregate these extensions together
REQUIRED_EXTS = AUDIO_EXTS + CHART_EXTS + INI_EXTS


######################
#   Manifest Asset   #
######################

#### Helpers ####


def _lname(f: Dict[str, Any]) -> str:
    return str(f.get("filename", "")).strip().lower()


def _filter_required(files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [f for f in files if _lname(f).endswith(REQUIRED_EXTS)]


def _remove_s3_uuid_prefix(storage_path) -> str:
    """
    Removes the "s3://uns/<uuid>" prefix of a storage path.
    """
    s3_uuid_prefix_pattern = r'^s3://uns/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
    return re.sub(s3_uuid_prefix_pattern, '', storage_path).strip()


def _pick_single_storage_path(files: List[Dict[str, Any]], ext: str) -> Optional[str]:
    """
    Returns a storage path for .chart and .ini which should be singletons,
    but if multple exist, choose deterministically (largest file_size, then filename).
    """
    candidates = [f for f in files if _lname(f).endswith(ext)]
    if not candidates:
        return None
    candidates.sort(key=lambda x: (int(x.get("file_size", 0) or 0), _lname(x)), reverse=True)
    storage_path = str(candidates[0].get("storage_path"))
    return _remove_s3_uuid_prefix(storage_path)


def _sorted_opus_paths(files: List[Dict[str, Any]]) ->List[str]:
    opus = [f for f in files if _lname(f).endswith(".opus")]
    opus.sort(key=lambda x: (_lname(x), str(x.get("storage_path", ""))))
    storage_paths = [str(f.get("storage_path")) for f in opus if f.get("storage_path")]
    return [_remove_s3_uuid_prefix(sp) for sp in storage_paths]


def _collect_valid_songs(result_rows: list) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Iterates through song records in `directory_metadata` table from KlangScribe PostgreSQL database,
    keeping only songs which contain required files (e.g., .ini/.chart/.opus files).
    
    Returns:
        - rows dictionary with schema:
        {
            "dir_id": int,              # ID of the song in Postgres
            "dirname": str,             # Name of the song directory stored in S3
            "ini_path": str,            # S3 bucket/key string to a single expected .ini file (containing song metadata)
            "chart_path": str,          # S3 bucket/key string to a single expected .chart file (containing song note sequence info)
            "opus_paths": List[str],    # List of S3 bucket/key strings to 1+ .opus files (containing song audio)
            "uploaded_at": str          # UTC datetime string of when the song was stored into S3
        }

        - metadata dictionary with schema:
        {
            "total_songs": int,         # Total number of songs processed from PostgreSQL
            "kept_dirs": int,           # Number of songs kept
            "dropped_missing": int      # Number of sings dropped due to missing necessary files
        }
    """
    rows: List[Dict[str, Any]] = []

    total_songs: int = 0
    kept_dirs: int = 0
    dropped_missing: int = 0

    # iterate through songs (rows)
    for dir_id, dirname, _, _, files_json, uploaded_at in result_rows:
        total_songs += 1
    
        files: List[str] = _filter_required(files_json)
        opus_paths = _sorted_opus_paths(files)
        chart_path = _pick_single_storage_path(files, ".chart")
        ini_path = _pick_single_storage_path(files, ".ini")

        if not opus_paths or not chart_path or not ini_path:
            dropped_missing += 1
            continue

        kept_dirs += 1
        rows.append(
            {
                "dir_id": dir_id,
                "dirname": dirname,
                "ini_path": ini_path,
                "chart_path": chart_path,
                "opus_paths": json.dumps(opus_paths),
                "uploaded_at": uploaded_at.strftime("%Y-%m-%d %H:%M:%S")
            }
        )
    
    metadata={
        "total_songs": total_songs,
        "kept_dirs": kept_dirs,
        "dropped_missing": dropped_missing
    }

    return rows, metadata


def _store_manifest_parquet_to_s3(s3: S3Resource, rows: List[Dict[str, Any]], run_id: str) -> Tuple[str, str]:
    """
    Writes validated song records to a collected songs manifest .parquet file and stores in s3,
    using (bucket='data-collection', prefix='manifests').

    Returns:
        - String of the bucket name where the manifest was stored
        - String of the object key where the manifest was stored
    """
    
    # Build Polars DataFrame
    df = pl.DataFrame(rows, schema={
        "dir_id": pl.Utf8,
        "dirname": pl.Utf8,
        "ini_path": pl.Utf8,
        "chart_path": pl.Utf8,
        "opus_paths": pl.Utf8,
        "uploaded_at": pl.Utf8,
    })

    # Write parquet to bytes (S3 resource handles storage)
    buf = io.BytesIO()
    df.write_parquet(buf, compression="zstd")
    data = buf.getvalue()


    manifest_name = f"manifest_{run_id}.parquet"
    bucket = 'data-collection'
    manifest_key = f"manifests/{manifest_name}"
    s3.put_bytes(bucket_name=bucket, obj_key=manifest_key, data=data, content_type="application/octet-stream")

    return bucket, manifest_key


#### Asset Definition ####


@dg.asset(
    key=dg.AssetKey(["raw", "manifest_parquet"]),
    kinds={"python", "s3", "postgres", "parquet"}
)
def raw_manifest_parquet(
    context: dg.AssetExecutionContext,
    pg: PostgresResource,
    s3: S3Resource
) -> dg.MaterializeResult:
    """
    Defines a manifest parquet asset including all song directories with valid essential files.
    Included filetypes:
      - .opus files (list of CH audio files)
      - .chart (Clone Hero chart file)
      - .ini (Clone Hero song metadata file)
    """
    
    query: str = """
        SELECT
            id AS dir_id,
            dirname AS dirname,
            file_count AS file_count,
            total_size AS total_size,
            files_json AS files_json, 
            uploaded_at AS uploaded_at
        FROM
            directory_metadata
        LIMIT
            1000
    """

    # list of songs from data collection metadata table
    result_rows = pg.fetchall(query)

    # Filter song records missing the proper files
    rows, metadata = _collect_valid_songs(result_rows)

    total_songs = metadata["total_songs"]
    kept_dirs = metadata["kept_dirs"]
    dropped_missing = metadata["dropped_missing"]

    context.log.info(f"Manifest: total={total_songs} kept={kept_dirs} dropped_missing={dropped_missing}")

    # Store valid song data as a manifest parquet to s3
    bucket, manifest_key = _store_manifest_parquet_to_s3(s3, rows, context.run_id)

    context.log.info(f"Manifest stored to bucket={bucket} with key={manifest_key}")

    return dg.MaterializeResult(
        metadata={
            "manifest_bucket": dg.MetadataValue.text(bucket),
            "manifest_key": dg.MetadataValue.text(manifest_key),
            "dirs_total": dg.MetadataValue.int(total_songs),
            "dirs_kept": dg.MetadataValue.int(kept_dirs),
            "dirs_dropped_missing_required": dg.MetadataValue.int(dropped_missing)
        }
    )


##############################
#   Data Processing Assets   #
##############################


#### Helpers ####

@dataclass
class MergeResult:
    dir_id: str
    dirname: str
    song_wav_bucket: Optional[str]
    song_wav_prefix: Optional[str]
    song_wav_key: Optional[str]
    status: str
    error: Optional[str]


def _get_parent_asset_metadata(context: dg.AssetExecutionContext, asset_key: List[str]):
    instance = context.instance
    materialization_event = instance.get_latest_materialization_event(
        dg.AssetKey(asset_key)
    )

    if materialization_event and materialization_event.asset_materialization:
        metadata = materialization_event.asset_materialization.metadata
        return metadata
    else:
        raise ValueError(f"No materialization found for asset with key: {asset_key}")
    

def _merge_one_dir(
    s3: S3Resource,
    dir_id: str,
    dirname: str,
    opus_paths_json: str,
    out_bucket: str,
    out_prefix: str,
    target_sample_rate: int = 22050
) -> MergeResult:
    """
    Merges multiple .opus files for a single song directory into a single .wav file, and stores this result into s3.
    """
    try:
        opus_paths = json.loads(opus_paths_json)
        if not opus_paths:
            return MergeResult(dir_id, dirname, None, None, None, "skipped_no_opus", "opus_paths_json empty")
        
        # (1) download .opus files from s3 into memory
        opus_bytes_list: List[io.BytesIO] = []
        for opus_path in opus_paths:
            bucket, key = opus_path.split("/", 1)
            opus_bytes_io = s3.get_object(bucket_name=bucket, obj_key=key)
            opus_bytes_list.append(opus_bytes_io)
        
        # (2) convert and merge .opus files into a single .wav byte stream
        wav_bytes: List[io.BytesIO] = [opus_to_wav_bytes(opus_bytes, target_sample_rate) for opus_bytes in opus_bytes_list]
        merged_wav_bytes = merge_wav_bytes(wav_bytes)

        # (3) store merged .wav back to s3
        song_wav_key = f"{out_prefix}/{dirname}/song.wav"
        s3.put_bytes(bucket_name=out_bucket, obj_key=song_wav_key, data=merged_wav_bytes.getvalue(), content_type="audio/wav")

        return MergeResult(dir_id, dirname, out_bucket, out_prefix, song_wav_key, "success", None)

    except Exception as e:
        tbe = traceback.TracebackException.from_exception(e)
        tb_str = ''.join(tbe.format())
        err_str = '\n'.join([str(e), tb_str])
        return MergeResult(dir_id, dirname, None, None, None, "error", err_str)


#### Asset Definitions ####


@dg.asset(
    key=dg.AssetKey(["raw", "ini_metadata"]),
    deps=[["raw", "manifest_parquet"]],
    kinds={"python", "s3", "parquet"}
)
def raw_ini_metadata(
    context: dg.AssetExecutionContext,
    s3: S3Resource
) -> dg.MaterializeResult:
    """
    Extracts song.ini metadata from manifest_parquet, creating a record of essential Clone Hero song
    metadata, which will be useful for later exploratory data analysis.
    """
    return dg.MaterializeResult()


@dg.asset(
    key=dg.AssetKey(["raw", "chart_data"]),
    deps=[["raw", "manifest_parquet"]],
    kinds={"python", "s3", "parquet"}
)
def raw_chart_data(
    context: dg.AssetExecutionContext,
    s3: S3Resource
) -> dg.MaterializeResult:
    return dg.MaterializeResult()


@dg.asset(
    key=dg.AssetKey(["raw", "song_data"]),
    deps=[["raw", "manifest_parquet"]],
    kinds={"python", "s3", "parquet"}
)
def raw_song_data(
    context: dg.AssetExecutionContext,
    s3: S3Resource
) -> dg.MaterializeResult:
    """
    Merges multiple .opus audio files into a single `song.wav` file for a single song,
    and stores this result into s3 at: data-collection/transformed/audio/<song_name>/song.wav 
    """
    # (1) Read parquet to locate s3 files
    
    # retrieve previous asset instance
    parent_asset_metadata = _get_parent_asset_metadata(context, asset_key=["raw", "manifest_parquet"])

    manifest_bucket_entry = parent_asset_metadata.get('manifest_bucket')
    if manifest_bucket_entry:
        manifest_bucket = manifest_bucket_entry.value

    manifest_key_entry = parent_asset_metadata.get('manifest_key')
    if manifest_key_entry:
        manifest_key = manifest_key_entry.value

    context.log.info(f"located manifest: bucket={manifest_bucket}, key={manifest_key}")

    # define s3 storage bucket/key/obj name for merged song wav files
    out_bucket = manifest_bucket
    out_prefix = "transformed/audio"

    # parse parquet into memory (selecting only the relevant columns)
    manifest_bytes = s3.get_object(bucket_name=manifest_bucket, obj_key=manifest_key)
    manifest_df = pl.read_parquet(manifest_bytes).select(["dir_id", "dirname", "opus_paths"])

    total_rows = manifest_df.height
    context.log.info(f"Loaded manifest: {total_rows} directories")

    results: List[MergeResult] = []
    success = 0
    err = 0

    # define total number of concurrent workers based on the system
    max_workers = max(2, (os.cpu_count() or 8) // 2)
    # Submit tasks
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [
            ex.submit(_merge_one_dir, s3, row["dir_id"], row["dirname"], row["opus_paths"], out_bucket, out_prefix)
            for row in manifest_df.iter_rows(named=True)
        ]

        for i, fut in enumerate(as_completed(futures), start=1):
            r = fut.result()
            results.append(r)
            if r.status == "success":
                success += 1
            else:
                err += 1
            
            if i % 200 == 0:
                context.log.info(f"Progress: {i}/{total_rows} success={success} error={err}")
    end = time.perf_counter()
    
    context.log.info(f"Completed merging audio for {total_rows} directories with {success} successes and {err} errors in {end - start:.2f} seconds using {max_workers} workers")

    # write updated manifest (add song_opus path + status/error)
    out_manifest_bucket = manifest_bucket
    out_manifest_key = f"manifests/manifest_merged_audio_{context.run_id}.parquet"
    out_df = pl.DataFrame(
        {
            "dir_id": [r.dir_id for r in results],
            "song_wav_bucket": [r.song_wav_bucket for r in results],
            "song_wav_prefix": [r.song_wav_prefix for r in results],
            "song_wav_key": [r.song_wav_key for r in results],
            "merge_status": [r.status for r in results],
            "merge_error": [r.error for r in results]
        }
    )

    # Write parquet to bytes (S3 resource handles storage)
    buf = io.BytesIO()
    out_df.write_parquet(buf, compression="zstd")
    data = buf.getvalue()
    s3.put_bytes(bucket_name=out_manifest_bucket, obj_key=out_manifest_key, data=data, content_type="application/octet-stream")

    return dg.MaterializeResult(
        metadata={
            "input_manifest_bucket": dg.MetadataValue.text(manifest_bucket),
            "input_manifest_key": dg.MetadataValue.text(manifest_key),
            "output_manifest_bucket": dg.MetadataValue.text(out_manifest_bucket),
            "output_manifest_key": dg.MetadataValue.text(out_manifest_key),
            "songs_total": dg.MetadataValue.int(total_rows),
            "songs_success": dg.MetadataValue.int(success),
            "songs_error": dg.MetadataValue.int(err),
            "workers_used": dg.MetadataValue.int(max_workers)
        }
    )