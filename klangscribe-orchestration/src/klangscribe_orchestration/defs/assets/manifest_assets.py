import os
import io
import re
import json
import time
import traceback
from ...utils.audio_processing import opus_to_wav_bytes, merge_wav_bytes, merge_opus_bytes
from ...utils.raw_processing import (
    parse_ini_file, get_empty_df_for_ini_metadata, add_ini_metadata_to_df, parse_chart_file,
    convert_notes_to_seconds, calculate_note_density_summary, convert_notes_to_fixed_grid
)
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import dagster as dg
import numpy as np
import polars as pl
import pyarrow as pa
from psycopg2 import sql
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
    
    if os.getenv("PRODUCTION", "false").lower() == "true":
        context.log.info("Running in PRODUCTION mode: processing all songs in PostgreSQL")
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
        """
        # list of songs from data collection metadata table
        result_rows = pg.fetchall(query)
    else:
        context.log.info("Running in TEST mode: processing only a subset of songs from PostgreSQL (for faster iteration during development)")
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
                :limit
        """
        try:
            limit = int(os.getenv("DATA_LIMIT", "10"))
        except ValueError:
            context.log.warning(f"Invalid DATA_LIMIT value: {os.getenv('DATA_LIMIT')}, defaulting to 10")
            limit = 10
        params = {'limit': limit}
        # list of songs from data collection metadata table
        result_rows = pg.fetchall(query, params)

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
class AudioMergeResult:
    dir_id: str
    dirname: str
    song_wav_bucket: Optional[str]
    song_wav_prefix: Optional[str]
    song_wav_key: Optional[str]
    status: str
    error: Optional[str]


@dataclass
class ChartDataResult:
    dir_id: str
    dirname: str
    song_chart_bucket: Optional[str]
    song_chart_prefix: Optional[str]
    song_chart_key: Optional[str]
    status: str
    error: Optional[str]


@dataclass
class MetadataMergeResult:
    dir_id: str
    dirname: str
    data: Optional[Dict[str, str]]
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
    

def _extract_ini_metadata(
    s3: S3Resource,
    dir_id: str,
    dirname: str,
    ini_path: str,
) -> MetadataMergeResult:
    """
    Extracts metadata from the .ini file for a song directory, returning this data as a dictionary.
    """
    try:
        ini_bucket, ini_key = ini_path.split("/", 1)
        ini_bytes = s3.get_object(bucket_name=ini_bucket, obj_key=ini_key)
        metadata = parse_ini_file(ini_bytes)
        return MetadataMergeResult(dir_id, dirname, metadata, "success", None)
    except Exception as e:
        tbe = traceback.TracebackException.from_exception(e)
        tb_str = ''.join(tbe.format())
        err_str = '\n'.join([str(e), tb_str])
        return MetadataMergeResult(dir_id, dirname, None, "error", err_str)


def _extract_chart_data(
    s3: S3Resource,
    dir_id: str,
    dirname: str,
    chart_path: str,
    out_bucket: str,
    out_prefix: str
) -> ChartDataResult:
    """
    Extracts note sequence data from the .chart file for a song directory, vectorizing this data and storing it as a .npz file in s3.
    """
    try:
        if not chart_path:
            return ChartDataResult(dir_id, dirname, None, None, None, "skipped_no_chart", "No chart path provided")

        # (1) download .chart file from s3 into memory
        chart_bucket, chart_key = chart_path.split("/", 1)
        chart_bytes = s3.get_object(bucket_name=chart_bucket, obj_key=chart_key)

        # (2) parse .chart file and extract note sequence data into numpy arrays
        resolution, offset, tempo_changes, note_data = parse_chart_file(chart_bytes)

        # (3) store extracted data as .npz back to s3
        chart_data_key = f"{dirname}/chart_data.npz"
        chart_data_stream = io.BytesIO()
        np.savez(chart_data_stream, resolution=resolution, offset=offset, tempo_changes=tempo_changes, note_data=note_data)
        s3.put_bytes(bucket_name=out_bucket, obj_key=f"{out_prefix}/{chart_data_key}", data=chart_data_stream.getvalue(), content_type="application/octet-stream")

        return ChartDataResult(dir_id, dirname, out_bucket, out_prefix, chart_data_key, "success", None)
    except Exception as e:
        tbe = traceback.TracebackException.from_exception(e)
        tbe_str = ''.join(tbe.format())
        err_str = '\n'.join([str(e), tbe_str])
        return ChartDataResult(dir_id, dirname, None, None, None, "error", err_str)


def _merge_one_dir(
    s3: S3Resource,
    dir_id: str,
    dirname: str,
    opus_paths_json: str,
    out_bucket: str,
    out_prefix: str,
    target_sample_rate: int = 22050
) -> AudioMergeResult:
    """
    Merges multiple .opus files for a single song directory into a single .wav file, and stores this result into s3.
    """
    try:
        opus_paths = json.loads(opus_paths_json)
        if not opus_paths:
            return AudioMergeResult(dir_id, dirname, None, None, None, "skipped_no_opus", "opus_paths_json empty")
        
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
        song_wav_key = f"{dirname}/song.wav"
        s3.put_bytes(bucket_name=out_bucket, obj_key=f"{out_prefix}/{song_wav_key}", data=merged_wav_bytes.getvalue(), content_type="audio/wav")

        return AudioMergeResult(dir_id, dirname, out_bucket, out_prefix, song_wav_key, "success", None)

    except Exception as e:
        tbe = traceback.TracebackException.from_exception(e)
        tb_str = ''.join(tbe.format())
        err_str = '\n'.join([str(e), tb_str])
        return AudioMergeResult(dir_id, dirname, None, None, None, "error", err_str)


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
    Data is stored in s3 as a .parquet file stored at data-collection/transformed/song_metadata.parquet
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

    # parse parquet into memory (selecting only the relevant columns)
    manifest_bytes = s3.get_object(bucket_name=manifest_bucket, obj_key=manifest_key)
    manifest_df = pl.read_parquet(manifest_bytes).select(["dir_id", "dirname", "ini_path"])

    total_rows = manifest_df.height
    context.log.info(f"Loaded manifest: total={total_rows} directories")

    # (2) extract .ini metadata for each song and store results in a new parquet file in s3
    results: List[MetadataMergeResult] = []
    success = 0
    err = 0

    ini_metadata_dicts: List[Dict[str, str]] = []

    # Submit tasks
    start = time.perf_counter()
    with ThreadPoolExecutor() as ex:
        futures = [
            ex.submit(_extract_ini_metadata, s3, row["dir_id"], row["dirname"], row["ini_path"])
            for row in manifest_df.iter_rows(named=True)
        ]

        for i, fut in enumerate(as_completed(futures), start=1):
            r = fut.result()
            results.append(r)
            if r.status == "success":
                success += 1
                metadata_dict = {'dir_id': r.dir_id, **(r.data or {})}  # combine dir_id with extracted metadata for easier analysis later
                ini_metadata_dicts.append(metadata_dict)
            else:
                err += 1
            
            if i % 50 == 0:
                context.log.info(f"Progress: {i}/{total_rows} success={success} error={err}")
    end = time.perf_counter()

    context.log.info(f"Completed extracting .ini metadata for {total_rows} directories with {success} successes and {err} errors in {end - start:.2f} seconds")

    # (4) Write .ini metadata parquet to s3 (for later analysis, separate from the manifest parquet which is more focused on file paths and processing status)
    out_metadata_bucket = manifest_bucket
    out_metadata_key = f"transformed/ini_metadata_{context.run_id}.parquet"
    buf = io.BytesIO()
    ini_metadata_df = pl.from_dicts(ini_metadata_dicts)
    ini_metadata_df.write_parquet(buf, compression="zstd")
    s3.put_bytes(bucket_name=out_metadata_bucket, obj_key=out_metadata_key, data=buf.getvalue(), content_type="application/octet-stream")

    # (4) Write transformation manifest to s3:

    # write updated manifest (record status/error of ini metadata extraction)
    out_manifest_bucket = manifest_bucket
    out_manifest_key = f"manifests/manifest_ini_metadata_{context.run_id}.parquet"
    out_df = pl.DataFrame(
        {
            "dir_id": [r.dir_id for r in results],
            "dirname": [r.dirname for r in results],
            "ini_metadata": [json.dumps(r.data) if r.data else None for r in results],
            "metadata_extraction_status": [r.status for r in results],
            "metadata_extraction_error": [r.error for r in results] 
        }
    )

    # write parquet to bytes (S3 resource handles storage)
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
            "metadata_bucket": dg.MetadataValue.text(out_metadata_bucket),
            "metadata_key": dg.MetadataValue.text(out_metadata_key),
            "metadata_extraction_success": dg.MetadataValue.int(success),
            "metadata_extraction_error": dg.MetadataValue.int(err)
        }
    )







@dg.asset(
    key=dg.AssetKey(["raw", "chart_data"]),
    deps=[["raw", "manifest_parquet"]],
    kinds={"python", "s3", "parquet"}
)
def raw_chart_data(
    context: dg.AssetExecutionContext,
    s3: S3Resource
) -> dg.MaterializeResult:
    """
    Converts .chart files for each song into a vectorized format representing the note sequences, and stores this result in s3.
    Each song is represented as an .npz file containing numpy arrays for:
    - sync track (BPM changes)
    - note tracks (tick-based note sequences)
    - metadata (song resolution, offset)
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

    # define s3 storage bucket/key/obj name for vectorized chart data
    out_bucket = manifest_bucket
    out_prefix = "transformed/chart"

    # parse parquet into memory (selecting only the relevant columns)
    manifest_bytes = s3.get_object(bucket_name=manifest_bucket, obj_key=manifest_key)
    manifest_df = pl.read_parquet(manifest_bytes).select(["dir_id", "dirname", "chart_path"])

    total_rows = manifest_df.height
    context.log.info(f"Loaded manifest: total={total_rows} directories")

    results: List[dict] = []
    success = 0
    err = 0

    # Submit tasks
    start = time.perf_counter()

    with ThreadPoolExecutor() as ex:
        futures = [
            ex.submit(_extract_chart_data, s3, row["dir_id"], row["dirname"], row["chart_path"], out_bucket, out_prefix)
            for row in manifest_df.iter_rows(named=True)
        ]

        for i, fut in enumerate(as_completed(futures), start=1):
            r = fut.result()
            results.append(r)
            if r.status == "success":
                success += 1
            else:
                err += 1
            
            if i % 50 == 0:
                context.log.info(f"Progress: {i}/{total_rows} success={success} error={err}")

    end = time.perf_counter()
    context.log.info(f"Completed processing {total_rows} directories with {success} successes and {err} errors in {end - start:.2f} seconds")

    # write updated manifest (record status/error of chart data extraction)
    out_manifest_bucket = manifest_bucket
    out_manifest_key = f"manifests/manifest_chart_data_{context.run_id}.parquet"
    out_df = pl.DataFrame(
        {
            "dir_id": [r.dir_id for r in results],
            "dirname": [r.dirname for r in results],
            "song_chart_bucket": [r.song_chart_bucket for r in results],
            "song_chart_prefix": [r.song_chart_prefix for r in results],
            "song_chart_key": [r.song_chart_key for r in results],
            "chart_data_extraction_status": [r.status for r in results],
            "chart_data_extraction_error": [r.error for r in results] 
        }
    )

    # write parquet to bytes (S3 resource handles storage)
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
            "songs_chart_extraction_success": dg.MetadataValue.int(success),
            "songs_chart_extraction_error": dg.MetadataValue.int(err)
        }
    )






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

    results: List[dict] = []
    success = 0
    err = 0

    # Submit tasks
    start = time.perf_counter()
    with ThreadPoolExecutor() as ex:
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
            
            if i % 50 == 0:
                context.log.info(f"Progress: {i}/{total_rows} success={success} error={err}")
    end = time.perf_counter()
    
    context.log.info(f"Completed merging audio for {total_rows} directories with {success} successes and {err} errors in {end - start:.2f} seconds")

    # write updated manifest (add song_opus path + status/error)
    out_manifest_bucket = manifest_bucket
    out_manifest_key = f"manifests/manifest_merged_audio_{context.run_id}.parquet"
    out_df = pl.DataFrame(
        {
            "dir_id": [r.dir_id for r in results],
            "dirname": [r.dirname for r in results],
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
            "songs_error": dg.MetadataValue.int(err)
        }
    )


# ----------------------------------------------------- #
#   Merge Opus files into a single opus file per song   #
# ----------------------------------------------------- #

# HELPERS

def _merge_one_song(
    s3: S3Resource,
    dir_id: str,
    dirname: str,
    opus_paths_json: str,
    out_bucket: str,
    out_prefix: str
) -> AudioMergeResult:
    """
    Merges multiple .opus files for a single song directory into a single .opus file, and stores this result into s3.
    """
    try:
        opus_paths = json.loads(opus_paths_json)
        if not opus_paths:
            return AudioMergeResult(dir_id, dirname, None, None, None, "skipped_no_opus", "opus_paths_json empty")
        
        # (1) download .opus files from s3 into memory
        opus_bytes_list: List[io.BytesIO] = []
        for opus_path in opus_paths:
            bucket, key = opus_path.split("/", 1)
            opus_bytes_io = s3.get_object(bucket_name=bucket, obj_key=key)
            opus_bytes_list.append(opus_bytes_io)
        
        # (2) merge .opus files into a single .opus byte stream
        merged_opus_bytes = merge_opus_bytes(opus_bytes_list)

        # (3) store merged .opus back to s3
        song_opus_key = f"{dirname}/song.opus"
        s3.put_bytes(bucket_name=out_bucket, obj_key=f"{out_prefix}/{song_opus_key}", data=merged_opus_bytes.getvalue(), content_type="audio/opus")

        return AudioMergeResult(dir_id, dirname, out_bucket, out_prefix, song_opus_key, "success", None)

    except Exception as e:
        tbe = traceback.TracebackException.from_exception(e)
        tb_str = ''.join(tbe.format())
        err_str = '\n'.join([str(e), tb_str])
        return AudioMergeResult(dir_id, dirname, None, None, None, "error", err_str)

# ASSET DEFINITION

@dg.asset(
    key=dg.AssetKey(["raw", "opus_data"]),
    deps=[["raw", "manifest_parquet"]],
    kinds={"python", "s3", "parquet"}
)
def raw_opus_data(
    context: dg.AssetExecutionContext,
    s3: S3Resource
):
    """
    Merges multiple .opus audio files into a single `song.opus` file for a single song,
    and stores this result into s3 at: data-collection/transformed/opus/<song_name>/song.opus.

    This asset is a more storage-efficient alternative to `raw_song_data` which merges .opus files into a single .wav file.
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

    # define s3 storage bucket/key/obj name for merged opus files
    out_bucket = manifest_bucket
    out_prefix = "transformed/opus"

    # parse parquet into memory (selecting only the relevant columns)
    manifest_bytes = s3.get_object(bucket_name=manifest_bucket, obj_key=manifest_key)
    manifest_df = pl.read_parquet(manifest_bytes).select(["dir_id", "dirname", "opus_paths"])

    total_rows = manifest_df.height
    context.log.info(f"Loaded manifest: total={total_rows} directories")

    results: List[dict] = []
    success = 0
    err = 0

    # Submit tasks
    start = time.perf_counter()
    with ThreadPoolExecutor() as ex:
        futures = [
            ex.submit(_merge_one_song, s3, row["dir_id"], row["dirname"], row["opus_paths"], out_bucket, out_prefix)
            for row in manifest_df.iter_rows(named=True)
        ]

        for i, fut in enumerate(as_completed(futures), start=1):
            r = fut.result()
            results.append(r)
            if r.status == "success":
                success += 1
            else:
                err += 1
            
            if i % 50 == 0:
                context.log.info(f"Progress: {i}/{total_rows} success={success} error={err}")
    end = time.perf_counter()

    context.log.info(f"Completed merging opus audio for {total_rows} directories with {success} successes and {err} errors in {end - start:.2f} seconds")

    # write updated manifest (add song_opus path + status/error)
    out_manifest_bucket = manifest_bucket
    out_manifest_key = f"manifests/manifest_merged_opus_{context.run_id}.parquet"
    out_df = pl.DataFrame(
        {
            "dir_id": [r.dir_id for r in results],
            "dirname": [r.dirname for r in results],
            "song_opus_bucket": [r.song_wav_bucket for r in results],
            "song_opus_prefix": [r.song_wav_prefix for r in results],
            "song_opus_key": [r.song_wav_key for r in results],
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
            "songs_error": dg.MetadataValue.int(err)
        }
    )


# ---------------------------------------------- #
#   Log Mel Spectrogram Audio Extraction Asset   #
# ---------------------------------------------- #

# HELPERS


# ASSET DEFINITION

# @dg.asset(
#     key=dg.AssetKey(["raw", "log_mel_spectrogram"]),
#     deps=[["raw", "opus_data"]],
#     kinds={"python", "s3", "parquet"}
# )
# def raw_mel_spectrogram(
#     context: dg.AssetExecutionContext,
#     s3: S3Resource
# ) -> dg.MaterializeResult:
#     """
#     Converts """
#     pass


# --------------------------------------------- #
#   Absolute-Time Chart File Conversion Asset   #
# --------------------------------------------- #

# HELPERS

@dataclass
class ChartDataAbsTimeStats:
    min_note_separation_sec: Optional[float]
    avg_note_separation_sec: Optional[float]
    max_note_separation_sec: Optional[float]
    stddev_note_separation_sec: Optional[float]
    median_note_separation_sec: Optional[float]

@dataclass
class ChartDataAbsTimeResult:
    dir_id: str
    dirname: str
    song_chart_bucket: Optional[str]
    song_chart_prefix: Optional[str]
    song_chart_key: Optional[str]
    data: Optional[ChartDataAbsTimeStats]
    status: str
    error: Optional[str]

def _extract_chart_data_absolute_time(
    s3: S3Resource,
    dir_id: str,
    dirname: str,
    chart_bucket: str,
    chart_prefix: str,
    chart_key: str,
    out_bucket: str,
    out_prefix: str
) -> ChartDataAbsTimeResult:
    """
    Converts .chart files for each song into a vectorized format representing the note sequences in absolute time (seconds), and stores this result in s3. 
    This is derived from raw_chart_data but represents note sequences in absolute time rather than tick-based format.
    This will be used by raw_chart_data_fixed_grid asset which will convert this absolute-time data into a fixed time grid representation for easier use in machine learning models.
    """
    try:
        if not all([chart_bucket, chart_prefix, chart_key]):
            return ChartDataAbsTimeResult(dir_id, dirname, None, None, None, None, "skipped_no_chart", "No chart path provided")
        
        # (1) download raw chart data .npz file from s3 into memory
        chart_npz_bytes = s3.get_object(bucket_name=chart_bucket, obj_key=f"{chart_prefix}/{chart_key}")

        # (2) load .npz file
        chart_data = np.load(chart_npz_bytes)
        resolution = chart_data["resolution"].item()
        offset = chart_data["offset"].item()
        tempo_changes = chart_data["tempo_changes"]  # shape (num_tempo_changes, 2) with columns [tick, bpm]
        note_data = chart_data["note_data"]  # shape (num_notes, 3) with columns [tick, lane, duration]

        # (3) convert tick-based note data into absolute time (seconds)
        note_data_abs_time = convert_notes_to_seconds(note_data, tempo_changes, resolution, offset)
        # 5-number summary stats on note density (note separation in seconds)
        avg_delta, med_delta, min_delta, max_delta, std_delta = calculate_note_density_summary(note_data_abs_time)

        stats = ChartDataAbsTimeStats(
            min_note_separation_sec=min_delta,
            avg_note_separation_sec=avg_delta,
            max_note_separation_sec=max_delta,
            stddev_note_separation_sec=std_delta,
            median_note_separation_sec=med_delta
        )

        # (4) store absolute-time note data as .npy file back to s3
        abs_time_chart_data_key = f"{dirname}/chart_data_abs_time.npy"
        abs_time_chart_data_stream = io.BytesIO()
        np.save(abs_time_chart_data_stream, note_data_abs_time)
        s3.put_bytes(bucket_name=out_bucket, obj_key=f"{out_prefix}/{abs_time_chart_data_key}", data=abs_time_chart_data_stream.getvalue(), content_type="application/octet-stream")

        return ChartDataAbsTimeResult(dir_id, dirname, out_bucket, out_prefix, abs_time_chart_data_key, stats, "success", None)

    except Exception as e:
        tbe = traceback.TracebackException.from_exception(e)
        tb_str = ''.join(tbe.format())
        err_str = '\n'.join([str(e), tb_str])
        return ChartDataAbsTimeResult(dir_id, dirname, None, None, None, None, "error", err_str)

# ASSET DEFINITION

@dg.asset(
    key=dg.AssetKey(["raw", "chart_data_absolute_time"]),
    deps=[["raw", "chart_data"]],
    kinds={"python", "s3", "parquet"}
)
def raw_chart_data_absolute_time(
    context: dg.AssetExecutionContext,
    s3: S3Resource
) -> dg.MaterializeResult:
    """
    Converts .chart files for each song into a vectorized format representing the note sequences in absolute time (seconds),
    and stores this result in s3. This is an alternative to `raw_chart_data` which represents note sequences in tick-based format.
    """
    # retrieve previous asset instance
    parent_asset_metadata = _get_parent_asset_metadata(context, asset_key=["raw", "chart_data"])

    manifest_bucket_entry = parent_asset_metadata.get('output_manifest_bucket')
    if manifest_bucket_entry:
        manifest_bucket = manifest_bucket_entry.value
    
    manifest_key_entry = parent_asset_metadata.get('output_manifest_key')
    if manifest_key_entry:
        manifest_key = manifest_key_entry.value
    
    context.log.info(f"located parent asset manifest: bucket={manifest_bucket}, key={manifest_key}")
    
    # define s3 storage bucket/key/obj name for absolute-time vectorized chart data
    out_bucket = manifest_bucket
    out_prefix = "transformed/chart_absolute_time"
    # parse parquet into memory (selecting only the relevant columns)
    manifest_bytes = s3.get_object(bucket_name=manifest_bucket, obj_key=manifest_key)
    manifest_df = pl.read_parquet(manifest_bytes).filter(pl.col("chart_data_extraction_status") == "success")
    manifest_df = manifest_df.select(["dir_id", "dirname", "song_chart_bucket", "song_chart_prefix", "song_chart_key"])
    total_rows = manifest_df.height
    context.log.info(f"Loaded parent asset manifest: total={total_rows} chart directories")

    results: List[dict] = []
    success = 0
    err = 0

    # Submit tasks
    start = time.perf_counter()
    with ThreadPoolExecutor() as ex:
        futures = [
            ex.submit(
                _extract_chart_data_absolute_time,
                s3,
                row["dir_id"],
                row["dirname"],
                row["song_chart_bucket"],
                row["song_chart_prefix"],
                row["song_chart_key"],
                out_bucket,
                out_prefix
            )
            for row in manifest_df.iter_rows(named=True)
        ]

        for i, fut in enumerate(as_completed(futures), start=1):
            r = fut.result()
            results.append(r)
            if r.status == "success":
                success += 1
            else:
                err += 1
            
            if i % 50 == 0:
                context.log.info(f"Progress: {i}/{total_rows} success={success} error={err}")

    end = time.perf_counter()
    context.log.info(f"Completed chart data extraction in {end - start:.2f} seconds")

    # (1) write updated manifest (record status/error of chart data extraction)
    out_manifest_bucket = manifest_bucket
    out_manifest_key = f"manifests/manifest_chart_data_absolute_time_{context.run_id}.parquet"
    out_df = pl.DataFrame(
        {
            "dir_id": [r.dir_id for r in results],
            "dirname": [r.dirname for r in results],
            "song_chart_bucket": [r.song_chart_bucket for r in results],
            "song_chart_prefix": [r.song_chart_prefix for r in results],
            "song_chart_key": [r.song_chart_key for r in results],
            "chart_data_extraction_status": [r.status for r in results],
            "chart_data_extraction_error": [r.error for r in results] 
        }
    )
    
    # write parquet to bytes (S3 resource handles storage)
    buf = io.BytesIO()
    out_df.write_parquet(buf, compression="zstd")
    data = buf.getvalue()
    s3.put_bytes(bucket_name=out_manifest_bucket, obj_key=out_manifest_key, data=data, content_type="application/octet-stream")

    # (2) write summary stats of note density (in seconds) to parquet for later analysis
    stats_data_bucket = manifest_bucket
    stats_data_key = f"transformed/chart_absolute_time_stats_{context.run_id}.parquet"
    successful_results = [r for r in results if r.status == "success"]
    stats_df = pl.DataFrame(
        {
            "dir_id": [r.dir_id for r in successful_results],
            "dirname": [r.dirname for r in successful_results],
            "min_note_separation_sec": [r.data.min_note_separation_sec if r.data else None for r in successful_results],
            "avg_note_separation_sec": [r.data.avg_note_separation_sec if r.data else None for r in successful_results],
            "max_note_separation_sec": [r.data.max_note_separation_sec if r.data else None for r in successful_results],
            "stddev_note_separation_sec": [r.data.stddev_note_separation_sec if r.data else None for r in successful_results],
            "median_note_separation_sec": [r.data.median_note_separation_sec if r.data else None for r in successful_results]
        }
    )
    buf = io.BytesIO()
    stats_df.write_parquet(buf, compression="zstd")
    s3.put_bytes(bucket_name=stats_data_bucket, obj_key=stats_data_key, data=buf.getvalue(), content_type="application/octet-stream")

    # (3) return materialization result with metadata about the operation
    return dg.MaterializeResult(
        metadata={
            "input_manifest_bucket": dg.MetadataValue.text(manifest_bucket),
            "input_manifest_key": dg.MetadataValue.text(manifest_key),
            "output_manifest_bucket": dg.MetadataValue.text(out_manifest_bucket),
            "output_manifest_key": dg.MetadataValue.text(out_manifest_key),
            "stats_data_bucket": dg.MetadataValue.text(stats_data_bucket),
            "stats_data_key": dg.MetadataValue.text(stats_data_key),
            "songs_total": dg.MetadataValue.int(total_rows),
            "chart_data_extraction_success": dg.MetadataValue.int(success),
            "chart_data_extraction_error": dg.MetadataValue.int(err)
        }
    )


# ------------------------------------------ #
#   Fixed-Grid Chart Data Conversion Asset   #
# ------------------------------------------ #

# HELPERS

@dataclass
class ChartDataFixedGridResult:
    dir_id: str
    dirname: str
    song_chart_bucket: Optional[str]
    song_chart_prefix: Optional[str]
    song_chart_key: Optional[str]
    status: str
    error: Optional[str]


def _convert_chart_data_fixed_grid(
    s3: S3Resource,
    dir_id: str,
    dirname: str,
    chart_bucket: str,
    chart_prefix: str,
    chart_key: str,
    out_bucket: str,
    out_prefix: str,
    time_grid_interval_sec: float = 0.02,
    resolution: int = 480
) -> ChartDataFixedGridResult:
    """
    Converts absolute-time vectorized chart data into a fixed time grid representation, where each time step corresponds to a fixed interval (e.g. 20ms), 
    and note events are represented in binary format indicating whether a note is active at each time step.
    This asset also filters out any songs with a minimum note separation below 20ms to avoid issues with quantization in the fixed time grid representation.
    """
    try:
        if not all([chart_bucket, chart_prefix, chart_key]):
            return ChartDataFixedGridResult(dir_id, dirname, None, None, None, "skipped_no_chart", "No chart path provided")
        
        # (1) download absolute-time vectorized chart data .npy file from s3 into memory
        abs_time_chart_data_bytes = s3.get_object(bucket_name=chart_bucket, obj_key=f"{chart_prefix}/{chart_key}")

        # (2) load .npy file
        note_data_abs_time = np.load(abs_time_chart_data_bytes)

        # (3) convert absolute-time note data into fixed-grid representation
        # using 20ms grid interval and 480 resolution (ticks per quarter note) (reasons described in project paper)
        note_data_fixed_grid = convert_notes_to_fixed_grid(note_data_abs_time, grid_interval_sec=time_grid_interval_sec, resolution=resolution)

        # (4) store fixed-grid note data as .npy file back to s3
        fixed_grid_chart_data_key = f"{dirname}/chart_data_fixed_grid.npy"
        fixed_grid_chart_data_stream = io.BytesIO()
        np.save(fixed_grid_chart_data_stream, note_data_fixed_grid)
        s3.put_bytes(bucket_name=out_bucket, obj_key=f"{out_prefix}/{fixed_grid_chart_data_key}", data=fixed_grid_chart_data_stream.getvalue(), content_type="application/octet-stream")

        return ChartDataFixedGridResult(dir_id, dirname, out_bucket, out_prefix, fixed_grid_chart_data_key, "success", None)

    except Exception as e:
        tbe = traceback.TracebackException.from_exception(e)
        tb_str = ''.join(tbe.format())
        err_str = '\n'.join([str(e), tb_str])
        return ChartDataFixedGridResult(dir_id, dirname, None, None, None, "error", err_str)

# ASSET DEFINITION

@dg.asset(
    key=dg.AssetKey(["raw", "chart_data_fixed_grid"]),
    deps=[["raw", "chart_data_absolute_time"]],
    kinds={"python", "s3", "parquet"}
)
def raw_chart_data_fixed_grid(
    context: dg.AssetExecutionContext,
    s3: S3Resource
) -> dg.MaterializeResult:
    """
    Converts absolute-time vectorized chart data into a fixed time grid representation, where each time step corresponds to a fixed interval (e.g. 20ms), 
    and note events are represented in binary format indicating whether a note is active at each time step.
    This asset also filters out any songs with a minimum note separation below 20ms to avoid issues with quantization in the fixed time grid representation.
    """
    # (1) retrieve previous asset instance
    parent_asset_metadata = _get_parent_asset_metadata(context, asset_key=["raw", "chart_data_absolute_time"])

    # get the manifest bucket/key from the parent asset metadata to read the absolute-time chart data and to determine where to store the fixed-grid chart data
    manifest_bucket_entry = parent_asset_metadata.get('output_manifest_bucket')
    if manifest_bucket_entry:
        manifest_bucket = manifest_bucket_entry.value
    manifest_key_entry = parent_asset_metadata.get('output_manifest_key')
    if manifest_key_entry:
        manifest_key = manifest_key_entry.value

    context.log.info(f"located parent asset manifest: bucket={manifest_bucket}, key={manifest_key}")

    # parse manifest parquet into memory (selecting only the relevant columns)
    manifest_bytes = s3.get_object(bucket_name=manifest_bucket, obj_key=manifest_key)
    manifest_df = pl.read_parquet(manifest_bytes).filter(pl.col("chart_data_extraction_status") == "success")
    manifest_df = manifest_df.select(["dir_id", "dirname", "song_chart_bucket", "song_chart_prefix", "song_chart_key"])
    total_rows = manifest_df.height
    context.log.info(f"Loaded parent asset manifest: total={total_rows} chart directories")

    # get the stats data bucket/key from the parent asset metadata to read the note separation stats for filtering songs with very low note separation
    stats_data_bucket_entry = parent_asset_metadata.get('stats_data_bucket')
    if stats_data_bucket_entry:
        stats_data_bucket = stats_data_bucket_entry.value
    stats_data_key_entry = parent_asset_metadata.get('stats_data_key')
    if stats_data_key_entry:
        stats_data_key = stats_data_key_entry.value
    
    context.log.info(f"located parent asset stats data: bucket={stats_data_bucket}, key={stats_data_key}")

    # parse stats data parquet into memory to get note separation stats for filtering
    stats_data_bytes = s3.get_object(bucket_name=stats_data_bucket, obj_key=stats_data_key)
    stats_df = pl.read_parquet(stats_data_bytes).select(["dir_id", "min_note_separation_sec"])
    context.log.info(f"Loaded stats data: total={stats_df.height} rows")

    # join manifest_df with stats_df on dir_id to get the min_note_separation_sec for each song directory
    manifest_stats_df = manifest_df.join(stats_df, on="dir_id", how="left")

    # separate into kept vs filtered songs based on min_note_separation_sec
    # filter out songs with min_note_separation_sec below 0.02 seconds (20ms) to avoid issues with quantization in the fixed time grid representation
    # Note: this value is being hard-coded for now due to time constraints, but it could be made into a hyperparameter for tuning in the future
    separation_threshold_sec = 0.02
    filtered_df = manifest_stats_df.filter(pl.col("min_note_separation_sec") < separation_threshold_sec)    # songs which will be discarded from the dataset
    kept_df = manifest_stats_df.filter(pl.col("min_note_separation_sec") >= separation_threshold_sec)       # songs which will be kept and processed into the fixed-grid representation

    # store filtered songs manifest in s3 for later analysis (e.g. to analyze common characteristics of songs with very low note separation which were filtered out)
    filtered_manifest_bucket = manifest_bucket
    filtered_manifest_key = f"manifests/manifest_chart_data_fixed_grid_filtered_{context.run_id}.parquet"
    buf = io.BytesIO()
    filtered_df.write_parquet(buf, compression="zstd")
    s3.put_bytes(bucket_name=filtered_manifest_bucket, obj_key=filtered_manifest_key, data=buf.getvalue(), content_type="application/octet-stream")

    # define s3 storage bucket/key/obj name for fixed-grid vectorized chart data
    out_bucket = manifest_bucket
    out_prefix = "transformed/chart_fixed_grid"

    # (2) convert absolute-time chart data into fixed-grid representation for kept songs, and store results in s3
    results: List[dict] = []
    success = 0
    err = 0

    # Submit tasks
    start = time.perf_counter()
    with ThreadPoolExecutor() as ex:
        futures = [
            ex.submit(
                _convert_chart_data_fixed_grid,
                s3,
                row["dir_id"],
                row["dirname"],
                row["song_chart_bucket"],
                row["song_chart_prefix"],
                row["song_chart_key"],
                out_bucket,
                out_prefix,
                time_grid_interval_sec=separation_threshold_sec,   # use the same value as the filtering threshold for the grid interval to avoid quantization issues
                resolution=480
            )
            for row in kept_df.iter_rows(named=True)
        ]

        for i, fut in enumerate(as_completed(futures), start=1):
            r = fut.result()
            results.append(r)
            if r.status == "success":
                success += 1
            else:
                err += 1
            
            if i % 50 == 0:
                context.log.info(f"Progress: {i}/{kept_df.height} success={success} error={err}")
    end = time.perf_counter()
    context.log.info(f"Completed fixed-grid chart data conversion for {kept_df.height} directories with {success} successes and {err} errors in {end - start:.2f} seconds")

    # (3) write updated manifest (record status/error of fixed-grid chart data conversion)
    out_manifest_bucket = manifest_bucket
    out_manifest_key = f"manifests/manifest_chart_data_fixed_grid_{context.run_id}.parquet"
    out_df = pl.DataFrame(
        {
            "dir_id": [r.dir_id for r in results],
            "dirname": [r.dirname for r in results],
            "song_chart_bucket": [r.song_chart_bucket for r in results],
            "song_chart_prefix": [r.song_chart_prefix for r in results],
            "song_chart_key": [r.song_chart_key for r in results],
            "chart_data_conversion_status": [r.status for r in results],
            "chart_data_conversion_error": [r.error for r in results]
        }
    )

    # write parquet to bytes (S3 resource handles storage)
    buf = io.BytesIO()
    out_df.write_parquet(buf, compression="zstd")
    data = buf.getvalue()
    s3.put_bytes(bucket_name=out_manifest_bucket, obj_key=out_manifest_key, data=data, content_type="application/octet-stream")

    # (4) write materialization result with metadata about the operation, including the number of songs filtered out due to low note separation

    return dg.MaterializeResult(
        metadata={
            "input_manifest_bucket": dg.MetadataValue.text(manifest_bucket),
            "input_manifest_key": dg.MetadataValue.text(manifest_key),
            "input_stats_data_bucket": dg.MetadataValue.text(stats_data_bucket),
            "input_stats_data_key": dg.MetadataValue.text(stats_data_key),
            "output_manifest_bucket": dg.MetadataValue.text(out_manifest_bucket),
            "output_manifest_key": dg.MetadataValue.text(out_manifest_key),
            "filtered_manifest_bucket": dg.MetadataValue.text(filtered_manifest_bucket),
            "filtered_manifest_key": dg.MetadataValue.text(filtered_manifest_key),
            "songs_total": dg.MetadataValue.int(total_rows),
            "song_success": dg.MetadataValue.int(success),
            "song_error": dg.MetadataValue.int(err),
            "songs_filtered": dg.MetadataValue.int(filtered_df.height),
        }
    )


###################################
#   Dataset Construction Assets   #
###################################

# Contains assets for producing a canonical dataset to upload to Kaggle for future use
# Note: The dataset must be made private with request-access enabled in Kaggle

########################################
#   Canonical Dataset Manifest Asset   #
########################################

@dg.asset(
    key=dg.AssetKey(["dataset", "canonical_manifest"]),
    deps=[["raw", "opus_data"], ["raw", "chart_data"], ["raw", "ini_metadata"]],
    kinds={"python", "s3", "parquet"}
)
def dataset_canonical_manifest(
    context: dg.AssetExecutionContext,
    s3: S3Resource
) -> dg.MaterializeResult:
    """
    Creates a canonical manifest parquet file which combines all the relevant metadata and file paths from the raw assets, and stores this result in s3. 
    This manifest will be used for constructing the final dataset for upload to Kaggle, and it will also serve as a reference for all the data files in the dataset.
    """

    # retrieve previous asset instances for raw_chart_data, raw_ini_metadata, and raw_opus_data to get the manifest bucket/key and locate the relevant s3 files
    ini_metadata_metadata = _get_parent_asset_metadata(context, asset_key=["raw", "ini_metadata"])
    chart_data_metadata = _get_parent_asset_metadata(context, asset_key=["raw", "chart_data"])
    opus_data_metadata = _get_parent_asset_metadata(context, asset_key=["raw", "opus_data"])

    # (1) get ini_metadata manifest and ini_metadata data parquet

    # ini_metadata manifest
    ini_manifest_bucket_entry = ini_metadata_metadata.get('output_manifest_bucket')
    if ini_manifest_bucket_entry:
        ini_manifest_bucket = ini_manifest_bucket_entry.value
    ini_manifest_key_entry = ini_metadata_metadata.get('output_manifest_key')
    if ini_manifest_key_entry:
        ini_manifest_key = ini_manifest_key_entry.value
    
    # ini_metadata data
    ini_metadata_bucket_entry = ini_metadata_metadata.get('metadata_bucket')
    if ini_metadata_bucket_entry:
        ini_metadata_bucket = ini_metadata_bucket_entry.value
    ini_metadata_key_entry = ini_metadata_metadata.get('metadata_key')
    if ini_metadata_key_entry:
        ini_metadata_key = ini_metadata_key_entry.value

    # read ini_metadata manifest parquet into memory, and filter for successful rows, selecting only the dir_id column
    ini_manifest_bytes = s3.get_object(bucket_name=ini_manifest_bucket, obj_key=ini_manifest_key)
    ini_manifest_df = pl.read_parquet(ini_manifest_bytes).filter(pl.col("metadata_extraction_status") == "success")
    ini_manifest_df = ini_manifest_df.select(["dir_id"])

    # read ini_metadata data parquet into memory
    ini_metadata_bytes = s3.get_object(bucket_name=ini_metadata_bucket, obj_key=ini_metadata_key)
    ini_metadata_df = pl.read_parquet(ini_metadata_bytes)


    # (2) get chart_data manifest
    chart_manifest_bucket_entry = chart_data_metadata.get('output_manifest_bucket')
    if chart_manifest_bucket_entry:
        chart_manifest_bucket = chart_manifest_bucket_entry.value
    chart_manifest_key_entry = chart_data_metadata.get('output_manifest_key')
    if chart_manifest_key_entry:    
        chart_manifest_key = chart_manifest_key_entry.value
    
    # read chart_data manifest parquet into memory, and filter for successful rows
    chart_manifest_bytes = s3.get_object(bucket_name=chart_manifest_bucket, obj_key=chart_manifest_key)
    chart_manifest_df = pl.read_parquet(chart_manifest_bytes).filter(pl.col("chart_data_extraction_status") == "success")
    chart_manifest_df = chart_manifest_df.select(["dir_id", "song_chart_bucket", "song_chart_prefix", "song_chart_key"])

    # (3) get opus_data manifest
    opus_manifest_bucket_entry = opus_data_metadata.get('output_manifest_bucket')
    if opus_manifest_bucket_entry:
        opus_manifest_bucket = opus_manifest_bucket_entry.value
    opus_manifest_key_entry = opus_data_metadata.get('output_manifest_key')
    if opus_manifest_key_entry:
        opus_manifest_key = opus_manifest_key_entry.value
    
    # read opus_data manifest parquet into memory, and filter for successful rows
    opus_manifest_bytes = s3.get_object(bucket_name=opus_manifest_bucket, obj_key=opus_manifest_key)
    opus_manifest_df = pl.read_parquet(opus_manifest_bytes).filter(pl.col("merge_status") == "success")
    opus_manifest_df = opus_manifest_df.select(["dir_id", "song_opus_bucket", "song_opus_prefix", "song_opus_key"])

    # (4) join ini_metadata, chart_data, and opus_data dataframes on dir_id to create the canonical manifest dataframe
    manifest_df = ini_manifest_df.join(chart_manifest_df, on="dir_id", how="inner").join(opus_manifest_df, on="dir_id", how="inner")

    # store canonical manifest in s3
    manifest_bucket = ini_manifest_bucket
    manifest_key = f"manifests/manifest_canonical_{context.run_id}.parquet"
    buf = io.BytesIO()
    manifest_df.write_parquet(buf, compression="zstd")
    s3.put_bytes(bucket_name=manifest_bucket, obj_key=manifest_key, data=buf.getvalue(), content_type="application/octet-stream")

    # (5) filter out any rows from the ini_metadata data parquet which are missing from the manifest_df after the join
    ini_metadata_df = ini_metadata_df.join(manifest_df.select("dir_id"), on="dir_id", how="inner")

    # store filtered ini_metadata data in s3
    filtered_ini_metadata_bucket = ini_metadata_bucket
    filtered_ini_metadata_key = f"canonical/ini_metadata_{context.run_id}.parquet"
    buf = io.BytesIO()
    ini_metadata_df.write_parquet(buf, compression="zstd")
    s3.put_bytes(bucket_name=filtered_ini_metadata_bucket, obj_key=filtered_ini_metadata_key, data=buf.getvalue(), content_type="application/octet-stream")

    return dg.MaterializeResult(
        metadata={
            # canonical manifest and metadata file paths
            "canonical_manifest_bucket": dg.MetadataValue.text(manifest_bucket),
            "canonical_manifest_key": dg.MetadataValue.text(manifest_key),
            "canonical_ini_metadata_bucket": dg.MetadataValue.text(filtered_ini_metadata_bucket),
            "canonical_ini_metadata_key": dg.MetadataValue.text(filtered_ini_metadata_key),
            # input manifests and metadata file paths
            "input_ini_metadata_manifest_bucket": dg.MetadataValue.text(ini_manifest_bucket),
            "input_ini_metadata_manifest_key": dg.MetadataValue.text(ini_manifest_key),
            "input_ini_metadata_bucket": dg.MetadataValue.text(ini_metadata_bucket),
            "input_ini_metadata_key": dg.MetadataValue.text(ini_metadata_key),
            "input_chart_data_manifest_bucket": dg.MetadataValue.text(chart_manifest_bucket),
            "input_chart_data_manifest_key": dg.MetadataValue.text(chart_manifest_key),
            "input_opus_data_manifest_bucket": dg.MetadataValue.text(opus_manifest_bucket),
            "input_opus_data_manifest_key": dg.MetadataValue.text(opus_manifest_key),
        }
    )