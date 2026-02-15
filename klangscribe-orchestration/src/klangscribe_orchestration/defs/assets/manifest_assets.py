import os
import io
import re
import json
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict

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


#### Asset Definitions ####


@dg.asset(
    key=dg.AssetKey(["raw", "ini_metadata"]),
    deps=[["raw", "manifest_parquet"]],
    kinds={"python", "s3", "parquet"}
)
def raw_ini_metadata() -> dg.MaterializeResult:
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
def raw_chart_data() -> dg.MaterializeResult:
    return dg.MaterializeResult()


@dg.asset(
    key=dg.AssetKey(["raw", "song_data"]),
    deps=[["raw", "manifest_parquet"]],
    kinds={"python", "s3", "parquet"}
)
def raw_song_data() -> dg.MaterializeResult:
    return dg.MaterializeResult()