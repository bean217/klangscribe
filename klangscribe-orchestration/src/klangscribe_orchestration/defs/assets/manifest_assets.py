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


###############
#   Helpers   #
###############


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


#########################
#   Asset Definitions   #
#########################


@dg.asset(
    key=dg.AssetKey(["raw", "manifest_parquet"]),
    kinds={"python", "parquet"}
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

    context.log.info(
        f"Manifest: total={total_songs} kept={kept_dirs} dropped_missing={dropped_missing}"
    )

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

    manifest_name = f"manifest_{context.run_id}.parquet"
    bucket = 'data-collection'
    manifest_key = f"manifests/{manifest_name}"
    s3.put_bytes(bucket_name=bucket, obj_key=manifest_key, data=data, content_type="application/octet-stream")

    return dg.MaterializeResult(
        metadata={
            "manifest_key": dg.MetadataValue.text(manifest_key),
            "dirs_total": dg.MetadataValue.int(total_songs),
            "dirs_kept": dg.MetadataValue.int(kept_dirs),
            "dirs_dropped_missing_required": dg.MetadataValue.int(dropped_missing)
        }
    )