import os
import re
import json
from typing import Any, Dict, List, Optional, Tuple

import dagster as dg
import pandas as pd
from ..resources import PostgresResource


# Acceptable extensions
AUDIO_EXTS = (".opus",)
CHART_EXTS = (".chart",)     # omitting .mid extension, due to chart conversion issues
INI_EXTS = (".ini",)
# Aggregate these extensions together
REQUIRED_EXTS = AUDIO_EXTS + CHART_EXTS + INI_EXTS


@dg.asset(
    key=dg.AssetKey(["raw", "manifest_parquet"]),
    compute_kind="python"
)
def raw_manifest_parquet(
    context: dg.AssetExecutionContext,
    pg: PostgresResource
) -> dg.MaterializeResult:
    """
    Defines a manifest parquet asset cinluding all song directories with valid essential files.
    Included filetypes:
      - .opus files (list of CH audio files)
      - .chart (Clone Hero chart file)
      - .ini (Clone Hero song metadata file)
    """
    