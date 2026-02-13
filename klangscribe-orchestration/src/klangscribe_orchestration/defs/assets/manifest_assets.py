import os
import re
import json
from typing import Any, Dict, List, Optional, Tuple

import dagster as dg
import pandas as pd
import psycopg2


# Acceptable extensions
AUDIO_EXTS = (".opus",)
CHART_EXTS = (".chart",)     # omitting .mid extension, due to chart conversion issues
INI_EXTS = (".ini",)

# Aggregate these extensions together
REQUIRED_EXTS = AUDIO_EXTS + CHART_EXTS + INI_EXTS

def pick_file(files: List[Dict[str, Any]], exts: Tuple[str, ...]) -> Optional[str]:
    # choose the best match deterministically
    pass


@dg.asset
def manifest_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...
