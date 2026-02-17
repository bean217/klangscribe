#
# file: src/klangscribe-orchestration/utils/raw_processing.py
# desc: Utilities for processing raw Clone Hero song file data, such as .ini, .chart, and .opus files
# auth: Benjamin Piro (benpiro1118@gmail.com)
# date: 15 February, 2026
#

import io
import os
import polars as pl
from pathlib import Path


#######################
#   .ini Processing   #
#######################

# List of relevant metadata keys to extract from Clone Hero .ini files
# see https://wiki.clonehero.net/books/guides-and-tutorials/page/songini-guide

CLONE_HERO_METADATA_KEYS = (
    "name",                 # song name
    "artist",               # song artist
    "album",                # song album
    "genre",                # song genre
    "year",                 # song release year
    "album_track",          # track number on the album
    "playlist_track",       # track number in the playlist
    "charter",              # person who created the chart
    "frets",                # alias for `charter`
    "icon",                 # filename of song icon image
    "diff_guitar",          # difficulty rating for guitar part
    "diff_rhythm",          # difficulty rating for rhythm guitar part
    "diff_bass",            # difficulty rating for bass part
    "diff_guitar_coop",     # difficulty rating for guitar co-op part
    "diff_drums",           # difficulty rating for drums part
    "diff_drums_real",      # difficulty rating for the pro drums part 
    "diff_guitarghl",       # difficulty rating for the 6-fret guitar part in Guitar Hero Live mode
    "diff_bassghl",         # difficulty rating for the 6-fret bass part in Guitar Hero Live mode
    "diff_rhythm_ghl",      # difficulty rating for the 6-fret rhythm guitar part in Guitar Hero Live mode
    "diff_guitar_coop_ghl", # difficulty rating for the 6-fret guitar co-op part in Guitar Hero Live mode
    "diff_keys",            # difficulty rating for the keys part
    "song_length",          # length of the song in milliseconds
    "preview_start_time",   # start time of the song preview in milliseconds
    "video_start_time",     # start time of the song video in milliseconds
    "modchart",             # indicates if the chart is a modchart
    "loading_phrase",       # loading phrase for the song
    "delay"                 # delay for the song in milliseconds (deprecated for newer charts)
)


def parse_ini_file(ini_bytes: io.BytesIO) -> dict:
    """
    Parses a .ini file from bytes and returns a dictionary of Clone Hero song metadata.
    """
    ini_bytes.seek(0)
    content = ini_bytes.read().decode('utf-8', errors='ignore')
    metadata = {}
    CH_ini_keys = set(CLONE_HERO_METADATA_KEYS)
    for line in content.splitlines():
        if '=' in line:
            key, value = line.split('=', 1)
            key = key.strip().lower()
            if key in CH_ini_keys:
                metadata[key] = value.strip()
    return metadata


def get_empty_df_for_ini_metadata() -> pl.DataFrame:
    """
    Returns an empty Polars DataFrame with the appropriate schema for storing Clone Hero .ini metadata
    """
    return pl.DataFrame(
        schema={key: pl.Utf8 for key in CLONE_HERO_METADATA_KEYS}
    )


def add_ini_metadata_to_df(df: pl.DataFrame, metadata: dict) -> pl.DataFrame:
    """
    Adds a row of .ini metadata to the given DataFrame, ensuring all expected keys are present
    """
    row = {key: metadata.get(key, None) for key in CLONE_HERO_METADATA_KEYS}
    return df.with_row(pl.Series(row))

#########################
#   .chart Processing   #
#########################

