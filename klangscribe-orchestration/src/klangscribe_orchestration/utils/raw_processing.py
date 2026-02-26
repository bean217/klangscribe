#
# file: src/klangscribe-orchestration/utils/raw_processing.py
# desc: Utilities for processing raw Clone Hero song file data, such as .ini, .chart, and .opus files
# auth: Benjamin Piro (benpiro1118@gmail.com)
# date: 15 February, 2026
#

import io
import os
import re
import bisect
import numpy as np
import polars as pl
from abc import ABC
from enum import Enum
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

class CloneHeroSection(ABC):
    def __init__(self, name: str):
        self.name = name


# ------------------------ #
#   Song Section Parsing   #
# ------------------------ #


class SongMetadata(CloneHeroSection):
    def __init__(self, name: str):
        super().__init__(name)
        
        # Resolution is the only mandatory field in the [Song] section of a .chart file
        # ref: https://docs.google.com/document/d/1v2v0U-9HQ5qHeccpExDOLJ5CMPZZ3QytPmAG5WF0Kzs/mobilebasic
        self.resolution = 192   # default resolution, can be updated based on parsed metadata
        self.offset = 0.0       # optional offset value in ms for aligning chart data with audio

    def append(self, key: str, value: str):
        if key == "Resolution":
            self.resolution = int(value)
        elif key == "Offset":
            # value may be in an international format with a comma as the decimal separator,
            # so we replace it with a dot before converting to float
            value = value.replace(',', '.')
            self.offset = float(value)
        else:
            # we can ignore other metadata fields for the purposes of this project,
            # but they can be added here as needed in the future
            pass


metadata_regex = r'(Resolution|Offset)\s*=\s*"?([^"\n]+)"?'   # only need the resolution field


def _parse_song_section(section_content: str) -> SongMetadata:
    song_metadata = SongMetadata(name="Song")

    for line in section_content.splitlines():
        match = re.match(metadata_regex, line.strip())
        if match:
            key = match.group(1)
            value = match.group(2)
            song_metadata.append(key, value)

    return song_metadata


# ----------------------------- #
#   SyncTrack Section Parsing   #
# ----------------------------- #


class TempoChangeMarker:
    def __init__(self, tick: int, bpm: int):
        self.tick = tick    # the tick at which this tempo change occurs
        self.bpm = bpm      # the BPM value for this tempo change, multiplied by 1000
    
    def to_numpy(self) -> np.ndarray:
        # convert this tempo change marker to a numpy array representation for easier processing
        # structure: [tick, bpm]
        return np.array([self.tick, self.bpm], dtype=int)


class SyncTrackData(CloneHeroSection):
    def __init__(self, name: str):
        super().__init__(name)
        self.tempo_changes: list[TempoChangeMarker] = []

    def append(self, tick: int, bpm: int):
        marker = TempoChangeMarker(tick=tick, bpm=bpm)
        self.tempo_changes.append(marker)

    def to_numpy(self) -> np.ndarray:
        # convert the tempo change markers in this sync track to a numpy array representation for easier processing
        return np.array([marker.to_numpy() for marker in self.tempo_changes], dtype=int)


synctrack_regex = r'(\d+)\s*=\s*B\s*(\d+)'   # matches lines like "0 = B 120000" (tick = 0, BPM = 120)


def _parse_synctrack_section(section_content: str) -> SyncTrackData:
    synctrack_data = SyncTrackData(name="SyncTrack")

    for line in section_content.splitlines():
        match = re.match(synctrack_regex, line.strip())
        if match:
            tick = int(match.group(1))
            bpm = int(match.group(2))
            synctrack_data.append(tick, bpm)

    return synctrack_data


# ------------------------ #
#   Note Section Parsing   #
# ------------------------ #


class NoteType(Enum):
    REGULAR = 0
    HOPO = 1
    TAP = 2


fret_mapping = {
    0: 'green',
    1: 'red',
    2: 'yellow',
    3: 'blue',
    4: 'orange',
    7: 'open'
}


class Fret():
    pressed: bool = False
    sustain_length: int = 0


class NoteFrame:
    
    def __init__(self, tick: int):
        self.tick = tick
        self.frets = {
            'green': Fret(),    # corresponds to the green fret lane in Clone Hero
            'red': Fret(),      # corresponds to the red fret lane in Clone Hero
            'yellow': Fret(),   # corresponds to the yellow fret lane in Clone Hero
            'blue': Fret(),     # corresponds to the blue fret lane in Clone Hero
            'orange': Fret(),   # corresponds to the orange fret lane in Clone Hero
            'open': Fret()      # corresponds to open notes (e.g. strumming with no pressed frets, 
                                #   which can still be combined with other frets) in Clone Hero
        }
        self.note_type = NoteType.REGULAR   # default to regular (strummed) note, can be updated to HOPO or TAP as needed

    def update(self, fret_id: int, sustain_length: int):
        # add a fret to this note marker (e.g. for chords or adding a HOPO marker to an existing note)
        if (fret_id >= 0 and fret_id <= 4) or fret_id == 7:
            fret_name = fret_mapping[fret_id]
            self.frets[fret_name].pressed = True
            self.frets[fret_name].sustain_length = sustain_length
        elif fret_id == 5:
            self.note_type = NoteType.HOPO
        elif fret_id == 6:
            self.note_type = NoteType.TAP
        else:
            raise ValueError(f"Invalid fret ID: {fret_id}")
    
    def to_numpy(self) -> np.ndarray:
        # convert this note marker to a numpy array representation for easier processing
        # structure: [
        #   tick,                                                                                       # int: the tick at which this note occurs
        #   green_pressed, red_pressed, yellow_pressed, blue_pressed, orange_pressed, open_pressed,     # int: 1 if the corresponding fret is pressed for this note
        #   green_sustain, red_sustain, yellow_sustain, blue_sustain, orange_sustain, open_sustain,     # int: sustain length for the corresponding fret (0 if not sustained)
        #   note_type                                                                                   # int: the type of the note (0 = regular, 1 = HOPO, 2 = TAP)
        # ]
        
        # first add tick that the note occurs at
        fret_data = [self.tick]

        # then add fret pressed data
        for fret_name in ['green', 'red', 'yellow', 'blue', 'orange', 'open']:
            fret_data.append(1 if self.frets[fret_name].pressed else 0)
        
        # then add sustain length data for each fret
        for fret_name in ['green', 'red', 'yellow', 'blue', 'orange', 'open']:
            fret_data.append(self.frets[fret_name].sustain_length)
        
        # finally add note type data
        fret_data.append(self.note_type.value)

        return np.array(fret_data, dtype=int)


class InstrumentTrack(CloneHeroSection):
    """
    Class representing a single instrument track in a .chart file (e.g. ExpertSingle guitar track).
    Contains a list of NoteFrame objects representing the notes in the track, as well as any relevant metadata.
    """
    def __init__(self, name: str):
        super().__init__(name)
        self.note_markers: list[NoteFrame] = []
        # additional metadata fields can be added here as needed (e.g. difficulty level, star power sections, etc.)

    def append(self, tick: int, chart_val: int, sustain_length: int):
        # determine if new note marker refers to a new note or an update to the previous note marker (e.g. for sustains or adding frets)
        is_new_note = (not self.note_markers) or (tick != self.note_markers[-1].tick)
        
        if is_new_note:
            # new note marker
            marker = NoteFrame(tick=tick)
        else:
            # update to previous note marker
            marker = self.note_markers[-1]

        try:
            marker.update(chart_val, sustain_length)
            if is_new_note:
                self.note_markers.append(marker)
        except Exception as e:
            # When Clone Hero encounters malformed note data in a .chart file, it simply ignores the problematic note and continues processing the rest of the chart without crashing.
            # To be robust against potential issues in the .chart files we process, we will do the same by catching any exceptions that occur when updating a note marker and logging a warning, 
            # but allowing processing to continue for the rest of the chart.
            print(f"Warning: Skipping note at tick {tick} with chart value {chart_val} due to error: {e}")        
        
    
    def to_numpy(self) -> np.ndarray:
        # convert the note marker in this track to a numpy array representation for easier processing
        return np.array([marker.to_numpy() for marker in self.note_markers], dtype=int)


note_pattern = re.compile(r'(\d+)\s*=\s*N\s*(\d+)\s*(\d+)')


def _parse_notes_section(section_name: str, section_content: str) -> InstrumentTrack:
    # create an instrument track object to hold the note data for this section
    instrument_track = InstrumentTrack(name=section_name)

    # extracting only note data (no star power or event data for the purposes of this project)
    for line in section_content.splitlines():
        match = note_pattern.match(line.strip())
        if match:
            tick = int(match.group(1))
            chart_val = int(match.group(2))
            sustain_length = int(match.group(3))
            
            # add this note data to the instrument track
            # the append method will handle determining whether this is a new note marker or an update to the previous note marker
            instrument_track.append(tick, chart_val, sustain_length)

    return instrument_track


class ChartProcessor:
    """
    Class for processing a single .chart file, which contains the note data for a Clone Hero song.
    Note: Only extracts [ExpertSingle] guitar chart data for the purposes of this project.

    References:
        - .chart file format:   https://docs.google.com/document/d/1v2v0U-9HQ5qHeccpExDOLJ5CMPZZ3QytPmAG5WF0Kzs/mobilebasic
        - CH documentation:     https://github.com/TheNathannator/GuitarGame_ChartFormats/tree/main/docs/Chart-File-Formats/chart-format
        - Audio2Hero Code Ref:  https://github.com/3podi/audio2chart/blob/main/chart/chart_processor.py
    """

    def __init__(self):
        
        # .chart file section definitions (only a subset of sections are relevant for our purposes)
        self.sections = [
            "Song",             # general song metadata (overlaps with .ini metadata, but may contain additional fields)
            "SyncTrack",        # tempo and time signature changes
            "ExpertSingle"      # expert single guitar chart data
        ]

        # Regex patterns for identifying section headers
        self.section_regexes = {
            section_header: re.compile(rf'\[{section_header}\]\s*\{{(.*?)\}}', re.DOTALL)
            for section_header in self.sections
        }

    def parse_chart(self, chart_byte_stream: io.BytesIO) -> tuple[int, float, np.ndarray, np.ndarray]:
        """
        Parses the given .chart file byte stream and extracts relevant metadata and note data.
        """

        # extracted chart section data will be stored here
        section_content = {}

        # read .chart file bytes and decode to text
        chart_byte_stream.seek(0)
        # using 'utf-8-sig' to handle potential BOM in .chart files, and ignoring decoding errors to be robust against malformed files
        chart_text = chart_byte_stream.read().decode('utf-8-sig', errors='ignore')

        # extract sections using regex
        for section, regex in self.section_regexes.items():
            match = regex.search(chart_text)
            if match:
                section_content[section] = match.group(1).strip()
        
        # extract relevant metadata from each section
        extracted_metadata = None
        if "Song" in section_content:
            song_section = section_content["Song"]
            extracted_metadata = _parse_song_section(song_section)
        
        # extract tempo and time signature changes from SyncTrack section
        extracted_synctrack_data = None
        if "SyncTrack" in section_content:
            synctrack_section = section_content["SyncTrack"]
            extracted_synctrack_data = _parse_synctrack_section(synctrack_section)
        
        # extract expert single guitar chart data from ExpertSingle section
        extracted_expertsingle_data = None
        if "ExpertSingle" in section_content:
            expertsingle_section = section_content["ExpertSingle"]
            extracted_expertsingle_data = _parse_notes_section("ExpertSingle", expertsingle_section)
        
        # return song resolution, tempo changes numpy array, and expert single note data numpy array
        song_resolution: int = extracted_metadata.resolution if extracted_metadata else None
        song_offset: float = extracted_metadata.offset if extracted_metadata else None
        tempo_changes: np.ndarray = extracted_synctrack_data.to_numpy() if extracted_synctrack_data else None
        expertsingle_notes: np.ndarray = extracted_expertsingle_data.to_numpy() if extracted_expertsingle_data else None

        return song_resolution, song_offset, tempo_changes, expertsingle_notes


########################################
#   .chart Time Conversion Utilities   #
########################################

"""
This section contains utility functions for converting between ticks and milliseconds in Clone Hero charts, 
based on the song resolution and tempo changes extracted from the .chart file. These functions are essential 
for aligning the note data with the corresponding audio data when processing Clone Hero songs.
"""

def process_bpm_events(bpm_events: np.ndarray, resolution: int) -> np.ndarray:
    """
    Converts BPM events from tick-based timing to absolute time in milliseconds, based on the song resolution
    
    Args:
        - bpm_events: numpy array of shape (num_events, 2) where each row is [tick, bpm] 
            - assumed to be sorted by tick in ascending order
            - bpm values are multiplied by 1000 (e.g., a BPM of 120 is represented as 120000)
        - resolution: the song resolution (ticks per quarter note)
    Returns:
        - numpy array of shape (num_events, 2) where each row is [time_ms, bpm]
    """

    segments = []
    last_tick = 0
    cum_time = 0.0


    for tick, bpm in bpm_events:
        if tick < last_tick:
            raise ValueError("BPM events must be sorted by tick in ascending order")
        
        bpm = bpm / 1000.0   # convert BPM back to standard representation (e.g., 120000 -> 120.0 BPM)

        # calculate time elapsed since last BPM event
        if segments:
            prev_bpm = segments[-1][1]
            delta_ticks = tick - last_tick

            # calculate time elapsed since last BPM event
            delta_ticks = tick - last_tick
            delta_time = (delta_ticks / resolution) * (60.0 / prev_bpm)  # convert ticks to seconds based on current BPM
            cum_time += delta_time
        
        segments.append((tick, bpm, cum_time))
        last_tick = tick
    
    return segments


def tick_to_seconds(tick: int, bpm_ticks: np.ndarray,bpm_segments: list[tuple[int, float, float]], resolution: int) -> float:
    """
    Convert a tick position to absolute time in seconds based on the BPM segments and song resolution
    
    Args:
        - tick: the tick position to convert
        - bpm_segments: list of tuples (tick, bpm, cum_time) where:
            - tick is the tick at which the BPM change occurs
            - bpm is the true beats-per-minute value for that segment
            - cum_time is the cumulative time in seconds up to that tick
    
    Returns:
        - Absolute time in seconds
    """

    if not bpm_segments:
        raise ValueError("bpm_segments list cannot be empty")
    
    # Find the appropriate bpm segment for the given tick using binary search
    idx = np.searchsorted(bpm_ticks, tick, side='right') - 1

    # Handle edge case where tick is before first bpm event
    if idx < 0:
        idx = 0
        # if tick is before first bpm event, use first bpm
        bpm = bpm_segments[0][1]
        delta_ticks = tick - 0
        delta_time = (delta_ticks / resolution) * (60.0 / bpm)
        return delta_time
    
    base_tick, bpm, base_time = bpm_segments[idx]
    delta_ticks = tick - base_tick
    delta_time = (delta_ticks / resolution) * (60.0 / bpm)
    return base_time + delta_time


def convert_notes_to_seconds(notes: np.ndarray, bpm_events: np.ndarray, resolution: int, offset: float) -> np.ndarray:
    """
    Converts notes from tick-based timing to absolute time in seconds
    
    Args:
        - notes: numpy array of shape (num_notes, 14) (see NoteFrame.to_numpy for structure) where the first column is tick and the rest are note data
        - bpm_events: numpy array of shape (num_events, 2) where each row is [tick, bpm] (assumed to be sorted by tick in ascending order)
        - resolution: the song resolution (ticks per quarter note)
        - offset: the song offset in milliseconds
    Returns:
        - numpy array of shape (num_notes, 14) where the first column is time in seconds and the rest are note data
    """

    if notes is None or notes.shape[0] == 0:
        raise ValueError("No notes provided for conversion")
    
    if bpm_events is None or bpm_events.shape[0] == 0:
        raise ValueError("No BPM events provided for conversion")
    
    if resolution <= 0:
        raise ValueError("Resolution must be a positive integer")
    
    if offset is None:
        raise ValueError("Offset must be provided for conversion")
    
    # first convert BPM events to absolute time in seconds
    processed_bpm_events = process_bpm_events(bpm_events, resolution)
    note_times = []

    for note_frame in notes:
        # (1) retrieve note frame data

        # the tick at which this note occurs
        note_tick = note_frame[0]
        # the note press event data for each fret (binary)
        note_pressed_data = note_frame[1:7]   # columns 1-6 correspond to green, red, yellow, blue, orange, and open fret pressed data
        # the note sustain length data for each fret (0 if not sustained)
        note_sustain_data = note_frame[7:13]   # columns 7-12 correspond to green, red, yellow, blue, orange, and open fret sustain length data
        # the note type (0 = regular, 1 = HOPO, 2 = TAP)
        note_type = note_frame[13]

        # (2) convert note tick to time in seconds based on BPM events and song resolution

        # separate ticks from processed_bpm_events for binary search
        bpm_ticks = bpm_events[:, 0]
        start_sec = tick_to_seconds(note_tick, bpm_ticks, processed_bpm_events, resolution)

        note_press_durations = []
        for i in range(note_pressed_data.shape[0]):   
            # calculate duration of sustains based on sustain length data and BPM events
            if note_pressed_data[i] == 1 and note_sustain_data[i] > 0:
                end_tick = note_tick + note_sustain_data[i]
                end_sec = tick_to_seconds(end_tick, bpm_ticks, processed_bpm_events, resolution)
                duration_sec = end_sec - start_sec
            else:
                duration_sec = 0.0
            # append duration of this note press to the list of note press durations for this note frame
            note_press_durations.append(duration_sec)
        
        # (3) apply song offset (in seconds) to start_sec to get absolute time for this note frame
        absolute_time = start_sec + offset

        # (4) create a new note frame with absolute time and the rest of the note data
        note_times.append([absolute_time] + note_pressed_data.tolist() + note_press_durations + [note_type])
    
    # (5) convert list of note frames to numpy array for easier processing
    note_times = np.array(note_times, dtype=float)

    return note_times


def calculate_note_density_summary(note_frames: np.ndarray) -> tuple[float, float, float, float, float]:
    """
    Utility function to calculate summary statistics about note onset times for a given song.
    Includes:
    - average note onset delta time (in seconds)
    - median note onset delta time (in seconds)
    - minimum delta time between consecutive note onsets (in seconds)
    - maximum delta time between consecutive note onsets (in seconds)
    - standard deviation of delta times between consecutive note onsets (in seconds)
    """
    if note_frames is None or note_frames.shape[0] < 2:
        raise ValueError("At least two note frames are required to calculate note density summary")
    
    # calculate delta times between consecutive note frames
    delta_times = np.diff(note_frames[:, 0])  # assuming the first column of each note frame is the absolute time in seconds

    # calculate summary statistics (between the first and last note onsets, ignoring sustains)
    avg_delta = np.mean(delta_times)  # average time between note onsets in seconds
    median_delta = np.median(delta_times)  # median time between note onsets in seconds
    min_delta = np.min(delta_times)
    max_delta = np.max(delta_times)
    std_delta = np.std(delta_times)

    return avg_delta, median_delta, min_delta, max_delta, std_delta


def parse_chart_file(chart_byte_stream: io.BytesIO) -> tuple[int, float, np.ndarray, np.ndarray]:
    """
    Parses the given .chart file byte stream and extracts relevant metadata and note data.
    """
    chart_processor = ChartProcessor()
    return chart_processor.parse_chart(chart_byte_stream)