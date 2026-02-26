#
# file: src/klangscribe-orchestration/utils/audio_processing.py
# desc: Utilities for processing audio .opus/.wav files
# auth: Benjamin Piro (benpiro1118@gmail.com)
# date: 15 February, 2026
#

import io
import os
import wave
import ffmpeg
import tempfile
import numpy as np
from pathlib import Path
from pydub import AudioSegment


#############################
#   .opus/.wav Processing   #
#############################

DEFAULT_SAMPLE_RATE = 22050     # good balance between quality and size for ML tasks


def opus_to_wav_bytes(opus_bytes: io.BytesIO, target_sample_rate: int = DEFAULT_SAMPLE_RATE) -> io.BytesIO:
    """
    Converts .opus audio bytes to .wav format with the specified sample rate, and returns the resulting bytes.
    """
    # Configure process to read from .opus bytes and output .wav bytes
    process = (
        ffmpeg
        .input('pipe:0', format='ogg', acodec='opus')  # read from stdin as .ogg/.opus format
        .output('pipe:1', format='wav', acodec='pcm_s16le', ar=target_sample_rate)  # output to stdout as .wav with specified sample rate
        .overwrite_output()  # allow overwriting output if needed
        .run_async(pipe_stdin=True, pipe_stdout=True, pipe_stderr=True)
    )
    # write input bytes to process stdin and get output bytes from stdout
    opus_bytes.seek(0)
    stdout, stderr = process.communicate(input=opus_bytes.read())
    # check if the process completed successfully
    if process.returncode != 0:
        raise Exception(f"ffmpeg conversion failed: {stderr.decode('utf-8')}")
    return io.BytesIO(stdout)


def merge_wav_bytes(wav_bytes_list: list[io.BytesIO]) -> io.BytesIO:
    """
    Merges multiple .wav byte streams into a single .wav byte stream
    without writing to disk.
    
    Note: This function assumes all input .wav files have the same sample rate, number of channels, and bit depth.
    Expecting 16-bit PCM .wav files with 22.05 kHz sample rate, but function adapts to other formats as well.
    """

    if not wav_bytes_list:
        raise ValueError("No wav bytes provided for merging")

    # read format info from first .wav file
    first_wav = wav_bytes_list[0]
    first_wav.seek(0)  # ensure we're at the start of the file
    with wave.open(first_wav) as w:
        channels = w.getnchannels()
        sample_width = w.getsampwidth()
        frame_rate = w.getframerate()
    
    # Map sample width to numpy dtype
    dtype_map = {
        1: np.uint8,    # 8-bit PCM
        2: np.int16,    # 16-bit PCM
        3: None,        # 24-bit PCM is not directly supported by numpy, would require special handling 
        4: np.int32     # 32-bit PCM
    }
    dtype = dtype_map.get(sample_width)
    if dtype is None:
        raise ValueError(f"Unsupported sample width: {sample_width} bytes")
    
    # Extract PCM as numpy arrays and mix (overlay) them
    arrays = []
    for wav_bytes in wav_bytes_list:
        wav_bytes.seek(0)  # ensure we're at the start of the file
        with wave.open(wav_bytes) as w:
            frames = w.readframes(w.getnframes())
            arrays.append(np.frombuffer(frames, dtype=dtype))
    
    # Pad shorter arrays to the length of the longest array
    max_len = max(len(a) for a in arrays)
    padded = [np.pad(a, (0, max_len - len(a))) for a in arrays]

    # Sum in float to avoid clipping, then normalize back to original dtype range
    mixed = np.sum(padded, axis=0, dtype=np.float64)
    info = np.iinfo(dtype)
    peak = np.max(np.abs(mixed))
    if peak > info.max:
        mixed *= info.max / peak
    raw_pcm = mixed.astype(dtype).tobytes()

    # Map sample sample width to ffmpeg raw audio format
    format_map = {
        1: 'u8',        # unsigned 8-bit PCM
        2: 's16le',     # signed 16-bit PCM
        4: 's32le'      # 24 and 32-bit PCM can be treated as 32-bit for ffmpeg input
    }

    process = (
        ffmpeg
        .input('pipe:', format=format_map[sample_width], ar=frame_rate, ac=channels)  # read raw PCM from stdin
        .output('pipe:', format='wav')
        .run_async(pipe_stdin=True, pipe_stdout=True, pipe_stderr=True)
    )

    stdout, stderr = process.communicate(input=raw_pcm)
    if process.returncode != 0:
        raise Exception(f"ffmpeg conversion failed: {stderr.decode('utf-8')}")
    return io.BytesIO(stdout)


def merge_opus_bytes(opus_bytes_list: list[io.BytesIO]) -> io.BytesIO:
    """
    Merges multiple .opus byte streams into a single .opus byte stream.
    """

    # combine .opus files using pydub
    audio_files = [AudioSegment.from_file(b, format="ogg") for b in opus_bytes_list]

    # start with the first audio and overlay the rest
    mixed = audio_files[0]
    for audio in audio_files[1:]:
        mixed = mixed.overlay(audio)
    
    # normalize combined audio to prevent clipping
    if mixed.max_dBFS > 0.0:
        mixed = mixed.apply_gain(-mixed.max_dBFS)
    
    # export combined audio to .opus bytes
    output_bytes = io.BytesIO()
    mixed.export(output_bytes, format="ogg", codec="libopus")
    output_bytes.seek(0)
    return output_bytes