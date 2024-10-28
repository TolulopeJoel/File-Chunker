import multiprocessing
from pathlib import Path

import ffmpeg

from utils import get_chunks_folder_name


def get_video_duration(file_path: str) -> float:
    probe = ffmpeg.probe(file_path)
    video_info = next(s for s in probe['streams'] if s['codec_type'] == 'video')
    return float(video_info['duration'])

def split_chunk(args: tuple[str, str, float, float]) -> str:
    input_file, output_file, start_time, duration = args
    (
        ffmpeg
        .input(input_file, ss=start_time, t=duration)
        .output(output_file, c='copy')
        .overwrite_output()
        .run(quiet=True)
    )
    return output_file

def split_video(file_path: str, num_chunks: int = 2) -> list[str]:
    file_path = Path(file_path)
    file_name, file_extension = get_chunks_folder_name(file_path)

    duration = get_video_duration(file_path)
    chunk_duration = duration / num_chunks
    if duration % chunk_duration != 0:
        num_chunks += 1

    chunks_folder = Path(f"{file_name}_chunks")
    chunks_folder.mkdir(exist_ok=True)

    split_args = []
    for i in range(num_chunks):
        start = i * chunk_duration
        end = min((i + 1) * chunk_duration, duration)
        output_file = chunks_folder / f'{file_name}.chunk{i+1}{file_extension}'
        split_args.append((str(file_path), str(output_file), start, end - start))

    with multiprocessing.Pool() as pool:
        chunk_files = pool.map(split_chunk, split_args)

    return chunk_files
