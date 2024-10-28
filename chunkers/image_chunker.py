import multiprocessing
from pathlib import Path

import ffmpeg

from utils import get_chunks_folder_name


def get_image_dimensions(file_path: str) -> tuple[int, int]:
    probe = ffmpeg.probe(file_path)
    video_info = next(s for s in probe['streams'] if s['codec_type'] == 'video')
    width = int(video_info['width'])
    height = int(video_info['height'])
    return width, height

def split_chunk(args: tuple[str, str, int, int, int, int]) -> str:
    input_file, output_file, left, top, width, height = args
    (
        ffmpeg
        .input(input_file)
        .crop(left, top, width, height)
        .output(output_file)
        .overwrite_output()
        .run(quiet=True)
    )
    return output_file


def split_image(file_path: str, num_chunks: int = 2) -> list[str]:
    file_path = Path(file_path)
    file_name, file_extension = get_chunks_folder_name(file_path)

    width, height = get_image_dimensions(str(file_path))

    # Calculate grid dimensions based on num_chunks
    num_chunks_horizontal = int(num_chunks ** 0.5)
    num_chunks_vertical = num_chunks // num_chunks_horizontal

    # Calculate chunk dimensions
    chunk_width = (width + num_chunks_horizontal - 1) // num_chunks_horizontal
    chunk_height = (height + num_chunks_vertical - 1) // num_chunks_vertical

    chunks_folder = Path(f"{file_name}_chunks")
    chunks_folder.mkdir(exist_ok=True)

    split_args = []
    for y in range(num_chunks_vertical):
        for x in range(num_chunks_horizontal):
            left = x * chunk_width
            top = y * chunk_height
            w = min(chunk_width, width - left)
            h = min(chunk_height, height - top)

            output_file = chunks_folder / f'{file_name}.chunk{y*num_chunks_horizontal+x+1}{file_extension}'
            split_args.append((str(file_path), str(output_file), left, top, w, h))

    with multiprocessing.Pool() as pool:
        chunk_files = pool.map(split_chunk, split_args)

    return chunk_files
