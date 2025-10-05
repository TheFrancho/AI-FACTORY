import os
from pathlib import Path


def get_file_list(folder_path: Path):
    files_list = os.listdir(folder_path)
    files_path = sorted(
        [os.path.join(folder_path, file_path) for file_path in files_list]
    )
    return files_path
