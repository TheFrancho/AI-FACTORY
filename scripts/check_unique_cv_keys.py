"""
Simple helper script to print all the intros produced by the initial agent, to explore data consistency
"""

import json
import os
from pathlib import Path


def get_file_list(folder_path: Path):
    files_list = os.listdir(folder_path)
    files_path = sorted(
        [
            os.path.join(folder_path, file_path)
            for file_path in files_list
            if file_path.endswith("UTC")
        ]
    )
    return files_path


folder_path = Path("dataset_files")
files_path = get_file_list(folder_path)

for file in files_path:
    current_file_path = os.path.join(file, "files.json")
    past_week_file_path = os.path.join(file, "files_last_weekday.json")

    with (
        open(current_file_path, "r") as current_week_file,
        open(past_week_file_path, "r") as past_week_file,
    ):
        current_file_content = json.load(current_week_file)
        past_week_content = json.load(past_week_file)

    print(f"For folder {file} - current week ids: {current_file_content.keys()}")
    print(f"For folder {file} - past week ids: {past_week_content.keys()}")
