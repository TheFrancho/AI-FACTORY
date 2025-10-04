"""
Simple helper script to print all the intros produced by the initial agent, to explore data consistency
"""

import json
import os
from pathlib import Path


def get_file_list(folder_path: Path):
    files_list = os.listdir(folder_path)
    files_path = sorted(
        [os.path.join(folder_path, file_path) for file_path in files_list]
    )
    return files_path


folder_path = Path("custom_outputs")
files_path = get_file_list(folder_path)

for file_path in files_path:
    print(f"Working with file {file_path} - {os.path.basename(file_path)}")

    with open(file_path, "r") as file:
        file_content = json.load(file)

    print(file_content["split_sections"]["markdown_title"])
