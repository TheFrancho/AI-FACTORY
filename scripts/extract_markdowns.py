"""
Helper script that splits and print all categories by hardcoding sections, to explore data consistency
"""

import os
from pathlib import Path


INTRO_START = "# "
FILENAME_PATTERNS_START = "## **1"
UPLOAD_SCHEDULE_START = "## **2"
VOLUME_CHARACTERISTICS_START = "## **3"
DAY_OF_WEEK_START = "## **4"
RECURRING_PATTERNS_START = "## **5"
COMMENTS_FOR_ANALYST_START = "## **6"


def extract_text_chunk(
    text: str, start_marker: str, end_marker: str, end_of_file: bool = False
):
    """
    Extracts the content between two markers (inclusive of start_marker).
    If end_of_file=True, extracts from start_marker until the end of the file.
    """
    start_idx = text.find(start_marker)
    if start_idx == -1:
        return ""  # start marker not found

    if end_of_file:
        return text[start_idx:].rstrip()

    end_idx = text.find(end_marker, start_idx + len(start_marker))
    if end_idx == -1:
        return ""  # end marker not found

    return text[start_idx:end_idx].rstrip()


def read_file_list(md_folder_path: Path):
    files_list = os.listdir(md_folder_path)
    files_path = sorted(
        [
            os.path.join(md_folder_path, file_path)
            for file_path in files_list
            if file_path.lower().endswith(".md")
        ]
    )

    for file_path in files_path:

        print(f"START FILE {file_path}\n")
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        intro_section = extract_text_chunk(
            content, INTRO_START, FILENAME_PATTERNS_START
        )
        filename_section = extract_text_chunk(
            content, FILENAME_PATTERNS_START, UPLOAD_SCHEDULE_START
        )
        file_processing_pattern_section = extract_text_chunk(
            content, UPLOAD_SCHEDULE_START, VOLUME_CHARACTERISTICS_START
        )
        volume_characteristics_section = extract_text_chunk(
            content, VOLUME_CHARACTERISTICS_START, DAY_OF_WEEK_START
        )
        day_of_week_section_pattern = extract_text_chunk(
            content, DAY_OF_WEEK_START, RECURRING_PATTERNS_START
        )
        recurring_patterns_section = extract_text_chunk(
            content, RECURRING_PATTERNS_START, COMMENTS_FOR_ANALYST_START
        )
        comments_for_analyst_section = extract_text_chunk(
            content, COMMENTS_FOR_ANALYST_START, "", end_of_file=True
        )

        print("Intro Section:\n", intro_section, "\n", "-" * 50)
        print("\nFilename Patterns Section:\n", filename_section, "\n", "-" * 50)
        print(
            "\nFile Processing Section:\n",
            file_processing_pattern_section,
            "\n",
            "-" * 50,
        )
        print(
            "\nVolume Characteristics Section:\n",
            volume_characteristics_section,
            "\n",
            "-" * 50,
        )
        print("\nDay of Week Section:\n", day_of_week_section_pattern, "\n", "-" * 50)
        print(
            "\nRecurring Patterns Section:\n",
            recurring_patterns_section,
            "\n",
            "-" * 50,
        )
        print(
            "\nComments for Analyst Section:\n",
            comments_for_analyst_section,
            "\n",
            "-" * 50,
        )

        print(f"END FILE {file_path}", "\n\n\n")
        input("enter to continue")
        os.system("clear")
    print("Finished analyzing all file, woah")


md_folder_path = Path("dataset_files/datasource_cvs")
read_file_list(md_folder_path)
