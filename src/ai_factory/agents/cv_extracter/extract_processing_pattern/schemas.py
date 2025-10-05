from typing import List, Optional, Dict, Literal
from pydantic import BaseModel, Field

Weekday = Literal["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


class FileProcessingStatsRow(BaseModel):
    day: Weekday = Field(description="Weekday (Monday to Sunday)")
    mean_files: int = Field(description="Mean files for the day")
    median_files: int = Field(description="Median files for the day")
    mode_files: int = Field(description="Mode files for the day")
    stddev_files: int = Field(
        description="Standard deviation of files (count, not minutes)"
    )
    min_files: int = Field(description="Minimum files observed")
    max_files: int = Field(description="Maximum files observed")


class UploadScheduleRow(BaseModel):
    day: Weekday = Field(description="Weekday (Monday to Sunday)")
    upload_hour_slot_mean_utc: Optional[str] = Field(
        description="Mean upload hour slot in HH:MM (UTC), or None if 'No observed data'",
        default=None,
    )
    upload_hour_slot_median_utc: Optional[str] = Field(
        description="Median upload hour slot in HH:MM (UTC), or None", default=None
    )
    upload_hour_slot_mode_utc: Optional[str] = Field(
        description="Mode upload hour slot in HH:MM (UTC), or None", default=None
    )
    upload_hour_slot_stddev_minutes: Optional[int] = Field(
        description="StdDev of upload time in minutes (normalize '00 h 49 m' → 49). None if N/A",
        default=None,
    )
    expected_window_utc: Optional[str] = Field(
        description="Expected window as 'HH:MM:SS–HH:MM:SS UTC' or None if not present",
        default=None,
    )

    upload_lag_days_mode: Optional[int] = Field(
        description="Mode of (filename_date - upload_date) in days. Negative means filename date < upload date",
        default=None,
    )
    lag_days_mode_note: Optional[str] = Field(
        description="Raw text when lag days mode is not numeric (e.g., 'No observed data')",
        default=None,
    )


class UploadSectionOutput(BaseModel):
    file_processing_stats_by_day: List[FileProcessingStatsRow] = Field(
        description="Table A: per-weekday file count stats"
    )
    upload_schedule_by_day: List[UploadScheduleRow] = Field(
        description="Table B: per-weekday upload timing stats (+ optional lag-days mode)"
    )
    format_and_status_bullets: List[str] = Field(
        description="Each bullet as a separate string. Include both File Format Analysis and Processing Status Summary lines."
    )

    status_percentages: Optional[Dict[str, float]] = Field(
        description="Normalized status percentages parsed from bullets (e.g., {'processed': 94.82, 'empty': 2.64, 'failed': 2.54}). Optional.",
        default=None,
    )
    schedule_notes: Optional[List[str]] = Field(
        description="Free-form notes like time-drift clusters (e.g., 'Feb–mid-Jun 15:10 ±20m; mid-Jul 12:15 ±15m'). Optional.",
        default=None,
    )
