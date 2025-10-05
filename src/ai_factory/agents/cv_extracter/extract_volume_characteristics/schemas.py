from typing import List, Optional, Dict, Literal
from pydantic import BaseModel, Field

Weekday = Literal["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
Flavor = Literal["weekday", "global"]


class StatBlock(BaseModel):
    # Use float for central tendency & dispersion; int for min/max when they are counts.
    min: Optional[int] = Field(default=None, description="Minimum observed value")
    max: Optional[int] = Field(default=None, description="Maximum observed value")
    mean: Optional[float] = Field(default=None, description="Mean value")
    median: Optional[float] = Field(default=None, description="Median value")
    mode: Optional[float] = Field(default=None, description="Mode value")
    stdev: Optional[float] = Field(
        default=None, description="Standard deviation, if present"
    )


class PerWeekdayRow(BaseModel):
    day: Weekday = Field(description="Weekday (Monday to Sunday)")
    # The 'rows' block corresponds to 'Total Rows Processed' in Day-of-Week Summary.
    rows: StatBlock = Field(
        description="Stats for total rows processed on this weekday"
    )
    empty_files: StatBlock = Field(description="Stats for empty files on this weekday")
    duplicated_files: StatBlock = Field(
        description="Stats for duplicated files on this weekday"
    )
    failed_files: StatBlock = Field(
        description="Stats for failed files on this weekday"
    )
    # Optional short narrative if present in the table ("High volume day ...")
    analysis_note: Optional[str] = Field(
        default=None, description="One-line analysis per weekday if present"
    )


class Normal95(BaseModel):
    lo: Optional[int] = Field(
        default=None, description="Lower bound for ~95% normal interval (per-file rows)"
    )
    hi: Optional[int] = Field(
        default=None, description="Upper bound for ~95% normal interval (per-file rows)"
    )


class MaxEmptyFilesDay(BaseModel):
    date: Optional[str] = Field(
        default=None, description="YYYY-MM-DD of the max-empty-files day"
    )
    count: Optional[int] = Field(
        default=None, description="Number of empty files on that max day"
    )


class RowsStats(BaseModel):
    min: Optional[int] = Field(default=None)
    max: Optional[int] = Field(default=None)
    mean: Optional[float] = Field(default=None)
    median: Optional[float] = Field(default=None)
    stdev: Optional[float] = Field(default=None)
    mode: Optional[float] = Field(default=None)


class DailyTotalsStats(BaseModel):
    # Sum(rows) per day (global flavor).
    min: Optional[int] = Field(default=None)
    max: Optional[int] = Field(default=None)
    mean: Optional[float] = Field(default=None)
    median: Optional[float] = Field(default=None)
    stdev: Optional[float] = Field(default=None)


class OverallBlock(BaseModel):
    file_count: Optional[int] = Field(
        default=None, description="Number of files used to compute stats"
    )
    rows_stats: RowsStats = Field(
        default_factory=RowsStats, description="Per-file row distribution overall"
    )
    normal_95: Normal95 = Field(
        default_factory=Normal95, description="95% interval for per-file rows"
    )
    empty_files: Optional[int] = Field(
        default=None, description="Total empty files in the stats window"
    )
    low_rows_files_lt_100: Optional[int] = Field(
        default=None, description="Total low-row files (<100) in the stats window"
    )
    max_empty_files_day: MaxEmptyFilesDay = Field(
        default_factory=MaxEmptyFilesDay, description="Max empty-files day, if present"
    )
    daily_totals: DailyTotalsStats = Field(
        default_factory=DailyTotalsStats, description="Stats for sum(rows) per day"
    )


class PresenceFlags(BaseModel):
    per_weekday_present: bool = Field(
        description="True if weekday stats were actually present in the source"
    )
    overall_present: bool = Field(
        description="True if overall/global stats were actually present in the source"
    )


class VolumeCharacteristicsOutput(BaseModel):
    extraction_flavor: Flavor = Field(
        description="'weekday' if Day-of-Week table was parsed, 'global' if Volume Characteristics was parsed"
    )
    presence: PresenceFlags = Field(
        description="Flags indicating which sub-blocks had real data"
    )

    # Weekday table: ALWAYS 7 rows Mon–Sun; nulls allowed if absent in source.
    per_weekday: List[PerWeekdayRow] = Field(
        description="Per-weekday stats (rows, empty, duplicated, failed). Must include 7 rows Mon–Sun."
    )

    # Global block from Volume Characteristics: always present with nulls if absent in source.
    overall: OverallBlock = Field(
        description="Overall/global stats (per-file distribution, 95% interval, daily totals, etc.)."
    )

    # Optional, for traceability: list brief strings like “parsed Day-of-Week table; no 95% interval present”
    inference_notes: Optional[List[str]] = Field(
        default=None, description="Short notes on what was found/normalized"
    )
