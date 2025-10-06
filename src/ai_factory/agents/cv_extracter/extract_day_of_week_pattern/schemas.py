from typing import List, Optional, Literal
from pydantic import BaseModel, Field

Weekday = Literal["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
Flavor = Literal["weekday", "entity", "both"]


class StatBlock(BaseModel):
    # Always include all six keys; null if unknown
    min: Optional[float] = None
    max: Optional[float] = None
    mean: Optional[float] = None
    median: Optional[float] = None
    mode: Optional[float] = None
    stdev: Optional[float] = None


class PerWeekdayRow(BaseModel):
    day: Weekday
    rows: StatBlock
    empty_files: StatBlock
    duplicated_files: StatBlock
    failed_files: StatBlock
    analysis_note: Optional[str] = None


class EntityWeekdayRow(BaseModel):
    entity: str
    day: Weekday
    median_files: Optional[float] = None
    median_rows: Optional[float] = None
    median_duplicated: Optional[float] = None
    median_failed: Optional[float] = None
    median_empty: Optional[float] = None
    mode_lag_days: Optional[int] = None


class Presence(BaseModel):
    has_weekday_table: bool
    has_entity_weekday_table: bool
    has_notes: bool


class ColumnAudit(BaseModel):
    columns_present: List[str]
    columns_missing: List[str]
    extra_columns: List[str]


class DayOfWeekPatternOutput(BaseModel):
    flavor: Flavor
    presence: Presence
    weekday: List[PerWeekdayRow]
    entity_weekday: List[EntityWeekdayRow]
    exceptions: List[str] = Field(default_factory=list)
    general_notes: List[str] = Field(default_factory=list)
    column_audit: ColumnAudit
