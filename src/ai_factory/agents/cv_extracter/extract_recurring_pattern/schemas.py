from pydantic import BaseModel, Field
from typing import List


class RecurringPatternsSectionOutput(BaseModel):
    recurring_patterns: List[str] = Field(
        description=(
            "Flat, ordered list of recurring pattern statements from Section 5"
            "Each item must be a complete sentence/string with all quantitative details preserved."
        )
    )
