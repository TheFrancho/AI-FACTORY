from pydantic import BaseModel, Field
from typing import List


class ExtraCommentsForAnalystSectionOutput(BaseModel):
    insights_recommendations: List[str] = Field(
        description=(
            "Flat, ordered list of Section 6 insights and recommendations. "
            "Each item is a complete sentence preserving quantitative details."
        )
    )
