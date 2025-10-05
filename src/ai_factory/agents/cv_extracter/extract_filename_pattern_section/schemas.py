from pydantic import BaseModel, Field
from typing import List, Dict, Optional


class FilenameSectionOutput(BaseModel):
    filename_canonical: str = Field(description="Canonical filename")
    filename_patterns: List[str] = Field(
        description="List of filename patterns extracted"
    )
    filename_rules: Optional[List[str]] = Field(
        description="List of rules applied to filenames", default=[]
    )
    entity_counts: Optional[Dict[str, int]] = Field(
        description="Mapping of entity to count", default={}
    )
    entity_counts_percentage: Optional[Dict[str, float]] = Field(
        description="Mapping of entity to percentage", default={}
    )


class FilenameSectionWrapper(BaseModel):
    split_sections: FilenameSectionOutput
