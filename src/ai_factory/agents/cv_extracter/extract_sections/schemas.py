from pydantic import BaseModel, Field


class SplitSectionsOutput(BaseModel):
    markdown_title: str = Field(description="Introduction section")
    filename_pattern_section: str = Field(description="Filename pattern section")
    file_processing_pattern_section: str = Field(
        description="File processing pattern section"
    )
    volume_characteristics_section: str = Field(
        description="Volume characteristics section"
    )
    day_of_week_section_pattern: str = Field(description="Day of week pattern section")
    recurring_patterns_section: str = Field(description="Recurring patterns section")
    comments_for_analyst_section: str = Field(
        description="Comments for analyst section"
    )


class SplitSectionsWrapper(BaseModel):
    split_sections: SplitSectionsOutput
