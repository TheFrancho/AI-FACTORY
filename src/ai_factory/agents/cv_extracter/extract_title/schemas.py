from pydantic import BaseModel, Field


class TitleSectionOutput(BaseModel):
    resource_id: str = Field(description="CV resource ID")
    workspace_id: str = Field(description="CV workspace ID")
    datasource_cv_name: str = Field(description="CV datasource name")


class TitleSectionWrapper(BaseModel):
    title_section: TitleSectionOutput
