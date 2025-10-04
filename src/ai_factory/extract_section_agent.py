import json
import os
from pathlib import Path

import asyncio
from pydantic import BaseModel, Field
from google.adk.agents import Agent
from google.adk.models.lite_llm import LiteLlm
from google.adk.sessions import InMemorySessionService
from google.adk.runners import Runner
from google.genai import types


def get_file_list(folder_path: Path):
    files_list = os.listdir(folder_path)
    files_path = sorted(
        [
            os.path.join(folder_path, file_path)
            for file_path in files_list
            if file_path.lower().endswith(".md")
        ]
    )
    return files_path


target_model = "gpt-5-nano"


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


model_instruction = """
You are a sppliter agent, you will receive a text in markdown format and your target is to extract all sections
Sections are sequential
The general schema will be
- The markdown title (this is a CV for a file format), extract its content including metadata and file collection range until the start of introduction (if it's there, otherwise until the point 1), This section is the start of the text
- The introduction (if it available), ignore it
- The content, it will hint you the structure to extract from the file (don't save it)
- Section 1: Filename patterns: Save this section in the corresponding scheme key, save the text as it is
- Section 2: schedule and processing patterns: Save this section in the corresponding scheme key, save the text as it is
- Section 3: Day of week summary: Save this section in the corresponding scheme key, save the text as it is
- Section 4: Entity statistics by day: Save this section in the corresponding scheme key, save the text as it is
- Section 5: Recurring patterns and data flow characteristics: Save this section in the corresponding scheme key, save the text as it is
- Section 6: Key insight and recommendations: Save this section in the corresponding scheme key, save the text as it is

Only save the section from start to end (including the title one until the end of metadata)
Only return the output schema and allow it to save, don't follow up the conversation
"""

model_description = """
Splits the CV file into sections
"""

cv_text_splitter_agent = Agent(
    model=LiteLlm(model=target_model),
    name="cv_text_splitter_agent",
    instruction=model_instruction,
    description=model_description,
    output_schema=SplitSectionsWrapper,
    output_key="split_sections",
)


async def main():
    app_name = "ai-factory"
    user_id = "thefrancho"
    session_service = InMemorySessionService()

    folder_path = Path("dataset_files/datasource_cvs")
    files_path = get_file_list(folder_path)

    for file_path in files_path:
        session = await session_service.create_session(
            app_name=app_name, user_id=user_id
        )
        runner = Runner(
            agent=cv_text_splitter_agent,
            app_name=app_name,
            session_service=session_service,
        )

        print(f"Working with file {file_path} - {os.path.basename(file_path)}")
        with open(file_path, "r", encoding="utf-8") as f:
            md = f.read()

        new_message = types.Content(role="user", parts=[types.Part(text=md)])

        async for event in runner.run_async(
            user_id=user_id, session_id=session.id, new_message=new_message
        ):
            pass

        refreshed_state = await session_service.get_session(
            app_name=app_name, user_id=user_id, session_id=session.id
        )
        sections = refreshed_state.state["split_sections"]

        output_path = f"custom_outputs/{os.path.basename(file_path)}.json"
        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(sections, file, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    asyncio.run(main())
