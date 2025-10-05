import json
import os
from pathlib import Path

import asyncio
from google.adk.agents import Agent
from google.adk.models.lite_llm import LiteLlm
from google.adk.sessions import InMemorySessionService
from google.adk.runners import Runner
from google.genai import types

from ai_factory.agents.cv_extracter.extract_filename_pattern.schemas import (
    FilenameSectionOutput,
)
from ai_factory.agents.cv_extracter.extract_filename_pattern.prompts import (
    model_instruction,
    model_description,
)
from ai_factory.agents.cv_extracter.extract_filename_pattern.tools import (
    convert_to_percentage,
)


from ai_factory.utils import get_file_list
from ai_factory.config import config

target_model = config.default_model


cv_filename_pattern_agent = Agent(
    model=LiteLlm(model=target_model),
    name="filename_pattern_section_processer",
    instruction=model_instruction,
    description=model_description,
    output_schema=FilenameSectionOutput,
    output_key="filename_pattern_section",
    tools=[convert_to_percentage],
)


async def main():
    app_name = "ai-factory"
    user_id = "thefrancho"
    session_service = InMemorySessionService()

    folder_path = Path("custom_outputs")
    files_path = get_file_list(folder_path)
    files_path = [file for file in files_path if not os.path.isdir(file)]

    for file_path in files_path:
        session = await session_service.create_session(
            app_name=app_name, user_id=user_id
        )
        runner = Runner(
            agent=cv_filename_pattern_agent,
            app_name=app_name,
            session_service=session_service,
        )

        print(f"Working with file {file_path} - {os.path.basename(file_path)}")

        with open(file_path, "r") as file:
            file_content = json.load(file)["filename_pattern_section"]

        new_message = types.Content(role="user", parts=[types.Part(text=file_content)])

        async for event in runner.run_async(
            user_id=user_id, session_id=session.id, new_message=new_message
        ):
            pass

        refreshed_state = await session_service.get_session(
            app_name=app_name, user_id=user_id, session_id=session.id
        )
        title_section = refreshed_state.state["filename_pattern_section"]
        print(title_section)

        output_base_path = "custom_outputs/filename_pattern_section"
        os.makedirs(output_base_path, exist_ok=True)

        output_path = os.path.join(
            output_base_path, f"{os.path.basename(file_path)}.json"
        )
        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(title_section, file, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    asyncio.run(main())
