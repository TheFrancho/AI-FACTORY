import json
import os
from pathlib import Path

import asyncio
from google.adk.agents import Agent
from google.adk.models.lite_llm import LiteLlm
from google.adk.sessions import InMemorySessionService
from google.adk.runners import Runner
from google.genai import types


from ai_factory.agents.cv_extracter.extract_sections.schemas import SplitSectionsWrapper
from ai_factory.agents.cv_extracter.extract_sections.prompts import (
    model_instruction,
    model_description,
)

from ai_factory.utils import get_file_list
from ai_factory.config import config

target_model = config.default_model


cv_text_splitter_agent = Agent(
    model=LiteLlm(model=target_model),
    name="cv_text_splitter_agent",
    instruction=model_instruction,
    description=model_description,
    output_schema=SplitSectionsWrapper,
    output_key="split_sections",
)


# Individual excecution
async def main():
    app_name = "ai-factory"
    user_id = "thefrancho"
    session_service = InMemorySessionService()

    folder_path = Path("dataset_files/datasource_cvs")
    files_path = get_file_list(folder_path)
    files_path = [file for file in files_path if not os.path.isdir()]

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

        output_base_path = "custom_outputs"
        os.makedirs(output_base_path, exist_ok=True)
        output_path = os.path.join(
            output_base_path, f"{os.path.basename(file_path)}.json"
        )

        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(sections, file, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    asyncio.run(main())
