import os
from pathlib import Path

import asyncio
from google.adk.agents import Agent
from google.adk.models.lite_llm import LiteLlm
from google.adk.sessions import InMemorySessionService

from ai_factory.agents.cv_extracter.extract_day_of_week_pattern.schemas import (
    DayOfWeekPatternOutput,
)
from ai_factory.agents.cv_extracter.extract_day_of_week_pattern.prompts import (
    model_instruction,
    model_description,
)
from ai_factory.agents.cv_extracter.utils import process_file


from ai_factory.utils import get_file_list
from ai_factory.config import config

target_model = config.default_model
model_name = "day_of_week_pattern_processer"
output_key = "day_of_week_section_pattern"


cv_day_of_week_pattern_agent = Agent(
    model=LiteLlm(model=target_model),
    name=model_name,
    instruction=model_instruction,
    description=model_description,
    output_schema=DayOfWeekPatternOutput,
    output_key=output_key,
)


async def main():
    OUTPUT_DIR = "custom_outputs/day_of_week_section_pattern"
    CONCURRENCY = 20
    file_section = "day_of_week_section_pattern"

    app_name = "ai-factory"
    user_id = "thefrancho"
    session_service = InMemorySessionService()

    folder_path = Path("custom_outputs")
    files_path = [p for p in get_file_list(folder_path) if not os.path.isdir(p)]

    sem = asyncio.Semaphore(CONCURRENCY)
    tasks = [
        asyncio.create_task(
            process_file(
                output_dir=OUTPUT_DIR,
                file_section=file_section,
                output_key=output_key,
                agent=cv_day_of_week_pattern_agent,
                session_service=session_service,
                app_name=app_name,
                user_id=user_id,
                file_path=fp,
                sem=sem,
            )
        )
        for fp in files_path
    ]

    for coro in asyncio.as_completed(tasks):
        try:
            await coro
        except Exception as e:
            print(f"Task failed: {e!r}")


if __name__ == "__main__":
    asyncio.run(main())
