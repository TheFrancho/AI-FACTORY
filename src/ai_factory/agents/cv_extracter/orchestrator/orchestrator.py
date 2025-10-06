import json
import os
from pathlib import Path
import asyncio

from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

from ai_factory.utils import get_file_list
from ai_factory.agents.cv_extracter.orchestrator.plan import build_overall_workflow


async def run_over_folder(file_path: str, sem: asyncio.Semaphore):
    async with sem:
        app_name = "ai-factory"
        user_id = "thefrancho"
        session_service = InMemorySessionService()

        overall_workflow = build_overall_workflow()

        session = await session_service.create_session(
            app_name=app_name, user_id=user_id
        )

        with open(file_path, "r", encoding="utf-8") as f:
            md = f.read()

        runner = Runner(
            agent=overall_workflow, app_name=app_name, session_service=session_service
        )
        new_message = types.Content(role="user", parts=[types.Part(text=md)])

        # Run splitter -> (title, filename) in parallel
        async for _ in runner.run_async(
            user_id=user_id, session_id=session.id, new_message=new_message
        ):
            pass

        # Inspect state (all keys written by agents)
        state = (
            await session_service.get_session(
                app_name=app_name, user_id=user_id, session_id=session.id
            )
        ).state

        full_extraction_sections = {
            "title_section": state["title_section"],
            "filename_pattern_section": state["filename_pattern_section"],
            "file_processing_pattern_section": state["file_processing_pattern_section"],
            "volume_characteristics_section": state["volume_characteristics_section"],
            "day_of_week_section_pattern": state["day_of_week_section_pattern"],
            "recurring_patterns_section": state["recurring_patterns_section"],
            "comments_for_analyst_section": state["comments_for_analyst_section"],
        }

        output_dir = "custom_outputs/complete_sections"

        os.makedirs(output_dir, exist_ok=True)
        out_path = os.path.join(output_dir, f"{os.path.basename(file_path)}.json")
        with open(out_path, "w", encoding="utf-8") as fp:
            json.dump(full_extraction_sections, fp, ensure_ascii=False, indent=4)


async def main():
    CONCURRENCY = 3
    folder = "dataset_files/datasource_cvs"
    sem = asyncio.Semaphore(CONCURRENCY)

    folder_path = Path(folder)
    files_path = [p for p in get_file_list(folder_path) if not os.path.isdir(p)]

    tasks = [
        asyncio.create_task(run_over_folder(file_path=fp, sem=sem)) for fp in files_path
    ]

    for coro in asyncio.as_completed(tasks):
        try:
            await coro
        except Exception as e:
            print(f"Task failed: {e!r}")


if __name__ == "__main__":
    asyncio.run(main())
