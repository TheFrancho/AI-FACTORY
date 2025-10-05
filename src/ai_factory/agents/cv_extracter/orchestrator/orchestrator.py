import os
from pathlib import Path
import asyncio

from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

from ai_factory.utils import get_file_list
from ai_factory.agents.cv_extracter.orchestrator.plan import build_overall_workflow


async def run_over_folder(folder: Path):
    app_name = "ai-factory"
    user_id = "thefrancho"
    session_service = InMemorySessionService()

    overall_workflow = build_overall_workflow()

    files = [p for p in get_file_list(folder) if not os.path.isdir(p)]
    for file_path in files:
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

        print(f"\n=== {os.path.basename(file_path)} ===")
        print(state["split_sections"]["markdown_title_section"])
        print("-" * 60)
        print(state["split_sections"]["filename_pattern_section"])
        print("-" * 60)
        print(state["title_section"])  # output of cv_title_pattern_agent
        print("-" * 60)
        print(state["filename_pattern_section"])  # output of cv_filename_pattern_agent
        print("-" * 60)
        input("Hold it there my kid")


folder = "dataset_files/datasource_cvs"
asyncio.run(run_over_folder(folder))
