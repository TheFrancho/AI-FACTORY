import json
import os

import asyncio
from google.adk.agents import Agent
from google.adk.sessions import InMemorySessionService
from google.adk.runners import Runner
from google.genai import types

CONCURRENCY = 20


async def process_file(
    output_dir: str,
    file_section: str,
    output_key: str,
    agent: Agent,
    session_service: InMemorySessionService,
    app_name: str,
    user_id: str,
    file_path: str,
    sem: asyncio.Semaphore,
):
    async with sem:
        # fresh session
        session = await session_service.create_session(
            app_name=app_name, user_id=user_id
        )
        runner = Runner(agent=agent, app_name=app_name, session_service=session_service)

        print(f"Working with file {file_path} - {os.path.basename(file_path)}")

        if file_section:
            with open(file_path, "r", encoding="utf-8") as f:
                file_content = json.load(f)[file_section]
        else:  # case of full text read and not json read
            with open(file_path, "r", encoding="utf-8") as f:
                file_content = f.read()

        new_message = types.Content(role="user", parts=[types.Part(text=file_content)])

        # run until completion
        async for _ in runner.run_async(
            user_id=user_id, session_id=session.id, new_message=new_message
        ):
            pass

        refreshed_state = await session_service.get_session(
            app_name=app_name, user_id=user_id, session_id=session.id
        )
        filename_section = refreshed_state.state[output_key]

        os.makedirs(output_dir, exist_ok=True)
        out_path = os.path.join(output_dir, f"{os.path.basename(file_path)}.json")
        with open(out_path, "w", encoding="utf-8") as fp:
            json.dump(filename_section, fp, ensure_ascii=False, indent=4)

        print(f"Done: {out_path}")
