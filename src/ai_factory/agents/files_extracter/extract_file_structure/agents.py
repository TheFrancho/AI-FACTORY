import json
import os
from typing import List, Dict, Any

import asyncio
from google.adk.agents import Agent
from google.adk.models.lite_llm import LiteLlm
from google.adk.sessions import InMemorySessionService
from google.adk.runners import Runner
from google.genai import types

from ai_factory.agents.files_extracter.extract_file_structure.schemas import (
    InferredBatchOutput,
)
from ai_factory.agents.files_extracter.extract_file_structure.prompts import (
    model_instruction,
    model_description,
)

from ai_factory.config import config

target_model = config.default_model
model_name = "file_formatter_agent"
output_key = "file_formatted"
CONCURRENCY = 1


def make_extract_file_structure_agent() -> Agent:
    return Agent(
        model=LiteLlm(model=target_model),
        name=model_name,
        instruction=model_instruction,
        description=model_description,
        output_schema=InferredBatchOutput,
        output_key=output_key,
    )


extract_file_structure_agent = make_extract_file_structure_agent()


def _normalize_status(s):
    if not s:
        return None
    s = str(s).strip().lower()
    m = {
        "processed": "processed",
        "success": "processed",
        "ok": "processed",
        "failed": "failed",
        "error": "failed",
        "empty": "empty",
        "unknown": "unknown",
    }
    return m.get(s, None)


def _infer_ext(fn: str):
    if not fn:
        return None
    _, ext = os.path.splitext(fn)
    return ext[1:].lower() if ext else None


def chunk(lst: List[Any], n: int):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def run_single_batch(
    *,
    app_name: str,
    user_id: str,
    session_service: InMemorySessionService,
    agent: Agent,
    datasource_id: str,
    filename_pattern_section: Dict[str, Any],
    files_batch: List[Dict[str, Any]],
    output_key: str,
    enforce_stateless: bool = True,
) -> Dict[str, Any]:
    svc = InMemorySessionService() if enforce_stateless else session_service

    session = await svc.create_session(app_name=app_name, user_id=user_id)
    runner = Runner(agent=agent, app_name=app_name, session_service=svc)

    rules_obj = filename_pattern_section.get(
        "filename_pattern_section", filename_pattern_section
    )

    slim_files = [
        {"filename": f.get("filename"), "status": f.get("status")} for f in files_batch
    ]

    input_json = {
        "datasource_id": datasource_id,
        "context": {"filename_pattern_section": rules_obj},
        "files": slim_files,
    }

    new_message = types.Content(
        role="user", parts=[types.Part(text=json.dumps(input_json))]
    )

    async for _ in runner.run_async(
        user_id=user_id, session_id=session.id, new_message=new_message
    ):
        pass

    refreshed = await svc.get_session(
        app_name=app_name, user_id=user_id, session_id=session.id
    )
    result = refreshed.state.get(output_key)

    # If ADK returns a Pydantic model instance, convert to dict
    if hasattr(result, "model_dump"):
        result = result.model_dump()

    # Expect {"inferred_batch": [...]}
    if isinstance(result, dict) and "inferred_batch" in result:
        return result

    return {"inferred_batch": []}


async def process_file_batched(
    output_key: str,
    agent: Agent,
    session_service: InMemorySessionService,
    app_name: str,
    user_id: str,
    datasource_id: str,
    filename_pattern_section: Dict[str, Any],
    files_all: List[Dict[str, Any]],
    batch_size: int = 40,
    batch_concurrency: int = 4,
) -> Dict[str, Any]:
    total = len(files_all)
    batches = [
        (start, files_all[start : start + batch_size])
        for start in range(0, total, batch_size)
    ]

    sem_batches = asyncio.Semaphore(batch_concurrency)

    async def run_one(start_idx: int, files_batch: List[Dict[str, Any]]):
        async with sem_batches:
            inferred = await run_single_batch(
                app_name=app_name,
                user_id=user_id,
                session_service=session_service,
                agent=agent,
                datasource_id=datasource_id,
                filename_pattern_section=filename_pattern_section,
                files_batch=files_batch,
                output_key=output_key,
                enforce_stateless=True,
            )
            items = inferred.get("inferred_batch", [])
            if len(items) != len(files_batch):
                print(
                    f"[WARN] Batch {start_idx}: expected {len(files_batch)} items, got {len(items)}"
                )
            else:
                print(f"[INFO] Batch {start_idx}: {len(items)} items")
            return (start_idx, items)

    tasks = [asyncio.create_task(run_one(start, batch)) for (start, batch) in batches]

    merged_items: List[Dict[str, Any]] = []
    done_count = 0
    results: List[tuple[int, List[Dict[str, Any]]]] = []

    for coro in asyncio.as_completed(tasks):
        start_idx, items = await coro
        results.append((start_idx, items))
        done_count += len(items)
        print(f"[INFO] Progress: {done_count}/{total}")

    # deterministic order
    results.sort(key=lambda t: t[0])
    for _, items in results:
        merged_items.extend(items)

    return {"inferred_batch": merged_items}


async def process_one_cv(
    cv_to_check: str,
    dataset_day_filepath: str,
    output_dir: str,
    output_key: str,
    agent: Agent,
    session_service: InMemorySessionService,
    app_name: str,
    user_id: str,
    sem: asyncio.Semaphore,
    batch_size: int = 20,
):
    async with sem:
        # Load files mapped by CV id
        with open(dataset_day_filepath, "r", encoding="utf-8") as f:
            dataset_day_json = json.load(f)[cv_to_check]  # List[files]

        cv_extracted_json_filepath = (
            f"custom_outputs/complete_sections/{cv_to_check}_native.md.json"
        )
        with open(cv_extracted_json_filepath, "r", encoding="utf-8") as f:
            cv_json_extracted = json.load(f)

        # Accept either wrapped or flat structure
        filename_pattern_json = cv_json_extracted.get(
            "filename_pattern_section", cv_json_extracted
        )

        result = await process_file_batched(
            output_key=output_key,
            agent=agent,
            session_service=session_service,
            app_name=app_name,
            user_id=user_id,
            datasource_id=cv_to_check,
            filename_pattern_section=filename_pattern_json,
            files_all=dataset_day_json,
            batch_size=batch_size,
            batch_concurrency=4,
        )

        inferred_items = result.get("inferred_batch", [])
        originals = dataset_day_json

        if len(inferred_items) > len(originals):
            inferred_items = inferred_items[: len(originals)]
        elif len(inferred_items) < len(originals):
            inferred_items += [{} for _ in range(len(originals) - len(inferred_items))]

        full_items = []
        for src, inf in zip(originals, inferred_items):
            merged = {
                # originals (pass-through)
                "filename": src.get("filename"),
                "rows": src.get("rows", None),
                "status": _normalize_status(src.get("status")),
                "is_duplicated": src.get("is_duplicated", None),
                "file_size": src.get("file_size", None),
                "uploaded_at": src.get("uploaded_at", None),
                "status_message": src.get("status_message", None),
                # inferred
                "cleaned_filename": inf.get("cleaned_filename") or src.get("filename"),
                "batch": inf.get("batch"),
                "entity": inf.get("entity"),
                "covered_date": inf.get("covered_date"),
                "extension": inf.get("extension") or _infer_ext(src.get("filename")),
            }
            full_items.append(merged)

        os.makedirs(output_dir, exist_ok=True)

        out_full = os.path.join(output_dir, f"{cv_to_check}_files.json")
        with open(out_full, "w", encoding="utf-8") as fp:
            json.dump({"inferred_batch": full_items}, fp, ensure_ascii=False, indent=2)

        print(f"Done: {out_full}")


async def main():
    app_name = "ai-factory"
    user_id = "thefrancho"
    session_service = InMemorySessionService()
    day_targets = [
        "2025-09-08_20_00_UTC",
        "2025-09-09_20_00_UTC",
        "2025-09-10_20_00_UTC",
        "2025-09-11_20_00_UTC",
        "2025-09-12_20_00_UTC",
    ]

    for day_target in day_targets:
        dataset_day_filepath = f"dataset_files/{day_target}/files_last_weekday.json"
        OUTPUT_DIR = f"files_outputs/{day_target}/files_structure/last_weekday_files"
        # Hardcoded for testing purpose
        cv_list = [
            "195385",
            "195436",
            "195439",
            "196125",
            "199944",
            "207936",
            "207938",
            "209773",
            "211544",
            "220504",
            "220505",
            "220506",
            "224602",
            "224603",
            "228036",
            "228038",
            "239611",
            "239613",
        ]

        agent = make_extract_file_structure_agent()

        sem = asyncio.Semaphore(CONCURRENCY)

        tasks = [
            asyncio.create_task(
                process_one_cv(
                    cv_to_check=cv_id,
                    dataset_day_filepath=dataset_day_filepath,
                    output_dir=OUTPUT_DIR,
                    output_key=output_key,
                    agent=agent,
                    session_service=session_service,
                    app_name=app_name,
                    user_id=user_id,
                    sem=sem,
                    batch_size=20,
                )
            )
            for cv_id in cv_list
        ]

        for coro in asyncio.as_completed(tasks):
            try:
                await coro
            except Exception as e:
                print(f"Task failed: {e!r}")


if __name__ == "__main__":
    asyncio.run(main())
