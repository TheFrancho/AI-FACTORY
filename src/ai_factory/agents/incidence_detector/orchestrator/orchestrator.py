import argparse
import asyncio
import csv
import glob
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from google.adk.agents import Agent
from google.adk.models.lite_llm import LiteLlm
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

from ai_factory.agents.incidence_detector.extract_file_structure.schemas import (
    InferredBatchOutput,
)
from ai_factory.agents.incidence_detector.extract_file_structure.prompts import (
    model_instruction,
    model_description,
)

from ai_factory.agents.incidence_detector.detect_remove_duplicates.agents import (
    dedupe_records,
    compute_dedupe_and_status_anomalies,
)

from ai_factory.agents.incidence_detector.detect_unexpected_empty.agents import (
    UnexpectedEmptyDetectorAgent,
)
from ai_factory.agents.incidence_detector.detect_unexpected_volume.agents import (
    UnexpectedVolumeVariationAgent,
)
from ai_factory.agents.incidence_detector.detect_after_schedule.agents import (
    UploadAfterScheduleDetectorAgent,
)
from ai_factory.agents.incidence_detector.detect_missing_file.agents import (
    MissingFileDetectorSimple,
)

# === Config ===
from ai_factory.config import config

TARGET_MODEL = config.default_model
MODEL_NAME = "file_formatter_agent"
OUTPUT_KEY = "file_formatted"

BATCH_SIZE = 20
BATCH_CONCURRENCY = 4
CV_CONCURRENCY = 4

CUSTOM_OUTPUTS_DIR = "custom_outputs/complete_sections"


def _normalize_status(s: Any) -> Optional[str]:
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


def _infer_ext(fn: Optional[str]) -> Optional[str]:
    if not fn:
        return None
    _, ext = os.path.splitext(fn)
    return ext[1:].lower() if ext else None


def _write_json(path: str, data: Any):
    os.makedirs(os.path.dirname(path), exist_ok=True) if os.path.dirname(path) else None
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _suggest_action(inc: Dict[str, Any]) -> str:
    """
    Generate a concrete, human-actionable remediation for an incident.
    """
    t = str(inc.get("incident_type") or "").lower()
    reason = (inc.get("incident_reason") or "").lower()

    if t == "duplicate":
        return (
            "Keep the canonical file and ignore/delete the duplicates. "
            "Enable upstream deduplication and reprocess the canonical file if needed."
        )

    if t == "status_failure":
        if "failed" in reason:
            return (
                "Inspect ingestion/transformation logs for this file, fix the error, "
                "and re-run the pipeline for the file."
            )
        if "empty" in reason:
            return (
                "Validate with the data owner whether empty output was expected. "
                "If not, request a resend/backfill and re-run the pipeline."
            )
        return (
            "Verify the upstream job completed successfully; if not, re-run. "
            "If the behavior is expected, document it in the CV."
        )

    if t == "unexpected_empty":
        ent = inc.get("entity") or "this entity"
        wd = inc.get("weekday_utc") or "this weekday"
        return (
            f"Check upstream for {ent} on {wd}. If data exists, request a backfill and re-run; "
            "if the zero is expected, update the CV expectations."
        )

    if t == "volume_anomaly":
        return (
            "Compare the file’s row count against source-of-truth. "
            "Check for schema/filter changes or partial loads. "
            "If the change is legitimate, update CV baselines; otherwise, fix and reprocess."
        )

    if t == "upload_after_schedule":
        return (
            "Review uploader schedule and upstream delays. "
            "If this was a backfill, annotate the run; otherwise remediate the delay "
            "and ensure on-time future uploads."
        )

    if t == "missing_files":
        ent = inc.get("entity") or "the entity"
        return (
            f"Confirm {ent} was scheduled to arrive today. "
            "Check upstream job status and connectors; request resend/backfill and re-run. "
            "Update the CV if the schedule changed."
        )

    if t == "missing_source":
        return (
            "Confirm the source was expected today. "
            "Check integrations and job triggers; request resend/backfill and re-run. "
            "Update the CV if the weekday expectation changed."
        )

    # Default
    return (
        "Investigate upstream pipeline, validate expectations with the data owner, "
        "remediate the issue, and update CV rules if the behavior is expected."
    )


def _write_anomalies_csv(anoms: List[Dict[str, Any]], out_path: str):
    """
    Create a compact CSV: cleaned_filename, reason, action
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["cleaned_filename", "reason", "action"])
        for inc in anoms:
            cleaned = inc.get("cleaned_filename") or inc.get("filename") or ""
            reason = inc.get("incident_reason") or ""
            action = _suggest_action(inc)
            w.writerow([cleaned, reason, action])


def _exec_date_from_day_target(day_target: str) -> str:
    """
    day_target is like '2025-09-08_20_00_UTC' -> return '2025-09-08'
    """
    m = re.match(r"(\d{4}-\d{2}-\d{2})", day_target)
    return m.group(1) if m else day_target[:10]


# ---------- extract_file_structure ----------
def make_extract_file_structure_agent() -> Agent:
    return Agent(
        model=LiteLlm(model=TARGET_MODEL),
        name=MODEL_NAME,
        instruction=model_instruction,
        description=model_description,
        output_schema=InferredBatchOutput,
        output_key=OUTPUT_KEY,
    )


async def _run_single_batch(
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
    if hasattr(result, "model_dump"):
        result = result.model_dump()
    if isinstance(result, dict) and "inferred_batch" in result:
        return result
    return {"inferred_batch": []}


async def _process_file_batched(
    *,
    output_key: str,
    agent: Agent,
    session_service: InMemorySessionService,
    app_name: str,
    user_id: str,
    datasource_id: str,
    filename_pattern_section: Dict[str, Any],
    files_all: List[Dict[str, Any]],
    batch_size: int = BATCH_SIZE,
    batch_concurrency: int = BATCH_CONCURRENCY,
) -> Dict[str, Any]:
    total = len(files_all)
    batches = [
        (start, files_all[start : start + batch_size])
        for start in range(0, total, batch_size)
    ]
    sem_batches = asyncio.Semaphore(batch_concurrency)

    async def run_one(start_idx: int, files_batch: List[Dict[str, Any]]):
        async with sem_batches:
            inferred = await _run_single_batch(
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
    results: List[tuple[int, List[Dict[str, Any]]]] = []
    merged: List[Dict[str, Any]] = []

    for coro in asyncio.as_completed(tasks):
        start_idx, items = await coro
        results.append((start_idx, items))

    results.sort(key=lambda t: t[0])
    for _, items in results:
        merged.extend(items)

    return {"inferred_batch": merged}


async def _extract_one_cv(
    *,
    cv_id: str,
    files_for_cv: List[Dict[str, Any]],
    app_name: str,
    user_id: str,
    agent: Agent,
    session_service: InMemorySessionService,
) -> List[Dict[str, Any]]:
    cv_rules_path = os.path.join(CUSTOM_OUTPUTS_DIR, f"{cv_id}_native.md.json")
    if not os.path.exists(cv_rules_path):
        print(f"[WARN] Missing CV rules: {cv_rules_path} — passthrough for CV {cv_id}")
        passthrough = []
        for src in files_for_cv:
            passthrough.append(
                {
                    "filename": src.get("filename"),
                    "rows": src.get("rows", None),
                    "status": _normalize_status(src.get("status")),
                    "is_duplicated": src.get("is_duplicated", None),
                    "file_size": src.get("file_size", None),
                    "uploaded_at": src.get("uploaded_at", None),
                    "status_message": src.get("status_message", None),
                    "cleaned_filename": src.get("filename"),
                    "batch": None,
                    "entity": None,
                    "covered_date": None,
                    "extension": _infer_ext(src.get("filename")),
                }
            )
        return passthrough

    cv_json_extracted = _load_json(cv_rules_path)
    filename_pattern_json = cv_json_extracted.get(
        "filename_pattern_section", cv_json_extracted
    )

    result = await _process_file_batched(
        output_key=OUTPUT_KEY,
        agent=agent,
        session_service=session_service,
        app_name=app_name,
        user_id=user_id,
        datasource_id=cv_id,
        filename_pattern_section=filename_pattern_json,
        files_all=files_for_cv,
        batch_size=BATCH_SIZE,
        batch_concurrency=BATCH_CONCURRENCY,
    )

    inferred_items = list(result.get("inferred_batch", []))
    originals = files_for_cv

    if len(inferred_items) > len(originals):
        inferred_items = inferred_items[: len(originals)]
    elif len(inferred_items) < len(originals):
        inferred_items += [{} for _ in range(len(originals) - len(inferred_items))]

    full_items: List[Dict[str, Any]] = []
    for src, inf in zip(originals, inferred_items):
        merged = {
            "filename": src.get("filename"),
            "rows": src.get("rows", None),
            "status": _normalize_status(src.get("status")),
            "is_duplicated": src.get("is_duplicated", None),
            "file_size": src.get("file_size", None),
            "uploaded_at": src.get("uploaded_at", None),
            "status_message": src.get("status_message", None),
            "cleaned_filename": inf.get("cleaned_filename") or src.get("filename"),
            "batch": inf.get("batch"),
            "entity": inf.get("entity"),
            "covered_date": inf.get("covered_date"),
            "extension": inf.get("extension") or _infer_ext(src.get("filename")),
        }
        full_items.append(merged)

    return full_items


# ---------- dataset processing ----------
async def _process_dataset(
    *,
    label: str,  # "today_files" | "last_weekday_files"
    day_target: str,
    files_map_path: str,
    app_name: str,
    user_id: str,
    compute_anomalies: bool,  # True only for today_files
) -> Dict[str, Any]:
    out_base_struct = f"files_outputs/{day_target}/files_structure/{label}"
    out_base_clean = f"files_outputs/{day_target}/files_cleaned/{label}"
    out_base_anoms = f"files_outputs/{day_target}/anomalies/{label}"

    files_map = _load_json(files_map_path)
    if not isinstance(files_map, dict):
        raise ValueError(f"{files_map_path} must be a dict of CV -> list[records]")

    session_service = InMemorySessionService()
    agent = make_extract_file_structure_agent()

    sem = asyncio.Semaphore(CV_CONCURRENCY)

    async def per_cv(cv_id: str, files_for_cv: List[Dict[str, Any]]):
        async with sem:
            merged_items = await _extract_one_cv(
                cv_id=cv_id,
                files_for_cv=files_for_cv,
                app_name=app_name,
                user_id=user_id,
                agent=agent,
                session_service=session_service,
            )
            # write structure
            cv_struct_path = os.path.join(out_base_struct, f"{cv_id}_files.json")
            _write_json(cv_struct_path, {"inferred_batch": merged_items})

            # dedupe
            dedup = dedupe_records(merged_items)
            stats = dedup["stats"]

            stem = f"{cv_id}_files"
            _write_json(
                os.path.join(out_base_clean, f"{stem}_cleaned.json"),
                {"inferred_batch": dedup["final"]},
            )
            _write_json(
                os.path.join(out_base_clean, f"{stem}_removed.json"),
                {"inferred_batch": dedup["removed"]},
            )
            _write_json(
                os.path.join(out_base_clean, f"{stem}_harmless.json"),
                {"inferred_batch": dedup["harmless"]},
            )

            anomalies_count = 0
            if compute_anomalies:
                anomalies, _ok = compute_dedupe_and_status_anomalies(
                    merged_items, dedup
                )
                cv_anom_path = os.path.join(
                    out_base_anoms, f"{cv_id}_dup_fail_anomalies.json"
                )
                _write_json(cv_anom_path, anomalies)
                anomalies_count = len(anomalies)

            return {
                "cv_id": cv_id,
                "stats": stats,
                "anomalies_count": anomalies_count,
            }

    tasks = [per_cv(cv_id, files_for_cv) for cv_id, files_for_cv in files_map.items()]
    results = []
    for coro in asyncio.as_completed(tasks):
        try:
            results.append(await coro)
        except Exception as e:
            print(f"[ERROR] CV job failed: {e!r}")

    # Aggregate dedupe/status anomalies for this label (if enabled)
    all_anoms: List[Dict[str, Any]] = []
    anomalies_aggregate_path = None
    if compute_anomalies:
        os.makedirs(out_base_anoms, exist_ok=True)
        for cv_id in files_map.keys():
            p = os.path.join(out_base_anoms, f"{cv_id}_dup_fail_anomalies.json")
            if os.path.exists(p):
                all_anoms.extend(_load_json(p))
        agg_path = os.path.join(out_base_anoms, "_ALL_anomalies.json")
        _write_json(agg_path, all_anoms)
        anomalies_aggregate_path = agg_path

    per_cv_index = {r["cv_id"]: r for r in results if r}
    return {
        "per_cv": per_cv_index,
        "anomalies_aggregate_path": anomalies_aggregate_path,
        "anomalies_count": len(all_anoms) if compute_anomalies else 0,
        "out_base_anoms": out_base_anoms,
        "out_base_clean": out_base_clean,
    }


# ---------- run 4 detectors in parallel for TODAY ----------
async def _run_detectors_for_path(
    cleaned_json_path: str,
    out_base_anoms_today: str,
    exec_date_iso: str,
    base_clean_dir: str,
) -> list[dict]:
    """
    Run:
      - UnexpectedEmptyDetectorAgent
      - UnexpectedVolumeVariationAgent
      - UploadAfterScheduleDetectorAgent
      - MissingFileDetectorSimple  (needs payload incl. last_weekday cleaned path + CV path + exec_date)
    """
    rid = Path(cleaned_json_path).stem.split("_")[0]
    cv_path = f"{CUSTOM_OUTPUTS_DIR}/{rid}_native.md.json"

    last_weekday_cleaned_path = os.path.join(
        base_clean_dir, "last_weekday_files", f"{rid}_files_cleaned.json"
    )
    if not os.path.exists(last_weekday_cleaned_path):
        last_weekday_cleaned_path = None

    # instantiate agents
    empty_agent = UnexpectedEmptyDetectorAgent()
    vol_agent = UnexpectedVolumeVariationAgent()
    schedule_agent = UploadAfterScheduleDetectorAgent()
    missing_agent = MissingFileDetectorSimple()

    # build the missing-file payload
    missing_payload = {
        "today_path": cleaned_json_path,
        "cv_path": cv_path,
        "last_weekday_path": last_weekday_cleaned_path,
        "exec_date": exec_date_iso,
    }

    # run them concurrently
    t_empty = empty_agent.run(cleaned_json_path)
    t_vol = vol_agent.run(cleaned_json_path)
    t_sched = schedule_agent.run(cleaned_json_path)
    t_missing = missing_agent.run(missing_payload)

    empty_res, vol_res, sched_res, miss_res = await asyncio.gather(
        t_empty, t_vol, t_sched, t_missing
    )

    # collect and write anomalies (only anomalies, not oks)
    empty_anoms = empty_res.get("anomalies", [])
    vol_anoms = vol_res.get("anomalies", [])
    sched_anoms = sched_res.get("anomalies", [])
    miss_anoms = (miss_res.get("anomalies") or {}).get("inferred_batch", [])

    _write_json(
        os.path.join(out_base_anoms_today, f"{rid}_unexpected_empty_anomalies.json"),
        empty_anoms,
    )
    _write_json(
        os.path.join(out_base_anoms_today, f"{rid}_volume_anomalies.json"), vol_anoms
    )
    _write_json(
        os.path.join(out_base_anoms_today, f"{rid}_schedule_anomalies.json"),
        sched_anoms,
    )
    _write_json(
        os.path.join(out_base_anoms_today, f"{rid}_missing_anomalies.json"), miss_anoms
    )

    merged: List[dict] = []
    merged.extend(empty_anoms)
    merged.extend(vol_anoms)
    merged.extend(sched_anoms)
    merged.extend(miss_anoms)
    return merged


async def _run_today_detectors_and_aggregate(
    day_target: str,
    base_anoms_dir: str,
    base_clean_dir: str,
) -> list[dict]:
    """
    Runs the four detectors ONLY for today_files cleaned outputs.
    """
    exec_date_iso = _exec_date_from_day_target(day_target)
    cleaned_dir = os.path.join(base_clean_dir, "today_files")
    out_base_anoms_today = os.path.join(base_anoms_dir, "today_files")
    os.makedirs(out_base_anoms_today, exist_ok=True)

    paths = glob.glob(os.path.join(cleaned_dir, "*_files_cleaned.json"))
    if not paths:
        return []

    sem = asyncio.Semaphore(8)

    async def _one(p: str):
        async with sem:
            return await _run_detectors_for_path(
                cleaned_json_path=p,
                out_base_anoms_today=out_base_anoms_today,
                exec_date_iso=exec_date_iso,
                base_clean_dir=base_clean_dir,
            )

    bundles = await asyncio.gather(*[_one(p) for p in paths])
    merged: List[dict] = []
    for b in bundles:
        merged.extend(b)
    return merged


# ---------- top-level orchestrate ----------
async def orchestrate(
    *,
    day_target: str,
    files_json_path: str,
    files_last_weekday_json_path: str,
    user_id: str = "thefrancho",
) -> Dict[str, Any]:
    # 1) TODAY: extract + dedupe + anomalies (duplicate/status)
    today_result = await _process_dataset(
        label="today_files",
        day_target=day_target,
        files_map_path=files_json_path,
        app_name="ai-factory",
        user_id=user_id,
        compute_anomalies=True,
    )
    today_agg_path = today_result["anomalies_aggregate_path"]
    base_anoms_dir = f"files_outputs/{day_target}/anomalies"
    base_clean_dir = f"files_outputs/{day_target}/files_cleaned"

    # 2) LAST WEEKDAY: extract + dedupe ONLY (NO anomalies)
    _ = await _process_dataset(
        label="last_weekday_files",
        day_target=day_target,
        files_map_path=files_last_weekday_json_path,
        app_name="ai-factory",
        user_id=user_id,
        compute_anomalies=False,
    )

    # 3) Run the 4 extra detectors (today only) and merge with today's aggregate
    extra_today = await _run_today_detectors_and_aggregate(
        day_target, base_anoms_dir, base_clean_dir
    )

    all_today_anoms: List[dict] = []
    if today_agg_path and os.path.exists(today_agg_path):
        all_today_anoms = _load_json(today_agg_path)
    all_today_anoms += extra_today

    # overwrite final today aggregate with extras included
    if today_agg_path:
        _write_json(today_agg_path, all_today_anoms)

    csv_path = f"{base_anoms_dir}/today_files/_ALL_anomalies.csv"
    _write_anomalies_csv(all_today_anoms, csv_path)

    summary = {
        "date": day_target,
        "today": {
            "anomalies_count": len(all_today_anoms),
            "anomalies_path": today_agg_path,
        },
        "last_weekday": {"anomalies_count": 0, "anomalies_path": None},
    }

    summary_path = f"{base_anoms_dir}/_SUMMARY.json"
    _write_json(summary_path, summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return summary


# ---- CLI ----
def _parse_args():
    ap = argparse.ArgumentParser(
        description="Incidence Orchestrator: extract + dedupe + anomalies"
    )
    ap.add_argument("--date", required=True, help="e.g., 2025-09-08_20_00_UTC")
    ap.add_argument("--files-json", required=True, help="Path to files.json (today)")
    ap.add_argument(
        "--files-last-weekday-json",
        required=True,
        help="Path to files_last_weekday.json",
    )
    return ap.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        asyncio.run(
            orchestrate(
                day_target=args.date,
                files_json_path=args.files_json,
                files_last_weekday_json_path=args.files_last_weekday_json,
            )
        )
    except KeyboardInterrupt:
        pass
