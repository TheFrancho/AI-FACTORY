# orchestrate_extract_and_dedupe.py

from __future__ import annotations
import argparse
import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# === ADK / LLM ===
from google.adk.agents import Agent
from google.adk.models.lite_llm import LiteLlm
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

# === Your extract_file_structure agent pieces (as provided) ===
from ai_factory.agents.incidence_detector.extract_file_structure.schemas import (
    InferredBatchOutput,
)
from ai_factory.agents.incidence_detector.extract_file_structure.prompts import (
    model_instruction,
    model_description,
)
from ai_factory.config import config

# -------------------------
# Config / constants
# -------------------------
TARGET_MODEL = config.default_model
MODEL_NAME = "file_formatter_agent"
OUTPUT_KEY = "file_formatted"

# Tunables
BATCH_SIZE = 20
BATCH_CONCURRENCY = 4
OUT_CONCURRENCY = 4  # CV-level concurrency for extraction

CUSTOM_OUTPUTS_DIR = "custom_outputs/complete_sections"  # where filename_pattern lives


# -------------------------
# Utilities
# -------------------------
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
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _ts(s: Optional[str]) -> float:
    if not s:
        return 0.0
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


# -------------------------
# Build extract_file_structure agent
# -------------------------
def make_extract_file_structure_agent() -> Agent:
    return Agent(
        model=LiteLlm(model=TARGET_MODEL),
        name=MODEL_NAME,
        instruction=model_instruction,
        description=model_description,
        output_schema=InferredBatchOutput,
        output_key=OUTPUT_KEY,
    )


# -------------------------
# Stateless batch runner for extraction (uses fresh SessionService per batch)
# -------------------------
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

    slim_files = [{"filename": f.get("filename"), "status": f.get("status")} for f in files_batch]

    input_json = {
        "datasource_id": datasource_id,
        "context": {"filename_pattern_section": rules_obj},
        "files": slim_files,
    }

    new_message = types.Content(role="user", parts=[types.Part(text=json.dumps(input_json))])

    async for _ in runner.run_async(
        user_id=user_id, session_id=session.id, new_message=new_message
    ):
        pass

    refreshed = await svc.get_session(app_name=app_name, user_id=user_id, session_id=session.id)
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
    batches = [(start, files_all[start : start + batch_size]) for start in range(0, total, batch_size)]
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
                enforce_stateless=True,  # important: avoid contaminating context
            )
            items = inferred.get("inferred_batch", [])
            if len(items) != len(files_batch):
                print(f"[WARN] Batch {start_idx}: expected {len(files_batch)} items, got {len(items)}")
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


# -------------------------
# MinimalDedupeAgentV2 (no-LLM)
# -------------------------
def _status_is_processed(r: Dict[str, Any]) -> bool:
    return str(r.get("status", "")).strip().lower() == "processed"


def _status_text(r: Dict[str, Any]) -> str:
    return str(r.get("status", "")).strip().lower()


def _choose_keeper(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    # Prefer: rows (desc) -> file_size (desc) -> uploaded_at (most recent)
    return max(
        items,
        key=lambda it: (
            int(it.get("rows") or 0),
            float(it.get("file_size") or 0.0),
            _ts(it.get("uploaded_at")),
        ),
    )


def _group_by(records: List[Dict[str, Any]], key_fn) -> Dict[Tuple[Any, ...], List[int]]:
    buckets: Dict[Tuple[Any, ...], List[int]] = {}
    for i, r in enumerate(records):
        k = key_fn(r)
        if k is None:
            continue
        buckets.setdefault(k, []).append(i)
    return buckets


def _dedupe_records(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Returns:
      {
        "stats": {...},
        "final":    [records deduped],
        "removed":  [duplicates with dedupe_reason],
        "harmless": [non-keepers from groups with exactly 1 processed]
      }
    """
    # Pass 1: exact filename
    by_filename = _group_by(records, lambda r: (r["filename"],) if "filename" in r else None)

    grouped = set()
    dup_groups: List[Dict[str, Any]] = []

    for k, idxs in by_filename.items():
        if len(idxs) > 1:
            dup_groups.append({"key_type": "filename", "key_value": k, "idxs": idxs})
            grouped.update(idxs)

    # Pass 2: (cleaned_filename, batch)
    remaining_indices = [i for i in range(len(records)) if i not in grouped]

    def ck_key(r):
        cf = r.get("cleaned_filename")
        if not cf:
            return None
        b = r.get("batch") or ""
        return (cf, b)

    by_ck_local = _group_by([records[i] for i in remaining_indices], ck_key)

    for k, local_idxs in by_ck_local.items():
        if len(local_idxs) > 1:
            real_idxs = [remaining_indices[j] for j in local_idxs]
            dup_groups.append({"key_type": "cleaned_filename+batch", "key_value": k, "idxs": real_idxs})
            grouped.update(real_idxs)

    final: List[Dict[str, Any]] = []
    removed: List[Dict[str, Any]] = []
    harmless: List[Dict[str, Any]] = []

    # Non-duplicates go straight to final
    non_dup_indices = [i for i in range(len(records)) if i not in grouped]
    final.extend(records[i] for i in non_dup_indices)

    # Handle duplicate groups
    for g in dup_groups:
        items = [records[i] for i in g["idxs"]]
        processed = [it for it in items if _status_is_processed(it)]

        if len(processed) == 0:
            reason = "no_processed (no keeper selected)"
            for it in items:
                r = dict(it)
                r["dedupe_reason"] = reason
                removed.append(r)

        elif len(processed) > 1:
            keeper = _choose_keeper(processed)
            final.append(keeper)
            reason = f"multi_processed (keeper={keeper.get('filename')})"
            for it in items:
                if it is keeper:
                    continue
                r = dict(it)
                r["dedupe_reason"] = reason
                removed.append(r)

        else:
            keeper = processed[0]
            final.append(keeper)
            for it in items:
                if it is keeper:
                    continue
                harmless.append(dict(it))

    stats = {
        "total_records": len(records),
        "duplicate_groups": len(dup_groups),
        "final_count": len(final),
        "removed_count": len(removed),
        "harmless_count": len(harmless),
    }
    return {"stats": stats, "final": final, "removed": removed, "harmless": harmless}


def _removed_reason(item: Dict[str, Any]) -> Tuple[str, str]:
    reason = item.get("dedupe_reason")
    if reason and "multi_processed" in reason:
        return ("duplicate_multi_processed", "urgent")
    if reason and "no_processed" in reason:
        return ("duplicate_none_processed", "attention")
    return ("duplicate_unprocessed_copy", "attention")


def _compute_anomalies_and_ok(records_after_extract: List[Dict[str, Any]], dedupe_result: Dict[str, Any]):
    removed = dedupe_result["removed"]
    final = dedupe_result["final"]

    removed_set = {
        (
            it.get("filename"),
            it.get("cleaned_filename"),
            it.get("batch"),
            it.get("uploaded_at"),
        )
        for it in removed
    }

    final_index = {
        (
            it.get("filename"),
            it.get("cleaned_filename"),
            it.get("batch"),
            it.get("uploaded_at"),
        )
        for it in final
    }

    anomalies: List[Dict[str, Any]] = []
    ok: List[Dict[str, Any]] = []

    # Duplicates removed -> anomalies
    for item in removed:
        r = dict(item)
        reason, severity = _removed_reason(item)
        r["incident_type"] = "duplicate"
        r["incident_reason"] = reason
        r["severity"] = severity
        anomalies.append(r)

    # Status failures and flagged duplicates among remaining records
    for r in records_after_extract:
        key = (
            r.get("filename"),
            r.get("cleaned_filename"),
            r.get("batch"),
            r.get("uploaded_at"),
        )

        if key in removed_set:
            continue

        status = _status_text(r)
        is_dupe_flag = bool(r.get("is_duplicated"))

        if status != "processed":
            rr = dict(r)
            rr["incident_type"] = "status_failure"
            rr["incident_reason"] = f"status={status}"
            rr["severity"] = "urgent" if status in {"failed"} else "attention"
            anomalies.append(rr)
            continue

        if is_dupe_flag:
            rr = dict(r)
            rr["incident_type"] = "duplicate"
            rr["incident_reason"] = "flagged_is_duplicated"
            rr["severity"] = "attention"
            anomalies.append(rr)
            continue

        ok.append(r)

    return anomalies, ok


# -------------------------
# Extraction for one CV
# -------------------------
async def _extract_one_cv(
    *,
    cv_id: str,
    files_for_cv: List[Dict[str, Any]],
    app_name: str,
    user_id: str,
    agent: Agent,
    session_service: InMemorySessionService,
) -> List[Dict[str, Any]]:
    """
    Returns the merged 'inferred_batch' list for this CV.
    """
    cv_rules_path = os.path.join(CUSTOM_OUTPUTS_DIR, f"{cv_id}_native.md.json")
    if not os.path.exists(cv_rules_path):
        print(f"[WARN] Missing CV rules: {cv_rules_path} — skipping inference for CV {cv_id}")
        # Pass-through originals with minimal inference (extension, normalized status)
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
    filename_pattern_json = cv_json_extracted.get("filename_pattern_section", cv_json_extracted)

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

    inferred_items = list(result.get("inferred_batch", []))  # ensure list copy

    # Align lengths (defensive)
    originals = files_for_cv
    if len(inferred_items) > len(originals):
        inferred_items = inferred_items[: len(originals)]
    elif len(inferred_items) < len(originals):
        inferred_items += [{} for _ in range(len(originals) - len(inferred_items))]

    # Merge original + inferred
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


# -------------------------
# Full pipeline for one dataset (today_files or last_weekday_files)
# -------------------------
async def _process_dataset(
    *,
    label: str,  # "today_files" | "last_weekday_files"
    day_target: str,
    files_map_path: str,
    app_name: str,
    user_id: str,
) -> Dict[str, Any]:
    """
    Returns a dict with:
      {
        "per_cv": {
          cv_id: {
            "structure_path": ".../{cv}_files.json",
            "cleaned_path": ".../{cv}_files_cleaned.json",
            "removed_path": ".../{cv}_files_removed.json",
            "harmless_path": ".../{cv}_files_harmless.json",
            "anomalies_path": ".../{cv}_dup_fail_anomalies.json",
            "stats": {...}
          }, ...
        },
        "anomalies_aggregate_path": ".../_ALL_anomalies.json",
        "anomalies_count": int
      }
    """
    out_base_struct = f"files_outputs/{day_target}/files_structure/{label}"
    out_base_clean = f"files_outputs/{day_target}/files_cleaned/{label}"
    out_base_anoms = f"files_outputs/{day_target}/anomalies/{label}"

    # Load map {cv_id: [files...]}
    files_map = _load_json(files_map_path)
    if not isinstance(files_map, dict):
        raise ValueError(f"{files_map_path} must be a dict of CV -> list[records]")

    # Build one agent once (cheap)
    session_service = InMemorySessionService()
    agent = make_extract_file_structure_agent()

    sem = asyncio.Semaphore(OUT_CONCURRENCY)

    async def per_cv(cv_id: str, files_for_cv: List[Dict[str, Any]]):
        async with sem:
            # 1) Extract structure
            merged_items = await _extract_one_cv(
                cv_id=cv_id,
                files_for_cv=files_for_cv,
                app_name="ai-factory",
                user_id=user_id,
                agent=agent,
                session_service=session_service,
            )

            # Save raw structure
            cv_struct_path = os.path.join(out_base_struct, f"{cv_id}_files.json")
            _write_json(cv_struct_path, {"inferred_batch": merged_items})

            # 2) Dedupe + anomalies
            dedup = _dedupe_records(merged_items)
            stats = dedup["stats"]

            # Save cleaned variants
            stem = f"{cv_id}_files"
            _write_json(os.path.join(out_base_clean, f"{stem}_cleaned.json"), {"inferred_batch": dedup["final"]})
            _write_json(os.path.join(out_base_clean, f"{stem}_removed.json"), {"inferred_batch": dedup["removed"]})
            _write_json(os.path.join(out_base_clean, f"{stem}_harmless.json"), {"inferred_batch": dedup["harmless"]})

            # Compute anomalies vs extracted items
            anomalies, ok = _compute_anomalies_and_ok(merged_items, dedup)

            # Save ONLY anomalies state
            cv_anom_path = os.path.join(out_base_anoms, f"{cv_id}_dup_fail_anomalies.json")
            _write_json(cv_anom_path, anomalies)

            return {
                "cv_id": cv_id,
                "structure_path": cv_struct_path,
                "cleaned_path": os.path.join(out_base_clean, f"{stem}_cleaned.json"),
                "removed_path": os.path.join(out_base_clean, f"{stem}_removed.json"),
                "harmless_path": os.path.join(out_base_clean, f"{stem}_harmless.json"),
                "anomalies_path": cv_anom_path,
                "stats": stats,
                "anomalies_count": len(anomalies),
            }

    # Run per-CV concurrently
    tasks = [per_cv(cv_id, files_for_cv) for cv_id, files_for_cv in files_map.items()]
    results = []
    for coro in asyncio.as_completed(tasks):
        try:
            results.append(await coro)
        except Exception as e:
            print(f"[ERROR] CV job failed: {e!r}")

    # Aggregate anomalies across CVs for this label
    all_anoms: List[Dict[str, Any]] = []
    for r in results:
        if not r:
            continue
        anoms = _load_json(r["anomalies_path"])
        all_anoms.extend(anoms)

    agg_path = os.path.join(out_base_anoms, "_ALL_anomalies.json")
    _write_json(agg_path, all_anoms)

    per_cv_index = {r["cv_id"]: r for r in results if r}
    return {
        "per_cv": per_cv_index,
        "anomalies_aggregate_path": agg_path,
        "anomalies_count": len(all_anoms),
    }


async def orchestrate(
    *,
    day_target: str,
    files_json_path: str,
    files_last_weekday_json_path: str,
    user_id: str = "thefrancho",
) -> Dict[str, Any]:
    """
    High-level pipeline:
      - Process today_files (files_json_path)
      - Process last_weekday_files (files_last_weekday_json_path)
      - Return summary with paths + anomaly counts
    """
    # today_files
    today_result = await _process_dataset(
        label="today_files",
        day_target=day_target,
        files_map_path=files_json_path,
        app_name="ai-factory",
        user_id=user_id,
    )

    # last_weekday_files
    last_wk_result = await _process_dataset(
        label="last_weekday_files",
        day_target=day_target,
        files_map_path=files_last_weekday_json_path,
        app_name="ai-factory",
        user_id=user_id,
    )

    summary = {
        "date": day_target,
        "today": {
            "anomalies_count": today_result["anomalies_count"],
            "anomalies_path": today_result["anomalies_aggregate_path"],
        },
        "last_weekday": {
            "anomalies_count": last_wk_result["anomalies_count"],
            "anomalies_path": last_wk_result["anomalies_aggregate_path"],
        },
    }

    # Save a single summary “state” file for quick reporting
    summary_path = f"files_outputs/{day_target}/anomalies/_SUMMARY.json"
    _write_json(summary_path, summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return summary


def _parse_args():
    ap = argparse.ArgumentParser(description="Extract + Dedupe Orchestrator")
    ap.add_argument("--date", required=True, help="e.g., 2025-09-08_20_00_UTC")
    ap.add_argument("--files-json", required=True, help="Path to files.json")
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
