from datetime import datetime
import glob
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import asyncio
from google.adk.agents import Agent
from google.adk.sessions import InMemorySessionService
from google.adk.runners import Runner


DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _safe_get(d: Dict[str, Any], *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        if k not in cur:
            return default
        cur = cur[k]
    return cur


def _weekday_from_base_folder(base_folder: str) -> str:
    """
    base_folder looks like '2025-09-09_20_00_UTC'
    We take the first 10 chars as YYYY-MM-DD and compute weekday in UTC semantics.
    """
    date_str = base_folder[:10]
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return DAY_NAMES[dt.weekday()]
    except Exception:
        # fallback - treat as Tue to avoid crashing
        return "Tue"


def _resource_id_from_stem(stem: str) -> Optional[str]:
    """
    <RESOURCE_ID>_files_cleaned -> return <RESOURCE_ID>
    """
    if stem.endswith("_files_cleaned"):
        return stem[: -len("_files_cleaned")]
    return None


def _weekday_from_iso_date(date_str: str) -> Optional[str]:
    try:
        dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
        return DAY_NAMES[dt.weekday()]
    except Exception:
        return None


def _weekday_for_record(r: Dict[str, Any], folder_weekday: str) -> str:
    """
    Priority:
      1) covered_date (YYYY-MM-DD)
      2) uploaded_at (ISO)
      3) folder_weekday (fallback)
    """
    cd = r.get("covered_date")
    if cd:
        wd = _weekday_from_iso_date(str(cd))
        if wd:
            return wd
    ua = r.get("uploaded_at")
    if ua:
        try:
            dt = datetime.fromisoformat(ua.replace("Z", "+00:00"))
            return DAY_NAMES[dt.weekday()]
        except Exception:
            pass
    return folder_weekday


def _cv_expected_zero_global_weekday(
    cv: Dict[str, Any], weekday: str
) -> Optional[bool]:
    """
    From section 3 (volume_characteristics_section.per_weekday[*].empty_files),
    return True if any of mean/median/mode/max > 0, False if explicitly all 0, None if not found.
    """
    per = _safe_get(cv, "volume_characteristics_section", "per_weekday", default=[])
    for row in per:
        if str(row.get("day")) == weekday:
            emp = row.get("empty_files")
            if not isinstance(emp, dict) or not emp:  # no data → unknown
                return None

            saw_any_key = False
            for k in ("mean", "median", "mode", "max"):
                if k in emp:
                    saw_any_key = True
                    try:
                        if emp[k] is not None and float(emp[k]) > 0:
                            return True
                    except Exception:
                        pass
            if saw_any_key:
                # we had keys and none were > 0 → explicitly zero
                return False
            # keys block present but empty → unknown
            return None
    return None


def _cv_expected_zero_entity_weekday(
    cv: Dict[str, Any], entity: str, weekday: str
) -> Optional[bool]:
    """
    From section 4 (day_of_week_section_pattern.entity_weekday[*].median_empty),
    return True if median_empty > 0 for entity+weekday, False if == 0, None if not found.
    """
    ent_rows = _safe_get(
        cv, "day_of_week_section_pattern", "entity_weekday", default=[]
    )
    for row in ent_rows:
        if str(row.get("entity")) == entity and str(row.get("day")) == weekday:
            me = row.get("median_empty")
            try:
                if me is None:
                    return None
                return float(me) > 0
            except Exception:
                return None
    return None


def _is_zero_expected(cv: Dict[str, Any], entity: str, weekday: str) -> bool:
    # 1. Entity + weekday (section 4)
    ent = _cv_expected_zero_entity_weekday(cv, entity, weekday)
    if ent is not None:
        return ent

    # 1.5. Weekday table (section 4)
    wk4 = _cv_expected_zero_weekday_section4(cv, weekday)
    if wk4 is not None:
        return wk4

    # 2. Global weekday (section 3)
    glob = _cv_expected_zero_global_weekday(cv, weekday)
    if glob is not None:
        return glob

    # 3. Global fallback (section 2)
    glob2 = _cv_global_empty_expected_section2(cv)
    if glob2 is not None:
        return glob2

    # default
    return False


def _cv_global_empty_expected_section2(cv: Dict[str, Any]) -> Optional[bool]:
    pct = _safe_get(
        cv, "file_processing_pattern_section", "status_percentages", default={}
    )
    # keys can be 'empty' for some sources; or absent
    for k in ("empty", "empties", "empty_files"):
        if k in pct:
            try:
                return float(pct[k]) > 0.0
            except Exception:
                pass
    return None


def _is_empty_candidate(r: Dict[str, Any]) -> bool:
    rows = r.get("rows")
    status = str(r.get("status") or "").strip().lower()
    # rows == 0 counts
    try:
        if rows is not None and int(rows) == 0:
            return True
    except Exception:
        pass
    # pipeline-declared empties count too
    if status in {"empty", "no_data"}:
        return True
    return False


def _cv_expected_zero_weekday_section4(
    cv: Dict[str, Any], weekday: str
) -> Optional[bool]:
    """
    From section 4 (day_of_week_section_pattern.weekday[].empty_files),
    return True if any of min/mean/median/mode/max > 0 for the weekday,
    False if explicitly 0 across present keys, None if not found/missing.
    """
    wk = _safe_get(cv, "day_of_week_section_pattern", "weekday", default=[])
    for row in wk:
        if str(row.get("day")) != weekday:
            continue
        emp = row.get("empty_files")
        if not isinstance(emp, dict) or not emp:
            return None  # no data for this weekday
        saw_any_key = False
        for k in ("min", "mean", "median", "mode", "max"):
            if k in emp:
                saw_any_key = True
                try:
                    if emp[k] is not None and float(emp[k]) > 0:
                        return True
                except Exception:
                    pass
        if saw_any_key:
            return False  # keys present and none > 0 → explicitly zero
        return None
    return None


class UnexpectedEmptyDetectorAgent(Agent):
    """
    Unexpected Empty File Detector.

    Input to .run(): path to a *cleaned* file list JSON, e.g.:
      files_outputs/<DATE_UTC>/files_cleaned/today_files/<RESOURCE_ID>_files_cleaned.json

    Output (written by main()):
      files_outputs/<DATE_UTC>/empty_anomaly/<RESOURCE_ID>_empty_ok.json
      files_outputs/<DATE_UTC>/empty_anomalies/<RESOURCE_ID>_empty_anomalies.json
    """

    name: str = "unexpected_empty_detector_agent"
    description: str = (
        "Detects unexpected empty files (rows==0) using CV weekday/entity patterns."
    )

    urgent_entity_count_threshold: int = (
        3  # bump to 'urgent' if > N unexpected empties for same entity
    )

    async def run(self, input: str, **kwargs) -> Dict[str, Any]:
        if not isinstance(input, str):
            raise ValueError("Input must be a path to the *cleaned* JSON file.")
        if not os.path.exists(input):
            raise FileNotFoundError(input)

        parts = Path(input).parts
        try:
            i = parts.index("files_outputs")
            base_folder = parts[i + 1]
        except ValueError:
            base_folder = "1970-01-01_00_00_UTC"
        weekday = _weekday_from_base_folder(base_folder)

        # resource id
        rid = _resource_id_from_stem(Path(input).stem) or "unknown"

        # load cleaned records
        cleaned = _load_json(input)
        records: List[Dict[str, Any]] = list(cleaned.get("inferred_batch", []))

        # locate CV (complete_sections has 2/3/4 merged per your printouts)
        # custom_outputs/complete_sections/<RID>_native.md.json
        # fallback: try other known paths if needed
        cv_path = f"custom_outputs/complete_sections/{rid}_native.md.json"
        cv: Dict[str, Any] = {}
        if os.path.exists(cv_path):
            cv = _load_json(cv_path)

        # classify
        anomalies: List[Dict[str, Any]] = []
        ok: List[Dict[str, Any]] = []

        # step 1: pick candidates: processed & rows == 0
        candidates: List[Dict[str, Any]] = []
        for r in records:
            if _is_empty_candidate(r):
                candidates.append(r)
            else:
                ok.append(r)

        # step 2: expected vs unexpected using CV
        unexpected_by_entity: Dict[str, int] = {}
        expected_count = 0
        unexpected_count = 0

        for r in candidates:
            entity = str(r.get("entity") or "")
            rec_weekday = _weekday_for_record(r, weekday)
            expected_zero = _is_zero_expected(cv, entity, rec_weekday)

            if expected_zero:
                expected_count += 1
                ok.append(r)
                continue

            # unexpected
            unexpected_count += 1
            unexpected_by_entity[entity] = unexpected_by_entity.get(entity, 0) + 1

            anomalies.append(
                {
                    **r,
                    "incident_type": "unexpected_empty",
                    "incident_reason": f"rows==0 but CV indicates zero not expected for {rec_weekday}",
                    "weekday_utc": rec_weekday,
                    "severity": "attention",
                }
            )

        # step 3: bump severity if many per entity
        for item in anomalies:
            ent = str(item.get("entity") or "")
            if unexpected_by_entity.get(ent, 0) > self.urgent_entity_count_threshold:
                item["severity"] = "urgent"

        stats = {
            "total_records": len(records),
            "empty_candidates": len(candidates),
            "expected_empty": expected_count,
            "unexpected_empty": unexpected_count,
        }

        return {
            "stats": stats,
            "ok": ok,
            "anomalies": anomalies,
            "weekday_utc": weekday,
            "resource_id": rid,
            "cv_path": cv_path if os.path.exists(cv_path) else None,
        }


async def run_direct(json_path: str) -> Dict[str, Any]:
    agent = UnexpectedEmptyDetectorAgent()
    return await agent.run(json_path)


async def run_with_runner(json_path: str) -> Dict[str, Any]:
    session_service = InMemorySessionService()
    agent = UnexpectedEmptyDetectorAgent()
    runner = Runner(agent=agent, app_name="ai-factory", session_service=session_service)

    new_message = {"role": "user", "content": json_path}
    stream = runner.run(
        user_id="cli",
        session_id=f"unexpected_empty::{os.path.basename(json_path)}",
        new_message=new_message,
    )

    import inspect

    if inspect.isawaitable(stream):
        await stream
    elif inspect.isasyncgen(stream):
        async for _ in stream:
            pass
    elif inspect.isgenerator(stream):
        for _ in stream:
            pass

    return await agent.run(json_path)


async def main():
    base_files = [file for file in os.listdir("files_outputs")]

    for base_file in base_files:
        cleaned_dir = f"files_outputs/{base_file}/files_cleaned/today_files"
        paths = glob.glob(f"{cleaned_dir}/*_files_cleaned.json")
        if not paths:
            continue

        out_base = f"files_outputs/{base_file}/empty_anomaly/"
        os.makedirs(out_base, exist_ok=True)

        for p in paths:
            print(f"\n=== {p} ===")
            result = await run_direct(p)

            # write outputs per resource
            rid = result.get("resource_id") or Path(p).stem
            stem = f"{rid}"

            out_ok = f"{out_base}{stem}_empty_ok.json"
            out_anom = f"{out_base}{stem}_empty_anomalies.json"

            with open(out_ok, "w", encoding="utf-8") as f:
                json.dump(result["ok"], f, ensure_ascii=False, indent=2)

            with open(out_anom, "w", encoding="utf-8") as f:
                json.dump(result["anomalies"], f, ensure_ascii=False, indent=2)

            print(json.dumps(result["stats"], ensure_ascii=False, indent=2))
            print(f"Wrote:\n  {out_ok}\n  {out_anom}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
