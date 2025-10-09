from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import os
import json

from google.adk.agents import Agent

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
    date_str = base_folder[:10]
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return DAY_NAMES[dt.weekday()]
    except Exception:
        return "Tue"


def _resource_id_from_stem(stem: str) -> Optional[str]:
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
    cd = r.get("covered_date")
    if cd:
        wd = _weekday_from_iso_date(str(cd))
        if wd:
            return wd
    ua = r.get("uploaded_at")
    if ua:
        try:
            dt = datetime.fromisoformat(str(ua).replace("Z", "+00:00"))
            return DAY_NAMES[dt.weekday()]
        except Exception:
            pass
    return folder_weekday


def _cv_expected_zero_global_weekday(
    cv: Dict[str, Any], weekday: str
) -> Optional[bool]:
    per = _safe_get(cv, "volume_characteristics_section", "per_weekday", default=[])
    for row in per:
        if str(row.get("day")) == weekday:
            emp = row.get("empty_files")
            if not isinstance(emp, dict) or not emp:
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
                return False
            return None
    return None


def _cv_expected_zero_entity_weekday(
    cv: Dict[str, Any], entity: str, weekday: str
) -> Optional[bool]:
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


def _cv_expected_zero_weekday_section4(
    cv: Dict[str, Any], weekday: str
) -> Optional[bool]:
    wk = _safe_get(cv, "day_of_week_section_pattern", "weekday", default=[])
    for row in wk:
        if str(row.get("day")) != weekday:
            continue
        emp = row.get("empty_files")
        if not isinstance(emp, dict) or not emp:
            return None
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
            return False
        return None
    return None


def _cv_global_empty_expected_section2(cv: Dict[str, Any]) -> Optional[bool]:
    pct = _safe_get(
        cv, "file_processing_pattern_section", "status_percentages", default={}
    )
    for k in ("empty", "empties", "empty_files"):
        if k in pct:
            try:
                return float(pct[k]) > 0.0
            except Exception:
                pass
    return None


def _is_zero_expected(cv: Dict[str, Any], entity: str, weekday: str) -> bool:
    ent = _cv_expected_zero_entity_weekday(cv, entity, weekday)
    if ent is not None:
        return ent
    wk4 = _cv_expected_zero_weekday_section4(cv, weekday)
    if wk4 is not None:
        return wk4
    glob = _cv_expected_zero_global_weekday(cv, weekday)
    if glob is not None:
        return glob
    glob2 = _cv_global_empty_expected_section2(cv)
    if glob2 is not None:
        return glob2
    return False


def _is_empty_candidate(r: Dict[str, Any]) -> bool:
    rows = r.get("rows")
    status = str(r.get("status") or "").strip().lower()
    try:
        if rows is not None and int(rows) == 0:
            return True
    except Exception:
        pass
    if status in {"empty", "no_data"}:
        return True
    return False


class UnexpectedEmptyDetectorAgent(Agent):
    name: str = "unexpected_empty_detector_agent"
    description: str = (
        "Detects unexpected empty files (rows==0) using CV weekday/entity patterns"
    )
    urgent_entity_count_threshold: int = 3

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

        rid = _resource_id_from_stem(Path(input).stem) or "unknown"

        cleaned = _load_json(input)
        records: List[Dict[str, Any]] = list(cleaned.get("inferred_batch", []))

        cv_path = f"custom_outputs/complete_sections/{rid}_native.md.json"
        cv: Dict[str, Any] = {}
        if os.path.exists(cv_path):
            cv = _load_json(cv_path)

        anomalies: List[Dict[str, Any]] = []
        ok: List[Dict[str, Any]] = []

        candidates: List[Dict[str, Any]] = []
        for r in records:
            if _is_empty_candidate(r):
                candidates.append(r)
            else:
                ok.append(r)

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

            unexpected_count += 1
            unexpected_by_entity[entity] = unexpected_by_entity.get(entity, 0) + 1

            anomalies.append(
                {
                    **r,
                    "incident_type": "unexpected_empty",
                    "incident_reason": f"Rows are 0 where empties are not expected on {rec_weekday}.",
                    "weekday_utc": rec_weekday,
                    "severity": "attention",
                }
            )

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
