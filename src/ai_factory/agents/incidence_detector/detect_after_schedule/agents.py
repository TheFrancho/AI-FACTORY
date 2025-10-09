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


def _parse_time_to_minutes(hhmm: str) -> Optional[int]:
    if not hhmm:
        return None
    s = str(hhmm).strip().replace("UTC", "").strip()
    s = s.split()[0]
    if ":" in s:
        parts = s.split(":")
        if len(parts) >= 2:
            try:
                h = int(parts[0])
                m = int(parts[1])
                if 0 <= h < 24 and 0 <= m < 60:
                    return h * 60 + m
            except Exception:
                return None
    else:
        try:
            h = int(s)
            if 0 <= h < 24:
                return h * 60
        except Exception:
            return None
    return None


def _parse_window_end_minutes(window_str: str) -> Optional[int]:
    if not window_str:
        return None
    s = str(window_str).replace("UTC", "").strip()
    s = s.replace("–", "-").replace("—", "-")
    s = s.split()[0] if " " in s else s
    if "-" in s:
        start_end = s.split("-")
        if len(start_end) >= 2:
            end_part = start_end[1].strip()
            return _parse_time_to_minutes(end_part)
    return _parse_time_to_minutes(s)


def _upload_weekday_from_uploaded_at(uploaded_at: Optional[str], fallback: str) -> str:
    if uploaded_at:
        try:
            dt = datetime.fromisoformat(str(uploaded_at).replace("Z", "+00:00"))
            return DAY_NAMES[dt.weekday()]
        except Exception:
            pass
    return fallback


def _minutes_since_midnight(uploaded_at: Optional[str]) -> Optional[int]:
    if not uploaded_at:
        return None
    try:
        dt = datetime.fromisoformat(str(uploaded_at).replace("Z", "+00:00"))
        return dt.hour * 60 + dt.minute
    except Exception:
        return None


def _days_lag_utc(
    covered_date: Optional[str], uploaded_at: Optional[str]
) -> Optional[int]:
    if not covered_date or not uploaded_at:
        return None
    try:
        cd = datetime.strptime(str(covered_date)[:10], "%Y-%m-%d").date()
        ua = datetime.fromisoformat(str(uploaded_at).replace("Z", "+00:00")).date()
        return (ua - cd).days
    except Exception:
        return None


def _lag_mode_for_weekday(cv: Dict[str, Any], weekday: str) -> Optional[int]:
    sched = _safe_get(
        cv, "file_processing_pattern_section", "upload_schedule_by_day", default=[]
    )
    for row in sched:
        if str(row.get("day")) == weekday:
            lm = row.get("upload_lag_days_mode")
            try:
                return int(lm) if lm is not None else None
            except Exception:
                return None
    return None


def _schedule_end_minutes_for_weekday(
    cv: Dict[str, Any], weekday: str
) -> Optional[int]:
    sched = _safe_get(
        cv, "file_processing_pattern_section", "upload_schedule_by_day", default=[]
    )
    for row in sched:
        if str(row.get("day")) != weekday:
            continue
        endm = _parse_window_end_minutes(row.get("expected_window_utc"))
        if endm is not None:
            return endm
        endm = _parse_time_to_minutes(row.get("upload_hour_slot_median_utc"))
        if endm is not None:
            return endm
        endm = _parse_time_to_minutes(row.get("upload_hour_slot_mode_utc"))
        if endm is not None:
            return endm
        endm = _parse_time_to_minutes(row.get("upload_hour_slot_mean_utc"))
        if endm is not None:
            return endm
        return None
    return None


class UploadAfterScheduleDetectorAgent(Agent):
    """
    Detects files uploaded > +4h after the expected cutoff for the upload weekday.
    Uses Section 2 'upload_schedule_by_day'. Always severity='attention'
    """

    name: str = "upload_after_schedule_detector_agent"
    description: str = (
        "Flags files uploaded significantly after the expected schedule (+4h)"
    )
    late_threshold_minutes: int = 4 * 60  # 240

    async def run(self, input: str, **kwargs) -> Dict[str, Any]:
        if not isinstance(input, str):
            raise ValueError("Input must be a path to the *cleaned* JSON file")
        if not os.path.exists(input):
            raise FileNotFoundError(input)

        parts = Path(input).parts
        try:
            i = parts.index("files_outputs")
            base_folder = parts[i + 1]
        except ValueError:
            base_folder = "1970-01-01_00_00_UTC"

        weekday_fallback = _weekday_from_base_folder(base_folder)
        rid = _resource_id_from_stem(Path(input).stem) or "unknown"

        cleaned = _load_json(input)
        records: List[Dict[str, Any]] = list(cleaned.get("inferred_batch", []))

        cv_path = f"custom_outputs/complete_sections/{rid}_native.md.json"
        cv: Dict[str, Any] = {}
        if os.path.exists(cv_path):
            cv = _load_json(cv_path)

        anomalies: List[Dict[str, Any]] = []
        ok: List[Dict[str, Any]] = []

        candidates_judged = 0
        flagged = 0

        for r in records:
            ua = r.get("uploaded_at")
            upload_min = _minutes_since_midnight(ua)
            if upload_min is None:
                ok.append(r)
                continue

            upload_weekday = _upload_weekday_from_uploaded_at(ua, weekday_fallback)

            lag_days = _days_lag_utc(r.get("covered_date"), ua)
            lag_mode = _lag_mode_for_weekday(cv, upload_weekday)

            if lag_days is not None:
                if lag_mode is not None:
                    if abs(lag_days - lag_mode) > 1:
                        ok.append(r)
                        continue
                else:
                    if lag_days > 1:
                        ok.append(r)
                        continue

            expected_end_min = _schedule_end_minutes_for_weekday(cv, upload_weekday)
            if expected_end_min is None:
                ok.append(r)
                continue

            candidates_judged += 1
            if upload_min > expected_end_min + self.late_threshold_minutes:
                flagged += 1
                delta_min = upload_min - expected_end_min
                anomalies.append(
                    {
                        **r,
                        "incident_type": "upload_after_schedule",
                        "incident_reason": (
                            f"Uploaded {delta_min/60.0:.1f}h after expected cutoff "
                            f"({expected_end_min//60:02d}:{expected_end_min%60:02d} UTC) "
                            f"for {upload_weekday}."
                        ),
                        "weekday_upload_utc": upload_weekday,
                        "severity": "attention",
                    }
                )
            else:
                ok.append(r)

        stats = {
            "total_records": len(records),
            "candidates_judged": candidates_judged,
            "flagged": flagged,
        }

        return {
            "stats": stats,
            "ok": ok,
            "anomalies": anomalies,
            "resource_id": rid,
            "cv_path": cv_path if os.path.exists(cv_path) else None,
        }
