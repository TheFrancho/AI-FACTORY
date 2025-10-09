from datetime import datetime
import glob
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

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
    """
    Parse "HH:MM" or "H:MM" or "HH" into minutes since midnight.
    Returns None if not parseable.
    """
    if not hhmm:
        return None
    s = str(hhmm).strip()
    # remove trailing 'UTC' or spaces
    s = s.replace("UTC", "").strip()
    # only keep first token if extra stuff
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
        # "HH" form
        try:
            h = int(s)
            if 0 <= h < 24:
                return h * 60
        except Exception:
            return None
    return None


def _parse_window_end_minutes(window_str: str) -> Optional[int]:
    """
    Parse expected window like "07:30-15:30 UTC" or "07:30-15:30 UTC" and
    return END minutes (e.g., 15:30).
    """
    if not window_str:
        return None
    s = str(window_str).replace("UTC", "").strip()
    # normalize separators
    s = s.replace("–", "-").replace("—", "-")
    # keep first token if extra words
    s = s.split()[0] if " " in s else s
    if "-" in s:
        start_end = s.split("-")
        if len(start_end) >= 2:
            end_part = start_end[1].strip()
            return _parse_time_to_minutes(end_part)
    # if no '-', can be it's a single time - use it directly
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


def _schedule_end_minutes_for_weekday(
    cv: Dict[str, Any], weekday: str
) -> Optional[int]:
    """
    From file_processing_pattern_section.upload_schedule_by_day:
      Prefer expected_window_utc END - else upload_hour_slot_median_utc
      - else mode - else mean.
    Return minutes since midnight for that weekday, or None if unavailable.
    """
    sched = _safe_get(
        cv, "file_processing_pattern_section", "upload_schedule_by_day", default=[]
    )
    for row in sched:
        if str(row.get("day")) != weekday:
            continue

        # 1) expected window end
        endm = _parse_window_end_minutes(row.get("expected_window_utc"))
        if endm is not None:
            return endm

        # 2) median slot
        endm = _parse_time_to_minutes(row.get("upload_hour_slot_median_utc"))
        if endm is not None:
            return endm

        # 3) mode slot
        endm = _parse_time_to_minutes(row.get("upload_hour_slot_mode_utc"))
        if endm is not None:
            return endm

        # 4) mean slot
        endm = _parse_time_to_minutes(row.get("upload_hour_slot_mean_utc"))
        if endm is not None:
            return endm

        return None
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


class UploadAfterScheduleDetectorAgent(Agent):
    """
    Detects files uploaded > +4h after the expected cutoff for the upload weekday.
    Uses Section 2 'upload_schedule_by_day'. Always severity='attention'.
    """

    name: str = "upload_after_schedule_detector_agent"
    description: str = (
        "Flags files uploaded significantly after the expected schedule (+4h)."
    )

    # Minutes after schedule end to consider late
    late_threshold_minutes: int = 4 * 60  # 240

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

        weekday_fallback = _weekday_from_base_folder(base_folder)

        # resource id
        rid = _resource_id_from_stem(Path(input).stem) or "unknown"

        # load cleaned records
        cleaned = _load_json(input)
        records: List[Dict[str, Any]] = list(cleaned.get("inferred_batch", []))

        # load CV
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
                # Can't judge without uploaded_at time
                ok.append(r)
                continue

            # choose weekday of actual upload
            upload_weekday = _upload_weekday_from_uploaded_at(ua, weekday_fallback)

            lag_days = _days_lag_utc(r.get("covered_date"), ua)
            lag_mode = _lag_mode_for_weekday(cv, upload_weekday)

            if lag_days is not None:
                if lag_mode is not None:
                    # allow ±1 day around the mode
                    if abs(lag_days - lag_mode) > 1:
                        ok.append(r)
                        continue
                else:
                    # no mode in CV - skip if lag is clearly a backfill (>1 day)
                    if lag_days > 1:
                        ok.append(r)
                        continue

            # fetch expected end minutes for that weekday
            expected_end_min = _schedule_end_minutes_for_weekday(cv, upload_weekday)
            if expected_end_min is None:
                # No schedule info for this weekday
                ok.append(r)
                continue

            candidates_judged += 1
            # late if upload time-of-day > expected_end + threshold
            if upload_min > expected_end_min + self.late_threshold_minutes:
                flagged += 1
                delta_min = upload_min - expected_end_min
                anomalies.append(
                    {
                        **r,
                        "incident_type": "upload_after_schedule",
                        "incident_reason": f"Uploaded {delta_min/60.0:.1f}h after expected cutoff ({expected_end_min//60:02d}:{expected_end_min%60:02d} UTC) for {upload_weekday}.",
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


async def run_direct(json_path: str) -> Dict[str, Any]:
    agent = UploadAfterScheduleDetectorAgent()
    return await agent.run(json_path)


async def main():
    base_files = [file for file in os.listdir("files_outputs")]

    for base_file in base_files:
        cleaned_dir = f"files_outputs/{base_file}/files_cleaned/today_files"
        paths = glob.glob(f"{cleaned_dir}/*_files_cleaned.json")
        if not paths:
            continue

        out_base = f"files_outputs/{base_file}/schedule_anomaly/"
        os.makedirs(out_base, exist_ok=True)

        for p in paths:
            print(f"\n=== {p} ===")
            result = await run_direct(p)

            rid = result.get("resource_id") or Path(p).stem
            stem = f"{rid}"

            out_ok = f"{out_base}{stem}_schedule_ok.json"
            out_anom = f"{out_base}{stem}_schedule_anomalies.json"

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
