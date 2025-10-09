from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import os
import json

from google.adk.agents import Agent

WEEKDAY = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _to_utc_date(s: str) -> datetime:
    try:
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


def _weekday_name(d: datetime) -> str:
    return WEEKDAY[d.weekday()]


def _records_on_exec_day(
    records: List[Dict[str, Any]], exec_date: datetime
) -> List[Dict[str, Any]]:
    y, m, d = exec_date.year, exec_date.month, exec_date.day
    out = []
    for r in records:
        ts = r.get("uploaded_at")
        if not ts:
            out.append(r)
            continue
        dt = _to_utc_date(ts)
        if (dt.year, dt.month, dt.day) == (y, m, d):
            out.append(r)
    return out


def _actual_counts_by_entity(records: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for r in records:
        e = str(r.get("entity") or "").strip()
        if e:
            counts[e] = counts.get(e, 0) + 1
    return counts


def _expected_from_cv_entity_weekday(
    cv: Dict[str, Any], weekday: str
) -> List[Dict[str, Any]]:
    dsec = cv.get("day_of_week_section_pattern") or {}
    rows = dsec.get("entity_weekday") or []
    out = []
    for it in rows:
        if (it.get("day") or "").strip() != weekday:
            continue
        ent = str(it.get("entity") or "").strip()
        mf = it.get("median_files")
        if ent and mf is not None and float(mf) > 0:
            out.append({"entity": ent, "median_files": int(round(float(mf)))})
    return out


def _cv_has_entity_weekday(cv: Dict[str, Any]) -> bool:
    dsec = cv.get("day_of_week_section_pattern") or {}
    lst = dsec.get("entity_weekday") or []
    return len(lst) > 0


def _cv_weekday_rows_median(cv: Dict[str, Any], weekday: str) -> Optional[float]:
    dsec = cv.get("day_of_week_section_pattern") or {}
    rows = dsec.get("weekday") or []
    for it in rows:
        if (it.get("day") or "").strip() == weekday:
            rows_stats = it.get("rows") or {}
            med = rows_stats.get("median")
            if med is None:
                return 0.0
            try:
                return float(med)
            except Exception:
                return 0.0
    return None


def _cv_meta(cv: Dict[str, Any]) -> Dict[str, Optional[str]]:
    t = cv.get("title_section") or {}
    return {
        "resource_id": (t.get("resource_id") if t.get("resource_id") else None),
        "workspace_id": (t.get("workspace_id") if t.get("workspace_id") else None),
        "datasource_cv_name": (
            t.get("datasource_cv_name") if t.get("datasource_cv_name") else None
        ),
    }


class MissingFileDetectorSimple(Agent):
    """
    Input:
      {
        "today_path": ".../_files_cleaned.json",
        "cv_path": ".../{rid}_native.md.json",
        "last_weekday_path": ".../last_weekday_files/{rid}_files_cleaned.json" | None,
        "exec_date": "YYYY-MM-DD"
      }
    """

    name: str = "missing_file_detector_simple"
    description: str = (
        "Missing-file detector that emits per-entity incidents if CV has entity_weekday; else source-level."
    )

    async def run(self, input: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        today_path = input["today_path"]
        cv_path = input["cv_path"]
        last_weekday_path = input.get("last_weekday_path")
        exec_date = _to_utc_date(input["exec_date"])
        weekday = _weekday_name(exec_date)

        today_blob = _load_json(today_path)
        today_records = list((today_blob or {}).get("inferred_batch") or [])
        today_records = _records_on_exec_day(today_records, exec_date)

        cv_blob = _load_json(cv_path) if os.path.exists(cv_path) else {}
        meta = _cv_meta(cv_blob)

        last_week_blob = (
            _load_json(last_weekday_path)
            if (last_weekday_path and os.path.exists(last_weekday_path))
            else {}
        )
        last_week_records = list((last_week_blob or {}).get("inferred_batch") or [])
        last_week_entities = {
            str(r.get("entity") or "").strip()
            for r in last_week_records
            if r.get("entity")
        }

        anomalies: List[Dict[str, Any]] = []
        ok_files: List[Dict[str, Any]] = today_records[:]  # pass-through

        if _cv_has_entity_weekday(cv_blob):
            expected = _expected_from_cv_entity_weekday(cv_blob, weekday)
            actual_counts = _actual_counts_by_entity(today_records)

            for exp in expected:
                ent = exp["entity"]
                expected_cnt = int(exp["median_files"])
                actual_cnt = int(actual_counts.get(ent, 0))
                if actual_cnt > 0:
                    continue
                anomalies.append(
                    {
                        "incident_type": "missing_files",
                        "incident_reason": "Entity expected on this weekday (median_files>0) but no files received today.",
                        "entity": ent,
                        "expected_count_hint": expected_cnt,
                        "actual_count": 0,
                        "weekday": weekday,
                        "exec_date_utc": exec_date.date().isoformat(),
                        "severity": "attention",
                        "confidence_hint": (
                            "high" if ent in last_week_entities else "medium"
                        ),
                        "support": {
                            **meta,
                            "cv_source": os.path.basename(cv_path) if cv_path else None,
                            "today_source": os.path.basename(today_path),
                            "last_weekday_source": (
                                os.path.basename(last_weekday_path)
                                if last_weekday_path
                                else None
                            ),
                        },
                    }
                )
        else:
            med = _cv_weekday_rows_median(cv_blob, weekday)
            if med is not None and med > 0 and len(today_records) == 0:
                anomalies.append(
                    {
                        "incident_type": "missing_source",
                        "incident_reason": "CV shows typical activity for this weekday (rows.median>0) but no files were received today.",
                        "weekday": weekday,
                        "exec_date_utc": exec_date.date().isoformat(),
                        "expected_activity": True,
                        "actual_files_today": 0,
                        "severity": "attention",
                        "confidence_hint": (
                            "high" if len(last_week_records) > 0 else "medium"
                        ),
                        "support": {
                            **meta,
                            "cv_source": os.path.basename(cv_path) if cv_path else None,
                            "today_source": os.path.basename(today_path),
                            "last_weekday_source": (
                                os.path.basename(last_weekday_path)
                                if last_weekday_path
                                else None
                            ),
                        },
                    }
                )

        stats = {
            "exec_date_utc": exec_date.date().isoformat(),
            "weekday": weekday,
            "total_today_records": len(today_records),
            "anomalies_found": len(anomalies),
        }

        return {
            "stats": stats,
            "ok": {"inferred_batch": ok_files},
            "anomalies": {"inferred_batch": anomalies},
        }
