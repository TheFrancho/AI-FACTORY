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
    date_str = base_folder[:10]
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return DAY_NAMES[dt.weekday()]
    except Exception:
        return "Tue"


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


def _resource_id_from_stem(stem: str) -> Optional[str]:
    if stem.endswith("_files_cleaned"):
        return stem[: -len("_files_cleaned")]
    return None


def _current_nonzero_rows_median(records: List[Dict[str, Any]]) -> Optional[float]:
    vals: List[int] = []
    for r in records:
        rv = r.get("rows")
        try:
            if rv is None:
                continue
            n = int(rv)
            if n > 0:
                vals.append(n)
        except Exception:
            continue
    if not vals:
        return None
    vals.sort()
    m = len(vals)
    mid = m // 2
    if m % 2 == 1:
        return float(vals[mid])
    else:
        return (vals[mid - 1] + vals[mid]) / 2.0


def _band_from_rows_minmax_median(
    rows_block: Dict[str, Any],
) -> Optional[Tuple[float, float, float]]:
    if not isinstance(rows_block, dict):
        return None

    mn = rows_block.get("min")
    mx = rows_block.get("max")
    md = rows_block.get("median")

    try:
        if mn is not None and mx is not None:
            mnf = float(mn)
            mxf = float(mx)
            if mxf > 0:
                lo = max(0.0, 0.9 * mnf)  # 10% cushion
                hi = 1.1 * mxf
                center = float(md) if md is not None else (mnf + mxf) / 2.0
                return (lo, hi, max(0.0, center))
    except Exception:
        pass

    try:
        if md is not None:
            mdf = float(md)
            if mdf > 0:
                return (0.5 * mdf, 2.0 * mdf, mdf)
    except Exception:
        pass

    return None


def _band_from_normal_95(
    normal_95_block: Dict[str, Any], median_val: Optional[float]
) -> Optional[Tuple[float, float, float]]:
    if not isinstance(normal_95_block, dict):
        return None
    lo = normal_95_block.get("lo")
    hi = normal_95_block.get("hi")
    try:
        if lo is None or hi is None:
            return None
        lof = float(lo)
        hif = float(hi)
        if hif <= 0:
            return None
        center = float(median_val) if median_val is not None else (lof + hif) / 2.0
        return (max(0.0, lof), hif, max(0.0, center))
    except Exception:
        return None


def _weekday_row_median_from_section3(
    cv: Dict[str, Any], weekday: str
) -> Optional[float]:
    v3 = _safe_get(cv, "volume_characteristics_section", default={})
    for row in _safe_get(v3, "per_weekday", default=[]):
        if str(row.get("day")) == weekday:
            rows_block = row.get("rows") or {}
            md = rows_block.get("median")
            try:
                return float(md) if md is not None else None
            except Exception:
                return None
    return None


def _expected_band_from_section3(
    cv: Dict[str, Any],
    weekday: Optional[str],
    current_perfile_median: Optional[float],
    daily_total_ratio_flag: float = 20.0,
) -> Optional[Tuple[float, float, float]]:
    """
    Build expected band using ONLY volume_characteristics_section.

    Priority:
      A) per-weekday rows band (if present AND not obviously daily totals)
      B) overall rows_stats band
      C) overall normal_95 band
    """
    v3 = _safe_get(cv, "volume_characteristics_section", default={})
    if not isinstance(v3, dict):
        return None

    per_weekday_present = bool(
        _safe_get(v3, "presence", "per_weekday_present", default=False)
    )
    overall_present = bool(_safe_get(v3, "presence", "overall_present", default=False))

    # A) per-weekday, but only if it looks like per-file (not daily totals).
    if per_weekday_present and weekday:
        # detect daily-total shape by comparing weekday median to today's per-file median
        if current_perfile_median and current_perfile_median > 0:
            wd_md = _weekday_row_median_from_section3(cv, weekday)
            if wd_md is not None:
                try:
                    if (
                        float(wd_md) / float(current_perfile_median)
                        >= daily_total_ratio_flag
                    ):
                        # looks like daily totals → ignore per-weekday stats
                        pass
                    else:
                        for row in _safe_get(v3, "per_weekday", default=[]):
                            if str(row.get("day")) == weekday:
                                band = _band_from_rows_minmax_median(
                                    row.get("rows") or {}
                                )
                                if band is not None:
                                    return band
                                break
                except Exception:
                    # fall through to overall
                    pass
        else:
            # no current median → try using per-weekday directly
            for row in _safe_get(v3, "per_weekday", default=[]):
                if str(row.get("day")) == weekday:
                    band = _band_from_rows_minmax_median(row.get("rows") or {})
                    if band is not None:
                        return band
                    break

    # B) overall rows_stats band
    if overall_present:
        overall = _safe_get(v3, "overall", default={})
        band = _band_from_rows_minmax_median(overall.get("rows_stats") or {})
        if band is not None:
            return band

        # C) overall normal_95 band
        median_val = None
        rs = overall.get("rows_stats") or {}
        if isinstance(rs, dict):
            median_val = rs.get("median")
        band = _band_from_normal_95(overall.get("normal_95") or {}, median_val)
        if band is not None:
            return band

    return None


class UnexpectedVolumeVariationAgent(Agent):
    """
    Unexpected Volume Variation Detector

    Input (.run): cleaned file list JSON:
      files_outputs/<DATE_UTC>/files_cleaned/today_files/<RESOURCE_ID>_files_cleaned.json

    Output:
      files_outputs/<DATE_UTC>/volume_anomaly/<RESOURCE_ID>_volume_ok.json
      files_outputs/<DATE_UTC>/volume_anomaly/<RESOURCE_ID>_volume_anomalies.json
    """

    name: str = "unexpected_volume_variation_agent_s3_only"
    description: str = (
        "Flags files whose row counts are outside expected bands from volume_characteristics_section only."
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
        folder_weekday = _weekday_from_base_folder(base_folder)

        rid = _resource_id_from_stem(Path(input).stem) or "unknown"

        cleaned = _load_json(input)
        records: List[Dict[str, Any]] = list(cleaned.get("inferred_batch", []))

        cv_path = f"custom_outputs/complete_sections/{rid}_native.md.json"
        cv: Dict[str, Any] = {}
        if os.path.exists(cv_path):
            cv = _load_json(cv_path)

        # compute today's per-file median to detect daily-total shapes
        current_median = _current_nonzero_rows_median(records)

        # precompute overall band (may be None)
        overall_band = _expected_band_from_section3(
            cv, weekday=None, current_perfile_median=current_median
        )

        anomalies: List[Dict[str, Any]] = []
        ok: List[Dict[str, Any]] = []
        candidates_judged = 0

        # Decide if per-weekday is present at all (to try weekday band first)
        v3 = _safe_get(cv, "volume_characteristics_section", default={})
        per_weekday_present = bool(
            _safe_get(v3, "presence", "per_weekday_present", default=False)
        )

        for r in records:
            # rows
            rows_val = r.get("rows")
            try:
                rows_num = int(rows_val) if rows_val is not None else None
            except Exception:
                rows_num = None

            if rows_num is None or rows_num == 0:
                ok.append(r)
                continue

            # choose band
            rec_weekday = _weekday_for_record(r, folder_weekday)
            band = None
            if per_weekday_present and rec_weekday:
                band = _expected_band_from_section3(
                    cv, weekday=rec_weekday, current_perfile_median=current_median
                )
            if band is None:
                band = overall_band

            if band is None:
                # no baseline → don't flag
                ok.append(r)
                continue

            lo, hi, center = band
            candidates_judged += 1

            if rows_num < lo or rows_num > hi:
                anomalies.append(
                    {
                        **r,
                        "incident_type": "volume_anomaly",
                        "incident_reason": (
                            f"rows={rows_num} outside expected band "
                            f"[{int(lo)}..{int(hi)}] (center≈{int(center)})"
                        ),
                        "weekday_utc": rec_weekday,
                        "expected_lo": lo,
                        "expected_hi": hi,
                        "expected_center": center,
                        "severity": "attention",
                    }
                )
            else:
                ok.append(r)

        stats = {
            "total_records": len(records),
            "candidates_judged": candidates_judged,
            "flagged": len(anomalies),
        }

        return {
            "stats": stats,
            "ok": ok,
            "anomalies": anomalies,
            "weekday_utc": folder_weekday,
            "resource_id": rid,
            "cv_path": cv_path if os.path.exists(cv_path) else None,
        }


async def run_direct(json_path: str) -> Dict[str, Any]:
    agent = UnexpectedVolumeVariationAgent()
    return await agent.run(json_path)


async def main():
    base_files = [file for file in os.listdir("files_outputs")]

    for base_file in base_files:
        cleaned_dir = f"files_outputs/{base_file}/files_cleaned/today_files"
        paths = glob.glob(f"{cleaned_dir}/*_files_cleaned.json")
        if not paths:
            continue

        out_base = f"files_outputs/{base_file}/volume_anomaly/"
        os.makedirs(out_base, exist_ok=True)

        for p in paths:
            print(f"\n=== {p} ===")
            result = await run_direct(p)

            rid = result.get("resource_id") or Path(p).stem
            stem = f"{rid}"

            out_ok = f"{out_base}{stem}_volume_ok.json"
            out_anom = f"{out_base}{stem}_volume_anomalies.json"

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
