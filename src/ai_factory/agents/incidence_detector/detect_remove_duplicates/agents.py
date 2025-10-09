from datetime import datetime
from typing import Any, Dict, List, Tuple, Optional


def _ts(s: Optional[str]) -> float:
    if not s:
        return 0.0
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


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


def _group_by(
    records: List[Dict[str, Any]], key_fn
) -> Dict[Tuple[Any, ...], List[int]]:
    buckets: Dict[Tuple[Any, ...], List[int]] = {}
    for i, r in enumerate(records):
        k = key_fn(r)
        if k is None:
            continue
        buckets.setdefault(k, []).append(i)
    return buckets


def _removed_reason(item: Dict[str, Any]) -> Tuple[str, str]:
    reason = item.get("dedupe_reason")
    if reason and "multi_processed" in reason:
        return ("duplicate_multi_processed", "urgent")
    if reason and "no_processed" in reason:
        return ("duplicate_none_processed", "attention")
    return ("duplicate_unprocessed_copy", "attention")


def human_reason(incident_type: str, code_or_msg: str) -> str:
    """
    Map internal codes to human-readable messages
    Use this for dedupe + status anomalies. The two new detectors already output readable reasons.
    """
    code = (code_or_msg or "").strip().lower()

    if incident_type == "duplicate":
        if "multi_processed" in code:
            return "Multiple processed duplicates; kept the best version."
        if "none_processed" in code:
            return "Duplicate group without any processed file."
        if "flagged_is_duplicated" in code:
            return "Marked as duplicate by upstream pipeline."
        return "Duplicate file removed."

    if incident_type == "status_failure":
        if code.startswith("status="):
            status = code.split("=", 1)[1]
            if status == "failed":
                return "File processing failed."
            if status == "empty":
                return "File was empty."
            if status == "unknown":
                return "File status is unknown."
            return f"File has status '{status}'."
        return "File status anomaly."

    return code_or_msg or "Anomaly detected."


def dedupe_records(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    No-LLM dedupe with three outputs
    Returns:
      {
        "stats": {...},
        "final":    [deduped records],
        "removed":  [discarded duplicates with dedupe_reason],
        "harmless": [non-keepers from groups with exactly 1 processed]
      }
    """
    # Pass 1: exact filename
    by_filename = _group_by(
        records, lambda r: (r["filename"],) if "filename" in r else None
    )

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
            dup_groups.append(
                {
                    "key_type": "cleaned_filename+batch",
                    "key_value": k,
                    "idxs": real_idxs,
                }
            )
            grouped.update(real_idxs)

    final: List[Dict[str, Any]] = []
    removed: List[Dict[str, Any]] = []
    harmless: List[Dict[str, Any]] = []

    non_dup_indices = [i for i in range(len(records)) if i not in grouped]
    final.extend(records[i] for i in non_dup_indices)

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


def compute_dedupe_and_status_anomalies(
    original_records: List[Dict[str, Any]], dedupe_result: Dict[str, Any]
) -> tuple[list[dict], list[dict]]:
    """
    Build anomalies from dedupe + status. Returns (anomalies, ok).
    """
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

    anomalies: List[Dict[str, Any]] = []
    ok: List[Dict[str, Any]] = []

    # Duplicates removed
    for item in removed:
        r = dict(item)
        code, severity = _removed_reason(item)
        r["incident_type"] = "duplicate"
        r["incident_reason"] = human_reason("duplicate", code)
        r["severity"] = severity
        anomalies.append(r)

    # Status failures + upstream duplicate flag across originals
    for r in original_records:
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
            rr["incident_reason"] = human_reason("status_failure", f"status={status}")
            rr["severity"] = "urgent" if status in {"failed"} else "attention"
            anomalies.append(rr)
            continue

        if is_dupe_flag:
            rr = dict(r)
            rr["incident_type"] = "duplicate"
            rr["incident_reason"] = human_reason("duplicate", "flagged_is_duplicated")
            rr["severity"] = "attention"
            anomalies.append(rr)
            continue

        ok.append(r)

    return anomalies, ok
