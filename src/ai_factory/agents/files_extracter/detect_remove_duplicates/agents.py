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


def _ts(s: Optional[str]) -> float:
    if not s:
        return 0.0
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _choose_keeper(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Prefer: rows (desc) -> file_size (desc) -> uploaded_at (most recent)
    """
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


def _load_records(json_path: str) -> List[Dict[str, Any]]:
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return list(data.get("inferred_batch", []))


def _status_is_processed(r: Dict[str, Any]) -> bool:
    return str(r.get("status", "")).strip().lower() == "processed"


class MinimalDedupeAgentV2(Agent):
    """
    Detect duplicates by:
      1) exact filename
      2) (cleaned_filename, batch with "" if missing)

    Returns:
      {
        "stats": {...},
        "final":    [ input-like dicts, deduped ],
        "removed":  [ input-like dicts + dedupe_reason ],
        "harmless": [ input-like dicts (non-keepers) from groups with exactly 1 processed ],
    }
    """

    name: str = "minimal_name_dedupe_agent_v2"
    description: str = "No-LLM dedupe with three outputs: final, removed, harmless."

    async def run(self, input: str, **kwargs) -> Dict[str, Any]:
        if not isinstance(input, str):
            raise ValueError("Input must be a path to the JSON file.")
        if not os.path.exists(input):
            raise FileNotFoundError(input)

        records = _load_records(input)

        # Pass 1: exact filename
        by_filename = _group_by(
            records, lambda r: (r["filename"],) if "filename" in r else None
        )

        grouped = set()
        dup_groups: List[Dict[str, Any]] = []

        for k, idxs in by_filename.items():
            if len(idxs) > 1:
                dup_groups.append(
                    {"key_type": "filename", "key_value": k, "idxs": idxs}
                )
                grouped.update(idxs)

        # Pass 2: (cleaned_filename, batch) for items not yet grouped
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

        # Build outputs
        final: List[Dict[str, Any]] = []  # deduped set
        removed: List[Dict[str, Any]] = []  # discarded + reason
        harmless: List[Dict[str, Any]] = (
            []
        )  # non-keepers only from groups with exactly 1 processed

        # Start with all non-duplicates = final
        non_dup_indices = [i for i in range(len(records)) if i not in grouped]
        final.extend(records[i] for i in non_dup_indices)

        # Handle the duplicate groups
        for g in dup_groups:
            items = [records[i] for i in g["idxs"]]
            processed = [it for it in items if _status_is_processed(it)]

            if len(processed) == 0:
                # Case 1: none processed -> error; keep none, move all to removed
                reason = "no_processed (no keeper selected)"
                for it in items:
                    r = dict(it)
                    r["dedupe_reason"] = reason
                    removed.append(r)

            elif len(processed) > 1:
                # Case 2: >1 processed -> error; choose a keeper among processed
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
                # Case 3: exactly 1 processed -> harmless duplicates
                keeper = processed[0]
                final.append(keeper)
                for it in items:
                    if it is keeper:
                        continue
                    harmless.append(dict(it))  # no reason: harmless by definition

        stats = {
            "total_records": len(records),
            "duplicate_groups": len(dup_groups),
            "final_count": len(final),
            "removed_count": len(removed),
            "harmless_count": len(harmless),
        }
        return {
            "stats": stats,
            "final": final,
            "removed": removed,
            "harmless": harmless,
        }


async def run_direct(json_path: str) -> Dict[str, Any]:
    agent = MinimalDedupeAgentV2()
    return await agent.run(json_path)


async def run_with_runner(json_path: str) -> Dict[str, Any]:
    session_service = InMemorySessionService()
    agent = MinimalDedupeAgentV2()
    runner = Runner(agent=agent, app_name="ai-factory", session_service=session_service)

    new_message = {"role": "user", "content": json_path}
    stream = runner.run(
        user_id="cli",
        session_id=f"dedupe::{os.path.basename(json_path)}",
        new_message=new_message,
    )

    import inspect

    if inspect.isawaitable(stream):
        await stream
    elif inspect.isasyncgen(stream):
        async for _ in stream:  # drain
            pass
    elif inspect.isgenerator(stream):
        for _ in stream:
            pass

    return await agent.run(json_path)


async def main():
    base_files = [file for file in os.listdir("files_outputs")]

    for base_file in base_files:
        base_path = f"files_outputs/{base_file}/files_structure"
        paths = glob.glob(f"{base_path}/today_files/*_files.json")

        for p in paths:
            print(f"\n=== {p} ===")
            result = await run_direct(p)
            print(json.dumps(result["stats"], ensure_ascii=False, indent=2))

            base = f"files_outputs/{base_file}/files_cleaned/today_files/"
            os.makedirs(base, exist_ok=True)
            out_final = f"{base}{Path(p).stem}_cleaned.json"
            out_removed = f"{base}{Path(p).stem}_removed.json"
            out_harmless = f"{base}{Path(p).stem}_harmless.json"

            with open(out_final, "w", encoding="utf-8") as f:
                json.dump(
                    {"inferred_batch": result["final"]}, f, ensure_ascii=False, indent=2
                )
            with open(out_removed, "w", encoding="utf-8") as f:
                json.dump(
                    {"inferred_batch": result["removed"]},
                    f,
                    ensure_ascii=False,
                    indent=2,
                )
            with open(out_harmless, "w", encoding="utf-8") as f:
                json.dump(
                    {"inferred_batch": result["harmless"]},
                    f,
                    ensure_ascii=False,
                    indent=2,
                )

            print(f"Wrote:\n  {out_final}\n  {out_removed}\n  {out_harmless}")


if __name__ == "__main__":

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
