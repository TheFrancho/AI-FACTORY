model_instruction = """
You will receive a document that may contain:
(a) a weekday summary table,
(b) an entity×weekday table, and/or
(c) short notes.

TASK
Extract ONLY what the document explicitly states and output ONE JSON object that conforms to the DayOfWeekPatternOutput schema (the caller provides it). Return JSON only—no extra text.

FLAVOR & PRESENCE
- flavor:
  • "weekday" if only a weekday table appears
  • "entity" if only an entity×weekday table appears
  • "both" if both appear
- presence:
  • has_weekday_table: true/false
  • has_entity_weekday_table: true/false
  • has_notes: true/false (true if any notes exist, either per-day or general)

WHAT TO EXTRACT

1) weekday  (list of PerWeekdayRow)
- Include one item per weekday that actually appears in the source (Mon/Tue/Wed/Thu/Fri/Sat/Sun). Do NOT fabricate missing days.
- For each day:
  • Always include these four keys (even if unknown): "rows", "empty_files", "duplicated_files", "failed_files".
    - If any of these four sections are missing in the source for that day, output an empty object {} for that StatBlock.
  • rows, empty_files, duplicated_files, failed_files → each is a StatBlock
    - Fill only the stats that are explicitly present (any of: min, max, mean, median, mode, stdev).
    - If the source shows a single unlabeled value for a metric group, put it in "mean".
    - If a section has no explicit stats for that day, leave it as {}.
  • analysis_note: copy a short note if the source gives a per-day comment; otherwise omit.

2) entity_weekday  (list of EntityWeekdayRow)
- Add one item for each (entity, day) pair shown in the entity×weekday table.
- entity: copy label exactly as written.
- day: Mon/Tue/Wed/Thu/Fri/Sat/Sun.
- Map a cell’s content to the closest field name. Use these defaults:
  • values about “…files…” → median_files
  • values about “…rows…”, “…row count…”, “…volume…” → median_rows
  • “…duplicated…” → median_duplicated
  • “…failed…” → median_failed
  • “…empty…” → median_empty
- If a value is blank/missing, just omit that field for the row.
- If the table shows a lag value, set mode_lag_days (integer). Otherwise omit.

3) general_notes
- Collect short document-level notes exactly or tightly paraphrased (bullets/sentences the doc states). If none, return [].

4) exceptions
- Only include explicit exceptions/anomalies mentioned in the text (e.g., “no uploads on Sundays”, “rare backfills”). Otherwise [].

5) column_audit
- columns_present: the header names exactly as they appear in the used table(s).
- columns_missing: if the document implies a canonical header that should exist but is absent, list it; otherwise [].
  Canonical weekday headers to consider:
    ["Day","Row Statistics","Empty Files Analysis","Duplicated Files","Failed Files","Processing Notes"]
  Canonical entity×weekday headers to consider:
    ["Entity","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
- extra_columns: any additional headers not in the canonical sets.

HEADER–STAT CONSISTENCY (must enforce)
- Only populate a StatBlock for duplicated_files or failed_files if the source table explicitly provides that metric (via a dedicated column or clearly labeled data cell).
- If any StatBlock (duplicated_files or failed_files) contains one or more numeric fields, the corresponding header name MUST appear in column_audit.columns_present.
- Weekday tables use the canonical header set:
  ["Day","Row Statistics","Empty Files Analysis","Duplicated Files","Failed Files","Processing Notes"].
  If the table does not contain "Duplicated Files" and/or "Failed Files" as columns, list each missing one in column_audit.columns_missing — even if the StatBlocks are {}.
- Do NOT infer headers from narrative text or analysis_note. Narrative mentions (e.g., “no duplicates on Fridays”) may be captured in analysis_note, but do not justify adding a header to columns_present or filling StatBlock fields.
- If a header exists but the cells are blank for some days, keep the header in columns_present and emit {} for those days’ StatBlocks (do not fabricate values).

NUMBER PARSING (make values processable)
- Do NOT “format” numbers. Just make them machine-parseable.
- Remove thousand separators (commas, thin spaces) and any whitespace inside numbers:
  e.g., "1,234,567" → 1234567 ; "12 345" → 12345
- Keep only digits, a leading minus sign if present, and a single decimal point.
- Do NOT use scientific notation; never emit exponents (e.g., 4.066461e+25). Use plain decimals only.
- Do NOT round or scale. If a compact suffix appears (k/M/B), drop the suffix and do not rescale.
- If a percentage appears, drop the % sign and keep the numeric part as-is (e.g., "84%" → 84.0). Do not divide by 100.
- If a cell is blank / “n/a” / “—”, leave the StatBlock field absent; for an entire StatBlock that’s missing data, output {}.
- Never add units or extra text to numbers.

DO NOTS
- Do not compute or derive new stats.
- Do not normalize wording beyond minimal, faithful paraphrase of notes.
- Do not add days, entities, or fields that aren’t shown.

MINIMAL MISSING-VALUE FIX (apply after extraction; do not alter any extracted values)
- For every item in weekday, guarantee the four keys exist: "rows", "empty_files", "duplicated_files", "failed_files".
  If any are missing, null, or not an object, set that key to {}.
- Do NOT synthesize numeric stats. If unknown, leave the StatBlock as {}.
- Do NOT modify flavor, presence, weekday day list/order, entity_weekday list/order, exceptions, general_notes, or column_audit.

OUTPUT
- Return exactly one JSON object that validates against DayOfWeekPatternOutput.
- No prose, no markdown, no comments—JSON only.
"""

model_description = """
Read and extract the day per week file upload characteristics section of the CV text given
"""
