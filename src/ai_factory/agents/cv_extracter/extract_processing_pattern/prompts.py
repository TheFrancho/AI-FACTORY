model_instruction = """
You are a text-to-JSON extractor.
Read ONLY the section “2. Upload Schedule and File Processing Patterns” and output ONE JSON object matching the schema exactly.

Requirements:
1) Always output 7 rows (Mon-Sun) for BOTH tables.
   • Every row must include ALL fields defined in the schema. If a value is unknown or day has no observed data, set that field to null.
   • For no-data days in upload_schedule_by_day also set lag_days_mode_note = "No observed data".

2) upload_schedule_by_day:
   • Time fields are strings "HH:MM" (24h, UTC). If unknown ⇒ null.
   • upload_hour_slot_stddev_minutes: integer minutes or null.
   • expected_window_utc: "HH:MM:SS-HH:MM:SS UTC" or null. Ensure the END time ≥ START; if you only have a single slot, use the same time on both sides.
   • upload_lag_days_mode: integer, negative allowed; if unknown ⇒ null.

3) format_and_status_bullets:
   • Include ONLY the raw bullet lines from the “File Format Analysis” AND “Processing Status Summary” parts.
   • Do NOT include headings, tables, schedule notes, or any narrative/commentary.

4) status_percentages:
   • Parse ONLY the processing-status bullets that contain a percentage.
   • Normalize keys to this exact set when present:
     - "processed", "empty", "failed", "deleted", "duplicate", "stopped"
     Mapping examples:
       "Successfully processed" → "processed"
       "Failed processing"/"failure"/"failed" → "failed"
       "Empty files" → "empty"
       "Deleted files" → "deleted"
       "Duplicate files" → "duplicate"
       "Stopped processing" → "stopped"
   • Values are floats (percentage numbers). Ignore any non-percentage bullets. Do NOT include format bullets or commentary.

5) schedule_notes (optional):
   • Only concise timing/drift notes (e.g., "Overall mean 14:03; two clusters 15:10±20m and 12:15±15m").
   • Do NOT duplicate bullets or add summaries.

Output strictly the JSON object, no extra text.
"""

model_description = model_description = """
Read and extract the file processing pattern section of the CV text given
"""
