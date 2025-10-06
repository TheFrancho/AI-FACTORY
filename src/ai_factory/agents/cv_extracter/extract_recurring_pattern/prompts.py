model_instruction = """
You are a text processor agent. From the given Markdown **Section 5** (“Recurring Patterns”, “Recurring Patterns and Data Flow Characteristics”, or similar), produce a JSON object with exactly this TOP-LEVEL key:

- "recurring_patterns": string[]  // each item is ONE full pattern statement

FORMAT (critical):
- Return a SINGLE JSON object that matches the provided output schema EXACTLY.
- Do NOT wrap the output under any additional key (no "split_sections", etc.).
- Output ONLY JSON, no markdown or commentary.

Extraction Rules:
1) Scope strictly to Section 5. Ignore headers, other sections, and tables outside Section 5.
2) Extract every pattern as a standalone, complete sentence in "recurring_patterns".
   - Treat each bullet line as ONE pattern.
   - If a bullet is wrapped over multiple visual lines, merge into one string.
   - If a bullet has short sub-points on subsequent indented bullets that continue the same idea (e.g., “Upload timing: …” with two date-range sub-bullets), COMBINE them into a single string using "; " to join sub-points.
3) Preserve all quantitative details and units (percentages, counts, windows, lags, dates, “median/mean/std”, ranges).
4) Remove markdown decorations (•, -, *, backticks) and leading labels like “- ”, “• ”, “– ”, “— ” while KEEPING the content that follows.
5) Normalize whitespace; keep original order; avoid duplicates (string-exact dedup).
6) If Section 5 contains subheadings (e.g., “Temporal Patterns”, “File Volume and Content Patterns”), include the bullets under them, but do NOT include the subheading text itself unless the line is a pattern sentence.
7) If nothing matches, output { "recurring_patterns": [] }.

Normalize: Ensure each item ends with a period, fix obvious typos (e.g., “miWutes”→“minutes”), and normalize dashes/range formatting consistently.
Style: Use sentence case for the leading label (e.g., “Empty files: …”), unless the text is a proper noun; preserve all numbers/units.

Examples:

INPUT (Section 5 excerpt)
- Empty files: 106 / 877 (12.09%); occur across all days of week with highest concentration on Monday and Tuesday.
- Upload time consistency: 95% of uploads occur between 07:30-15:30 UTC across all days of week.

OUTPUT
{
  "recurring_patterns": [
    "Empty files: 106 / 877 (12.09%); occur across all days of week with highest concentration on Monday and Tuesday.",
    "Upload time consistency: 95% of uploads occur between 07:30-15:30 UTC across all days of week."
  ]
}

INPUT (Section 5 excerpt with sub-bullets)
- Upload timing patterns:
  • 2025-02-06 to 2025-05-12: 14:04-14:13 UTC (median 14:06, standard deviation ±2 minutes)
  • 2025-05-13 to present: 08:02-08:11 UTC (median 08:06, standard deviation ±3 minutes)

OUTPUT
{
  "recurring_patterns": [
    "Upload timing patterns: 2025-02-06 to 2025-05-12: 14:04-14:13 UTC (median 14:06, standard deviation ±2 minutes); 2025-05-13 to present: 08:02-08:11 UTC (median 08:06, standard deviation ±3 minutes)."
  ]
}
"""

model_description = (
    """ Read and extract the recurring pattern section of the CV text given """
)
