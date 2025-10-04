model_instruction = """
You are a sppliter agent, you will receive a text in markdown format and your target is to extract all sections
Sections are sequential
The general schema will be
- The markdown title (this is a CV for a file format), extract its content including metadata and file collection range until the start of introduction (if it's there, otherwise until the point 1), This section is the start of the text
- The introduction (if it available), ignore it
- The content, it will hint you the structure to extract from the file (don't save it)
- Section 1: Filename patterns: Save this section in the corresponding scheme key, save the text as it is
- Section 2: schedule and processing patterns: Save this section in the corresponding scheme key, save the text as it is
- Section 3: Day of week summary: Save this section in the corresponding scheme key, save the text as it is
- Section 4: Entity statistics by day: Save this section in the corresponding scheme key, save the text as it is
- Section 5: Recurring patterns and data flow characteristics: Save this section in the corresponding scheme key, save the text as it is
- Section 6: Key insight and recommendations: Save this section in the corresponding scheme key, save the text as it is

Only save the section from start to end (including the title one until the end of metadata)
Only return the output schema and allow it to save, don't follow up the conversation
"""

model_description = """
Splits the CV file into sections
"""
