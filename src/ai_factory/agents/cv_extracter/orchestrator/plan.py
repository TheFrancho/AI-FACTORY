from google.adk.agents import SequentialAgent, ParallelAgent, Agent, LlmAgent

from ai_factory.agents.cv_extracter.extract_sections.agents import (
    cv_text_splitter_agent,
)
from ai_factory.agents.cv_extracter.extract_title.agents import cv_title_pattern_agent
from ai_factory.agents.cv_extracter.extract_filename_pattern.agents import (
    cv_filename_pattern_agent,
)


def build_overall_workflow() -> SequentialAgent:
    """
    Build the graph once:
      1) cv_text_splitter_agent
      2) Parallel( title_agent_from_state, filename_agent_from_state )
    IMPORTANT: no module-level construction; this function returns a fresh graph.
    """

    title_agent_from_state: Agent = LlmAgent(
        model=cv_title_pattern_agent.model,
        name="title_section_processer_flow",
        instruction=(
            "Use ONLY this input:\n\n"
            "{split_sections.markdown_title_section}\n\n"
            "Then follow these rules and return the JSON exactly as specified:\n\n"
        )
        + "\n"
        + cv_title_pattern_agent.instruction,
        description=cv_title_pattern_agent.description,
        output_schema=cv_title_pattern_agent.output_schema,
        output_key=cv_title_pattern_agent.output_key,
        tools=getattr(cv_title_pattern_agent, "tools", None),
    )

    filename_agent_from_state: Agent = LlmAgent(
        model=cv_filename_pattern_agent.model,
        name="filename_pattern_section_processer_flow",
        instruction=(
            "Use ONLY this input:\n\n"
            "{split_sections.filename_pattern_section}\n\n"
            "Then follow these rules and return the JSON exactly as specified:\n\n"
        )
        + cv_filename_pattern_agent.instruction,
        description=cv_filename_pattern_agent.description,
        output_schema=cv_filename_pattern_agent.output_schema,
        output_key=cv_filename_pattern_agent.output_key,
        tools=getattr(cv_filename_pattern_agent, "tools", None),
    )

    section_formatter_agent = ParallelAgent(
        name="ConcurrentFetch",
        sub_agents=[title_agent_from_state, filename_agent_from_state],
    )

    return SequentialAgent(
        name="FetchAndSynthesize",
        sub_agents=[cv_text_splitter_agent, section_formatter_agent],
    )
