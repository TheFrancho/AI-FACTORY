from google.adk.agents import SequentialAgent, ParallelAgent, Agent, LlmAgent

from ai_factory.agents.cv_extracter.extract_sections.agents import (
    make_cv_text_splitter_agent,
)
from ai_factory.agents.cv_extracter.extract_title.agents import (
    make_cv_title_pattern_agent,
)
from ai_factory.agents.cv_extracter.extract_filename_pattern.agents import (
    make_cv_filename_pattern_agent,
)

from ai_factory.agents.cv_extracter.extract_processing_pattern.agents import (
    make_cv_file_processing_pattern_agent,
)
from ai_factory.agents.cv_extracter.extract_volume_characteristics.agents import (
    make_cv_volume_characteristics_agent,
)
from ai_factory.agents.cv_extracter.extract_day_of_week_pattern.agents import (
    make_cv_day_of_week_pattern_agent,
)
from ai_factory.agents.cv_extracter.extract_recurring_pattern.agents import (
    make_cv_recurring_pattern_agent,
)
from ai_factory.agents.cv_extracter.extract_comments_for_analyst.agents import (
    make_cv_comments_for_analyst_agent,
)


def build_overall_workflow() -> SequentialAgent:
    """
    Build the graph once:
      1) cv_text_splitter_agent
      2) Parallel( title_agent_from_state, filename_agent_from_state )
    IMPORTANT: no module-level construction; this function returns a fresh graph.
    """

    cv_text_splitter_agent = make_cv_text_splitter_agent()

    cv_title_pattern_agent = make_cv_title_pattern_agent()
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

    cv_filename_pattern_agent = make_cv_filename_pattern_agent()
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

    cv_file_processing_pattern_agent = make_cv_file_processing_pattern_agent()
    file_processing_agent_from_state: Agent = LlmAgent(
        model=cv_file_processing_pattern_agent.model,
        name="file_processing_section_flow",
        instruction=(
            "Use ONLY this input:\n\n"
            "{split_sections.file_processing_pattern_section}\n\n"
            "Then follow these rules and return the JSON exactly as specified:\n\n"
        )
        + cv_file_processing_pattern_agent.instruction,
        description=cv_file_processing_pattern_agent.description,
        output_schema=cv_file_processing_pattern_agent.output_schema,
        output_key=cv_file_processing_pattern_agent.output_key,
        tools=getattr(cv_file_processing_pattern_agent, "tools", None),
    )

    cv_volume_characteristics_agent = make_cv_volume_characteristics_agent()
    volume_characteristics_agent_from_state: Agent = LlmAgent(
        model=cv_volume_characteristics_agent.model,
        name="volume_characteristics_section_flow",
        instruction=(
            "Use ONLY this input:\n\n"
            "{split_sections.volume_characteristics_section}\n\n"
            "Then follow these rules and return the JSON exactly as specified:\n\n"
        )
        + cv_volume_characteristics_agent.instruction,
        description=cv_volume_characteristics_agent.description,
        output_schema=cv_volume_characteristics_agent.output_schema,
        output_key=cv_volume_characteristics_agent.output_key,
        tools=getattr(cv_volume_characteristics_agent, "tools", None),
    )

    cv_day_of_week_pattern_agent = make_cv_day_of_week_pattern_agent()
    day_of_week_pattern_agent_from_state: Agent = LlmAgent(
        model=cv_day_of_week_pattern_agent.model,
        name="day_of_week_pattern_section_flow",
        instruction=(
            "Use ONLY this input:\n\n"
            "{split_sections.day_of_week_section_pattern}\n\n"
            "Then follow these rules and return the JSON exactly as specified:\n\n"
        )
        + cv_day_of_week_pattern_agent.instruction,
        description=cv_day_of_week_pattern_agent.description,
        output_schema=cv_day_of_week_pattern_agent.output_schema,
        output_key=cv_day_of_week_pattern_agent.output_key,
        tools=getattr(cv_day_of_week_pattern_agent, "tools", None),
    )

    cv_recurring_pattern_agent = make_cv_recurring_pattern_agent()
    recurring_pattern_agent_from_state: Agent = LlmAgent(
        model=cv_recurring_pattern_agent.model,
        name="recurring_pattern_section_flow",
        instruction=(
            "Use ONLY this input:\n\n"
            "{split_sections.recurring_patterns_section}\n\n"
            "Then follow these rules and return the JSON exactly as specified:\n\n"
        )
        + cv_recurring_pattern_agent.instruction,
        description=cv_recurring_pattern_agent.description,
        output_schema=cv_recurring_pattern_agent.output_schema,
        output_key=cv_recurring_pattern_agent.output_key,
        tools=getattr(cv_recurring_pattern_agent, "tools", None),
    )

    cv_comments_for_analyst_agent = make_cv_comments_for_analyst_agent()
    comments_for_analyst_agent_from_state: Agent = LlmAgent(
        model=cv_comments_for_analyst_agent.model,
        name="cpmments_for_analyst_section_flow",
        instruction=(
            "Use ONLY this input:\n\n"
            "{split_sections.comments_for_analyst_section}\n\n"
            "Then follow these rules and return the JSON exactly as specified:\n\n"
        )
        + cv_comments_for_analyst_agent.instruction,
        description=cv_comments_for_analyst_agent.description,
        output_schema=cv_comments_for_analyst_agent.output_schema,
        output_key=cv_comments_for_analyst_agent.output_key,
        tools=getattr(cv_comments_for_analyst_agent, "tools", None),
    )

    section_formatter_agent = ParallelAgent(
        name="ConcurrentFetch",
        sub_agents=[
            title_agent_from_state,
            filename_agent_from_state,
            file_processing_agent_from_state,
            volume_characteristics_agent_from_state,
            day_of_week_pattern_agent_from_state,
            recurring_pattern_agent_from_state,
            comments_for_analyst_agent_from_state,
        ],
    )

    return SequentialAgent(
        name="FetchAndSynthesize",
        sub_agents=[cv_text_splitter_agent, section_formatter_agent],
    )
