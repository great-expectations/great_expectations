"""
TODO
"""

import logging
from unittest import mock

import pytest

from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)

logger = logging.getLogger(__name__)


@pytest.fixture
def enable_usage_stats(monkeypatch):
    monkeypatch.delenv("GE_USAGE_STATS")


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_all_relevant_context_methods_emit_usage_stats(
    mock_emit: mock.MagicMock,
    enable_usage_stats,  # Needs to be before context fixtures to ensure usage stats handlers are attached
    in_memory_runtime_context: EphemeralDataContext,
    empty_data_context: FileDataContext,
    empty_data_context_in_cloud_mode: CloudDataContext,
):
    contexts = (
        in_memory_runtime_context,
        empty_data_context,
        empty_data_context_in_cloud_mode,
    )

    methods_and_events = (
        (
            "save_expectation_suite",
            UsageStatsEvents.DATA_CONTEXT_SAVE_EXPECTATION_SUITE,
        ),
        ("add_datasource", UsageStatsEvents.DATA_CONTEXT_ADD_DATASOURCE),
        ("run_checkpoint", UsageStatsEvents.DATA_CONTEXT_RUN_CHECKPOINT),
        ("get_batch_list", UsageStatsEvents.DATA_CONTEXT_GET_BATCH_LIST),
        (
            "run_profiler_with_dynamic_arguments",
            UsageStatsEvents.DATA_CONTEXT_RUN_RULE_BASED_PROFILER_WITH_DYNAMIC_ARGUMENTS,
        ),
        (
            "run_profiler_on_data",
            UsageStatsEvents.DATA_CONTEXT_RUN_RULE_BASED_PROFILER_ON_DATA,
        ),
        (
            "run_validation_operator",
            UsageStatsEvents.DATA_CONTEXT_RUN_VALIDATION_OPERATOR,
        ),
        ("open_data_docs", UsageStatsEvents.DATA_CONTEXT_OPEN_DATA_DOCS),
        ("build_data_docs", UsageStatsEvents.DATA_CONTEXT_BUILD_DATA_DOCS),
    )

    for context in contexts:
        for (method_name, expected_event) in methods_and_events:
            logger.info(f"Testing {context.__class__}.{method_name}")

            try:
                method = getattr(context, method_name)
                method()
            except Exception:
                pass

            assert mock_emit.call_args_list[-1].args[0]["event"] == expected_event
