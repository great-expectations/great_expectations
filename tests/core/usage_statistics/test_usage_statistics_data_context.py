"""
TODO
"""
from __future__ import annotations

import ast
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
from great_expectations.data_context.util import file_relative_path

logger = logging.getLogger(__name__)


@pytest.fixture
def enable_usage_stats(monkeypatch):
    monkeypatch.delenv("GE_USAGE_STATS")


@pytest.fixture
def usage_stats_decorated_methods_on_abstract_data_context() -> list[str]:
    """
    Uses AST parsing to collect the names of all methods that are decorated with
    `usage_statistics_enabled_method`.

    This is used to ensure that any changes made to `AbstractDataContext` are automatically
    accounted for in our tests.
    """
    path_to_abstract_context = file_relative_path(
        __file__,
        "../../../great_expectations/data_context/data_context/abstract_data_context.py",
    )
    with open(path_to_abstract_context) as f:
        root = ast.parse(f.read())

    relevant_methods = []
    for node in ast.walk(root):
        if not isinstance(node, ast.FunctionDef):
            continue

        name = node.name
        # Ignore dunders like __init__
        if name.startswith("__") and name.endswith("__"):
            continue

        decorators = node.decorator_list
        for decorator in decorators:
            if isinstance(decorator, ast.Call):
                if decorator.func.id == "usage_statistics_enabled_method":
                    relevant_methods.append(name)
                    break

    return relevant_methods


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_all_relevant_context_methods_emit_usage_stats(
    mock_emit: mock.MagicMock,
    usage_stats_decorated_methods_on_abstract_data_context: list[str],
    enable_usage_stats,  # Needs to be before context fixtures to ensure usage stats handlers are attached
    in_memory_runtime_context: EphemeralDataContext,
    empty_data_context: FileDataContext,
    empty_data_context_in_cloud_mode: CloudDataContext,
):
    """
    What does this test and why?

    Ensures that all primary context types (EphemeralDataContext, FileDataContext, and CloudDataContext)
    emit the same usage stats events.

    This guards against the case where a child class overrides a method defined by AbstractDataContext
    but forgets to add the decorator, resulting in us losing event data.
    """

    relevant_methods = usage_stats_decorated_methods_on_abstract_data_context
    expected_events = (
        UsageStatsEvents.DATA_CONTEXT_SAVE_EXPECTATION_SUITE,
        UsageStatsEvents.DATA_CONTEXT_ADD_DATASOURCE,
        UsageStatsEvents.DATA_CONTEXT_RUN_CHECKPOINT,
        UsageStatsEvents.DATA_CONTEXT_GET_BATCH_LIST,
        UsageStatsEvents.DATA_CONTEXT_RUN_RULE_BASED_PROFILER_WITH_DYNAMIC_ARGUMENTS,
        UsageStatsEvents.DATA_CONTEXT_RUN_RULE_BASED_PROFILER_ON_DATA,
        UsageStatsEvents.DATA_CONTEXT_RUN_VALIDATION_OPERATOR,
        UsageStatsEvents.DATA_CONTEXT_OPEN_DATA_DOCS,
        UsageStatsEvents.DATA_CONTEXT_BUILD_DATA_DOCS,
    )
    assert len(relevant_methods) == len(
        expected_events
    ), "Please update the `expected_events` list to account for all usage stats decorated methods in AbstractDataContext"

    contexts = (
        in_memory_runtime_context,
        empty_data_context,
        empty_data_context_in_cloud_mode,
    )

    for context in contexts:
        for method_name, expected_event in zip(relevant_methods, expected_events):
            logger.info(f"Testing {context.__class__}.{method_name}")

            # As we only care about the decorator and not the underlying method being decorated,
            # we use the following try/except pattern. All method calls will generally fail due
            # to having no input args but we still manage to trigger our target decorator.
            try:
                method = getattr(context, method_name)
                method()
            except Exception:
                pass

            assert mock_emit.call_args_list[-1].args[0]["event"] == expected_event
