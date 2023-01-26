from __future__ import annotations

import logging
from unittest import mock

import pytest

from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.core.usage_statistics.usage_statistics import ENABLED_METHODS

logger = logging.getLogger(__name__)


@pytest.fixture
def enable_usage_stats(monkeypatch):
    monkeypatch.delenv("GE_USAGE_STATS")


@pytest.fixture
def usage_stats_decorated_methods_on_abstract_data_context() -> dict[
    str, UsageStatsEvents
]:
    return {
        k.split(".")[1]: v
        for k, v in ENABLED_METHODS.items()
        if k.startswith("AbstractDataContext") and not k.endswith("__init__")
    }


@pytest.mark.unit
def test_enabled_methods_map_to_appropriate_usage_stats_events(
    usage_stats_decorated_methods_on_abstract_data_context: dict[str, UsageStatsEvents]
):
    actual = usage_stats_decorated_methods_on_abstract_data_context
    expected = {
        "add_datasource": UsageStatsEvents.DATA_CONTEXT_ADD_DATASOURCE,
        "build_data_docs": UsageStatsEvents.DATA_CONTEXT_BUILD_DATA_DOCS,
        "get_batch_list": UsageStatsEvents.DATA_CONTEXT_GET_BATCH_LIST,
        "open_data_docs": UsageStatsEvents.DATA_CONTEXT_OPEN_DATA_DOCS,
        "run_checkpoint": UsageStatsEvents.DATA_CONTEXT_RUN_CHECKPOINT,
        "run_profiler_on_data": UsageStatsEvents.DATA_CONTEXT_RUN_RULE_BASED_PROFILER_ON_DATA,
        "run_profiler_with_dynamic_arguments": UsageStatsEvents.DATA_CONTEXT_RUN_RULE_BASED_PROFILER_WITH_DYNAMIC_ARGUMENTS,
        "run_validation_operator": UsageStatsEvents.DATA_CONTEXT_RUN_VALIDATION_OPERATOR,
        "save_expectation_suite": UsageStatsEvents.DATA_CONTEXT_SAVE_EXPECTATION_SUITE,
    }
    assert actual == expected


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@pytest.mark.parametrize(
    "data_context_fixture_name",
    [
        # In order to leverage existing fixtures in parametrization, we provide
        # their string names and dynamically retrieve them using pytest's built-in
        # `request` fixture.
        # Source: https://stackoverflow.com/a/64348247
        pytest.param("in_memory_runtime_context", id="EphemeralDataContext"),
        pytest.param("empty_data_context", id="FileDataContext"),
        pytest.param(
            "empty_data_context_in_cloud_mode",
            id="CloudDataContext",
        ),
    ],
)
@pytest.mark.integration
def test_all_relevant_context_methods_emit_usage_stats(
    mock_emit: mock.MagicMock,
    usage_stats_decorated_methods_on_abstract_data_context: dict[str, UsageStatsEvents],
    enable_usage_stats,  # Needs to be before context fixtures to ensure usage stats handlers are attached
    data_context_fixture_name: str,
    request,
):
    """
    What does this test and why?

    Ensures that all primary context types (EphemeralDataContext, FileDataContext, and CloudDataContext)
    emit the same usage stats events.

    This guards against the case where a child class overrides a method defined by AbstractDataContext
    but forgets to add the decorator, resulting in us losing event data.
    """
    context = request.getfixturevalue(data_context_fixture_name)

    relevant_methods = usage_stats_decorated_methods_on_abstract_data_context
    for method_name, expected_event in relevant_methods.items():
        logger.info(f"Testing {context.__class__}.{method_name}")

        # As we only care about the decorator and not the underlying method being decorated,
        # we use the following try/except pattern. All method calls will generally fail due
        # to having no input args but we still manage to trigger our target decorator.
        try:
            method = getattr(context, method_name)
            method()
        except Exception:
            pass

        mock_calls = mock_emit.call_args_list
        latest_call = mock_calls[-1]
        latest_event = latest_call.args[0]["event"]
        assert latest_event == expected_event
