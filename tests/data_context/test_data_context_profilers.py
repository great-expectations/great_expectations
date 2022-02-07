from unittest import mock

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context import DataContext
from great_expectations.marshmallow__shade import ValidationError
from great_expectations.rule_based_profiler.config import RuleBasedProfilerConfig
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler


def test_list_profilers_raises_configuration_error(empty_data_context: DataContext):
    with mock.patch(
        "great_expectations.data_context.DataContext.profiler_store",
    ) as mock_profiler_store:
        mock_profiler_store.__get__ = mock.Mock(return_value=None)
        with pytest.raises(ge_exceptions.StoreConfigurationError) as e:
            empty_data_context.list_profilers()

    assert "not a configured store" in str(e.value)


def test_add_profiler(
    empty_data_context: DataContext,
    profiler_config_with_placeholder_args: RuleBasedProfilerConfig,
):
    args = profiler_config_with_placeholder_args.to_json_dict()
    for attr in ("class_name", "module_name"):
        args.pop(attr, None)

    profiler = empty_data_context.add_profiler(**args)
    assert isinstance(profiler, RuleBasedProfiler)


def test_add_profiler_with_invalid_config_raises_error(
    empty_data_context: DataContext,
    profiler_config_with_placeholder_args: RuleBasedProfilerConfig,
):
    args = profiler_config_with_placeholder_args.to_json_dict()
    for attr in ("class_name", "module_name"):
        args.pop(attr, None)

    # Setting invalid configuration to check that it is caught by DataContext wrapper method
    args["config_version"] = -1

    with pytest.raises(ValidationError) as e:
        empty_data_context.add_profiler(**args)

    assert "config_version" in str(e.value)
