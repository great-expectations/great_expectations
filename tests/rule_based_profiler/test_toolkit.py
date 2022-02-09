from unittest import mock

import pandas as pd
import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context.store.profiler_store import ProfilerStore
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    GeCloudIdentifier,
)
from great_expectations.exceptions.exceptions import InvalidConfigError
from great_expectations.rule_based_profiler import RuleBasedProfiler
from great_expectations.rule_based_profiler.config import RuleBasedProfilerConfig
from great_expectations.rule_based_profiler.toolkit import (
    add_profiler,
    delete_profiler,
    get_profiler,
    list_profilers,
    run_profiler,
)


@mock.patch("great_expectations.rule_based_profiler.RuleBasedProfiler.run")
@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_run_profiler_without_dynamic_args(
    mock_data_context: mock.MagicMock,
    mock_profiler_run: mock.MagicMock,
    populated_profiler_store: ProfilerStore,
    profiler_name: str,
):
    run_profiler(
        data_context=mock_data_context,
        profiler_store=populated_profiler_store,
        name=profiler_name,
    )

    assert mock_profiler_run.called
    assert mock_profiler_run.call_args == mock.call(
        variables=None, rules=None, expectation_suite_name=None, include_citation=True
    )


@mock.patch("great_expectations.rule_based_profiler.RuleBasedProfiler.run")
@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_run_profiler_with_dynamic_args(
    mock_data_context: mock.MagicMock,
    mock_profiler_run: mock.MagicMock,
    populated_profiler_store: ProfilerStore,
    profiler_name: str,
):
    # Dynamic arguments used to override the profiler's attributes
    variables = {"foo": "bar"}
    rules = {"baz": "qux"}
    expectation_suite_name = "my_expectation_suite_name"
    include_citation = False

    run_profiler(
        data_context=mock_data_context,
        profiler_store=populated_profiler_store,
        name=profiler_name,
        variables=variables,
        rules=rules,
        expectation_suite_name=expectation_suite_name,
        include_citation=include_citation,
    )

    assert mock_profiler_run.called
    assert mock_profiler_run.call_args == mock.call(
        variables=variables,
        rules=rules,
        expectation_suite_name=expectation_suite_name,
        include_citation=include_citation,
    )


@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_get_profiler_with_too_many_args_raises_error(
    mock_data_context: mock.MagicMock,
    populated_profiler_store: ProfilerStore,
):
    with pytest.raises(AssertionError) as e:
        get_profiler(
            data_context=mock_data_context,
            profiler_store=populated_profiler_store,
            name="my_profiler",
            ge_cloud_id="my_ge_cloud_id",
        )

    assert "either name or ge_cloud_id" in str(e.value)


@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_add_profiler(
    mock_data_context: mock.MagicMock,
    profiler_key: ConfigurationIdentifier,
    profiler_config_with_placeholder_args: RuleBasedProfilerConfig,
):
    mock_data_context.ge_cloud_mode.return_value = False
    profiler = add_profiler(
        profiler_config_with_placeholder_args,
        data_context=mock_data_context,
        profiler_store=mock_data_context.profiler_store,
    )

    assert isinstance(profiler, RuleBasedProfiler)
    assert profiler.name == profiler_config_with_placeholder_args.name
    assert mock_data_context.profiler_store.set.call_args == mock.call(
        key=profiler_key, value=profiler_config_with_placeholder_args
    )


@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_add_profiler_ge_cloud_mode(
    mock_data_context: mock.MagicMock,
    ge_cloud_profiler_id: str,
    ge_cloud_profiler_key: GeCloudIdentifier,
    profiler_config_with_placeholder_args: RuleBasedProfilerConfig,
):
    mock_data_context.ge_cloud_mode.return_value = True
    profiler = add_profiler(
        profiler_config_with_placeholder_args,
        data_context=mock_data_context,
        profiler_store=mock_data_context.profiler_store,
        ge_cloud_id=ge_cloud_profiler_id,
    )

    assert isinstance(profiler, RuleBasedProfiler)
    assert profiler.name == profiler_config_with_placeholder_args.name
    assert mock_data_context.profiler_store.set.call_args == mock.call(
        key=ge_cloud_profiler_key, value=profiler_config_with_placeholder_args
    )


@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_add_profiler_with_batch_request_containing_batch_data_raises_error(
    mock_data_context: mock.MagicMock,
):
    profiler_config = RuleBasedProfilerConfig(
        name="my_profiler_config",
        class_name="RuleBasedProfiler",
        module_name="great_expectations.rule_based_profiler",
        config_version=1.0,
        rules={
            "rule_1": {
                "domain_builder": {
                    "class_name": "TableDomainBuilder",
                    "batch_request": {
                        "runtime_parameters": {
                            "batch_data": pd.DataFrame()  # Cannot be serialized in store
                        }
                    },
                },
                "parameter_builders": [
                    {
                        "class_name": "MetricMultiBatchParameterBuilder",
                        "name": "my_parameter",
                        "metric_name": "my_metric",
                    },
                ],
                "expectation_configuration_builders": [
                    {
                        "class_name": "DefaultExpectationConfigurationBuilder",
                        "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                    },
                ],
            }
        },
    )

    with pytest.raises(InvalidConfigError) as e:
        add_profiler(
            profiler_config,
            data_context=mock_data_context,
            profiler_store=mock_data_context.profiler_store,
        )

    assert "batch_data found in batch_request" in str(e.value)


@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_get_profiler(
    mock_data_context: mock.MagicMock,
    populated_profiler_store: ProfilerStore,
    profiler_config_with_placeholder_args: RuleBasedProfilerConfig,
):
    with mock.patch(
        "great_expectations.data_context.store.profiler_store.ProfilerStore.get",
        return_value=profiler_config_with_placeholder_args,
    ):
        profiler = get_profiler(
            data_context=mock_data_context,
            profiler_store=populated_profiler_store,
            name="my_profiler",
            ge_cloud_id=None,
        )

    assert isinstance(profiler, RuleBasedProfiler)


@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_get_profiler_non_existent_profiler_raises_error(
    mock_data_context: mock.MagicMock, empty_profiler_store: ProfilerStore
):
    with pytest.raises(ge_exceptions.ProfilerNotFoundError) as e:
        get_profiler(
            data_context=mock_data_context,
            profiler_store=empty_profiler_store,
            name="my_profiler",
            ge_cloud_id=None,
        )

    assert "Non-existent Profiler" in str(e.value)


def test_delete_profiler(
    populated_profiler_store: ProfilerStore,
):
    with mock.patch(
        "great_expectations.data_context.store.profiler_store.ProfilerStore.remove_key",
    ) as mock_remove_key:
        delete_profiler(
            profiler_store=populated_profiler_store,
            name="my_profiler",
            ge_cloud_id=None,
        )

    assert mock_remove_key.call_count == 1
    assert mock_remove_key.call_args == mock.call(
        key=ConfigurationIdentifier("my_profiler")
    )


def test_delete_profiler_with_too_many_args_raises_error(
    populated_profiler_store: ProfilerStore,
):
    with pytest.raises(AssertionError) as e:
        delete_profiler(
            profiler_store=populated_profiler_store,
            name="my_profiler",
            ge_cloud_id="my_ge_cloud_id",
        )

    assert "either name or ge_cloud_id" in str(e.value)


def test_delete_profiler_non_existent_profiler_raises_error(
    populated_profiler_store: ProfilerStore,
):
    with pytest.raises(ge_exceptions.ProfilerNotFoundError) as e:
        delete_profiler(
            profiler_store=populated_profiler_store,
            name="my_non_existent_profiler",
            ge_cloud_id=None,
        )

    assert "Non-existent Profiler" in str(e.value)


@mock.patch("great_expectations.data_context.store.ProfilerStore")
def test_list_profilers(mock_profiler_store: mock.MagicMock):
    store = mock_profiler_store()
    keys = ["a", "b", "c"]
    store.list_keys.return_value = [ConfigurationIdentifier(char) for char in keys]
    res = list_profilers(store, ge_cloud_mode=False)

    assert res == keys
    assert store.list_keys.called


@mock.patch("great_expectations.data_context.store.ProfilerStore")
def test_list_profilers_in_cloud_mode(mock_profiler_store: mock.MagicMock):
    store = mock_profiler_store()
    keys = ["a", "b", "c"]
    store.list_keys.return_value = keys
    res = list_profilers(store, ge_cloud_mode=True)

    assert res == keys
    assert store.list_keys.called
