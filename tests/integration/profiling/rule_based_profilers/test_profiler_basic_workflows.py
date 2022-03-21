from typing import List

import pytest
from ruamel import yaml

from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import BatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context import DataContext
from great_expectations.data_context.util import file_relative_path
from great_expectations.rule_based_profiler.domain_builder import (
    ColumnDomainBuilder,
    DomainBuilder,
)
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    DefaultExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    MetricMultiBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.rule.rule import Rule
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler


@pytest.fixture
def data_context_with_taxi_data(empty_data_context):
    context: DataContext = empty_data_context

    # finding path to taxi_data relative to current test file
    data_path: str = file_relative_path(
        __file__, "../../../test_sets/taxi_yellow_tripdata_samples"
    )

    datasource_config = {
        "name": "taxi_multibatch_datasource_other_possibility",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "PandasExecutionEngine",
        },
        "data_connectors": {
            "default_inferred_data_connector_name": {
                "class_name": "InferredAssetFilesystemDataConnector",
                "base_directory": data_path,
                "default_regex": {
                    "group_names": ["data_asset_name", "month"],
                    "pattern": "(yellow_tripdata_sample_2018)-(\\d.*)\\.csv",
                },
            },
            "default_inferred_data_connector_name_all_years": {
                "class_name": "InferredAssetFilesystemDataConnector",
                "base_directory": data_path,
                "default_regex": {
                    "group_names": ["data_asset_name", "year", "month"],
                    "pattern": "(yellow_tripdata_sample)_(\\d.*)-(\\d.*)\\.csv",
                },
            },
        },
    }

    context.test_yaml_config(yaml.dump(datasource_config))
    context.add_datasource(**datasource_config)
    return context


def test_domain_builder(data_context_with_taxi_data):
    """
    What does this test and why?

    In the process of building a RuleBasedProfiler, one of the first components we want to build/test
    is DomainBuilder, which returns the domains (in this case columns of our data) that the profiler
    will be run on.  This test will ColumnDomainBuilder on the suffix "_amount", which
    returns 4 columns as the domain.
    """
    context: DataContext = data_context_with_taxi_data
    batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_multibatch_datasource_other_possibility",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="yellow_tripdata_sample_2018",
        data_connector_query={"index": -1},
    )
    domain_builder: DomainBuilder = ColumnDomainBuilder(
        data_context=context,
        batch_request=batch_request,
        include_column_name_suffixes=["_amount"],
    )
    domains: list = domain_builder.get_domains()
    assert len(domains) == 4
    assert domains == [
        {"domain_type": "column", "domain_kwargs": {"column": "fare_amount"}},
        {"domain_type": "column", "domain_kwargs": {"column": "tip_amount"}},
        {"domain_type": "column", "domain_kwargs": {"column": "tolls_amount"}},
        {"domain_type": "column", "domain_kwargs": {"column": "total_amount"}},
    ]


def test_add_rule_and_run_profiler(data_context_with_taxi_data):
    """
    What does this test and why?

    This is the first test where we build a Rule in memory and use the add_rule() method
    to add to our RuleBasedProfiler and run the profiler. We use the DomainBuilder from
    the previous test (against "_amount" columns) and an ExpectationConfigurationBuilder
    that uses expect_column_values_to_not_be_null because it only needs a domain value.

    The test eventually asserts that the profiler return 4 Expectations, one per column in
    our domain.
    """
    context: DataContext = data_context_with_taxi_data
    batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_multibatch_datasource_other_possibility",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="yellow_tripdata_sample_2018",
        data_connector_query={"index": -1},
    )
    domain_builder: DomainBuilder = ColumnDomainBuilder(
        data_context=context,
        batch_request=batch_request.to_json_dict(),
        include_column_name_suffixes=["_amount"],
    )
    default_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
        expectation_type="expect_column_values_to_not_be_null",
        column="$domain.domain_kwargs.column",
    )
    simple_rule: Rule = Rule(
        name="rule_with_no_variables_no_parameters",
        domain_builder=domain_builder,
        expectation_configuration_builders=[default_expectation_configuration_builder],
    )
    my_rbp: RuleBasedProfiler = RuleBasedProfiler(
        name="my_simple_rbp",
        config_version=1.0,
        data_context=context,
    )
    my_rbp.add_rule(rule=simple_rule)
    res: ExpectationSuite = my_rbp.run()
    assert len(res.expectations) == 4


def test_profiler_parameter_builder_added(data_context_with_taxi_data):
    """
    What does this test and why?

    This test now adds a simple ParameterBuilder to our Rule. More specifically,
    we use a MetricMultiBatchParameterBuilder to pass in the min_value parameter to
    expect_column_values_to_be_greater_than.
    """
    context: DataContext = data_context_with_taxi_data
    batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_multibatch_datasource_other_possibility",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="yellow_tripdata_sample_2018",
        data_connector_query={"index": -1},
    )
    domain_builder: DomainBuilder = ColumnDomainBuilder(
        data_context=context,
        batch_request=batch_request.to_json_dict(),
        include_column_name_suffixes=["_amount"],
    )
    # parameter_builder
    numeric_range_parameter_builder: MetricMultiBatchParameterBuilder = (
        MetricMultiBatchParameterBuilder(
            data_context=context,
            batch_request=batch_request.to_json_dict(),
            metric_name="column.min",
            metric_domain_kwargs="$domain.domain_kwargs",
            name="my_column_min",
        )
    )
    config_builder: DefaultExpectationConfigurationBuilder = (
        DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_values_to_be_greater_than",
            value="$parameter.my_column_min.value[-1]",
            column="$domain.domain_kwargs.column",
        )
    )
    simple_rule: Rule = Rule(
        name="rule_with_variables_and_parameters",
        domain_builder=domain_builder,
        parameter_builders=[numeric_range_parameter_builder],
        expectation_configuration_builders=[config_builder],
    )
    my_rbp = RuleBasedProfiler(
        name="my_rbp",
        config_version=1.0,
        data_context=context,
    )
    my_rbp.add_rule(rule=simple_rule)
    res: ExpectationSuite = my_rbp.run()
    assert len(res.expectations) == 4


def test_profiler_save_and_load(data_context_with_taxi_data):
    """
    What does this test and why?

    This tests whether context.save_profiler() can be invoked to update a profiler that lives in Store.
    The test ensures that any changes that we make to the Profiler, like adding a rule, will be persisted.

    The test tests that context.save_profiler() and context.get_profiler() return the expected RBP.
    """
    context: DataContext = data_context_with_taxi_data
    batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_multibatch_datasource_other_possibility",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="yellow_tripdata_sample_2018",
        data_connector_query={"index": -1},
    )
    domain_builder: DomainBuilder = ColumnDomainBuilder(
        data_context=context,
        batch_request=batch_request.to_json_dict(),
        include_column_name_suffixes=["_amount"],
    )
    # parameter_builder
    numeric_range_parameter_builder: MetricMultiBatchParameterBuilder = (
        MetricMultiBatchParameterBuilder(
            data_context=context,
            batch_request=batch_request.to_json_dict(),
            metric_name="column.min",
            metric_domain_kwargs="$domain.domain_kwargs",
            name="my_column_min",
        )
    )
    config_builder: DefaultExpectationConfigurationBuilder = (
        DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_values_to_be_greater_than",
            value="$parameter.my_column_min.value[-1]",
            column="$domain.domain_kwargs.column",
        )
    )
    simple_variables_rule: Rule = Rule(
        name="rule_with_no_variables_no_parameters",
        domain_builder=domain_builder,
        parameter_builders=[numeric_range_parameter_builder],
        expectation_configuration_builders=[config_builder],
    )
    my_rbp = RuleBasedProfiler(
        name="my_rbp",
        config_version=1.0,
        data_context=context,
    )
    res: dict = my_rbp.config.to_json_dict()
    assert res == {
        "config_version": 1.0,
        "name": "my_rbp",
        "rules": None,
        "variables": None,
    }
    my_rbp.add_rule(rule=simple_variables_rule)
    context.save_profiler(name="my_rbp", profiler=my_rbp)

    # load profiler from store
    my_loaded_profiler: RuleBasedProfiler = context.get_profiler(name="my_rbp")

    res = my_loaded_profiler.config.to_json_dict()
    assert res == {
        "variables": None,
        "rules": {
            "rule_with_no_variables_no_parameters": {
                "domain_builder": {
                    "module_name": "great_expectations.rule_based_profiler.domain_builder.column_domain_builder",
                    "class_name": "ColumnDomainBuilder",
                    "include_column_name_suffixes": ["_amount"],
                    "batch_request": {
                        "datasource_name": "taxi_multibatch_datasource_other_possibility",
                        "data_connector_name": "default_inferred_data_connector_name",
                        "data_asset_name": "yellow_tripdata_sample_2018",
                        "batch_spec_passthrough": None,
                        "data_connector_query": {"index": -1},
                        "limit": None,
                    },
                },
                "parameter_builders": [
                    {
                        "metric_domain_kwargs": "$domain.domain_kwargs",
                        "class_name": "MetricMultiBatchParameterBuilder",
                        "replace_nan_with_zero": False,
                        "batch_request": {
                            "datasource_name": "taxi_multibatch_datasource_other_possibility",
                            "data_connector_name": "default_inferred_data_connector_name",
                            "data_asset_name": "yellow_tripdata_sample_2018",
                            "batch_spec_passthrough": None,
                            "data_connector_query": {"index": -1},
                            "limit": None,
                        },
                        "enforce_numeric_metric": False,
                        "json_serialize": True,
                        "metric_name": "column.min",
                        "reduce_scalar_metric": True,
                        "module_name": "great_expectations.rule_based_profiler.parameter_builder.metric_multi_batch_parameter_builder",
                        "name": "my_column_min",
                    }
                ],
                "expectation_configuration_builders": [
                    {
                        "expectation_type": "expect_column_values_to_be_greater_than",
                        "class_name": "DefaultExpectationConfigurationBuilder",
                        "column": "$domain.domain_kwargs.column",
                        "meta": {},
                        "value": "$parameter.my_column_min.value[-1]",
                        "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder.default_expectation_configuration_builder",
                    }
                ],
            }
        },
        "config_version": 1.0,
        "name": "my_rbp",
    }


def test_profiler_run_with_expectation_suite_arg(
    data_context_with_taxi_data: DataContext, basic_expectation_suite: ExpectationSuite
):
    context: DataContext = data_context_with_taxi_data
    batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_multibatch_datasource_other_possibility",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="yellow_tripdata_sample_2018",
        data_connector_query={"index": -1},
    )
    domain_builder: DomainBuilder = ColumnDomainBuilder(
        data_context=context,
        batch_request=batch_request.to_json_dict(),
        include_column_name_suffixes=["_amount"],
    )
    default_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
        expectation_type="expect_column_values_to_not_be_null",
        column="$domain.domain_kwargs.column",
    )
    simple_rule: Rule = Rule(
        name="rule_with_no_variables_no_parameters",
        domain_builder=domain_builder,
        expectation_configuration_builders=[default_expectation_configuration_builder],
    )
    my_rbp: RuleBasedProfiler = RuleBasedProfiler(
        name="my_simple_rbp", data_context=context, config_version=1.0
    )
    my_rbp.add_rule(rule=simple_rule)

    existing_expectations: List[ExpectationConfiguration] = [
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "infinities"},
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist", kwargs={"column": "nulls"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist", kwargs={"column": "naturals"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_unique",
            kwargs={"column": "naturals"},
        ),
    ]

    assert len(basic_expectation_suite.expectations) == 4
    assert basic_expectation_suite.expectations == existing_expectations

    res: ExpectationSuite = my_rbp.run(expectation_suite=basic_expectation_suite)

    assert id(res) == id(basic_expectation_suite)
    assert len(res.expectations) == 8
    assert basic_expectation_suite.expectations[:4] == existing_expectations
