from typing import List

import pytest

from great_expectations.core.batch import BatchRequest
from great_expectations.core.domain import (
    INFERRED_SEMANTIC_TYPE_KEY,
    SemanticDomainTypes,
)
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.util import file_relative_path
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.rule_based_profiler import RuleBasedProfilerResult
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

yaml = YAMLHandler()


@pytest.fixture
def data_context_with_taxi_data(empty_data_context):
    context = empty_data_context

    # finding path to taxi_data relative to current test file
    data_path: str = file_relative_path(__file__, "../../../test_sets/taxi_yellow_tripdata_samples")

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


@pytest.mark.big
def test_domain_builder(data_context_with_taxi_data):
    """
    What does this test and why?

    In the process of building a RuleBasedProfiler, one of the first components we want to build/test
    is DomainBuilder, which returns the domains (in this case columns of our data) that the profiler
    will be run on.  This test will ColumnDomainBuilder on the suffix "_amount", which
    returns 4 columns as the domain.
    """  # noqa: E501
    context = data_context_with_taxi_data
    batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_multibatch_datasource_other_possibility",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="yellow_tripdata_sample_2018",
        data_connector_query={"index": -1},
    )
    domain_builder: DomainBuilder = ColumnDomainBuilder(
        include_column_name_suffixes=["_amount"],
        data_context=context,
    )
    domains: list = domain_builder.get_domains(rule_name="my_rule", batch_request=batch_request)
    assert len(domains) == 4
    assert domains == [
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "fare_amount",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "fare_amount": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "tip_amount",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "tip_amount": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "tolls_amount",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "tolls_amount": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "total_amount",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "total_amount": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
    ]


@pytest.mark.big
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
    context = data_context_with_taxi_data
    batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_multibatch_datasource_other_possibility",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="yellow_tripdata_sample_2018",
        data_connector_query={"index": -1},
    )
    domain_builder: DomainBuilder = ColumnDomainBuilder(
        include_column_name_suffixes=["_amount"],
        data_context=context,
    )
    default_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
        expectation_type="expect_column_values_to_not_be_null",
        column="$domain.domain_kwargs.column",
    )
    simple_rule = Rule(
        name="rule_with_no_variables_no_parameters",
        variables=None,
        domain_builder=domain_builder,
        expectation_configuration_builders=[default_expectation_configuration_builder],
    )
    my_rbp: RuleBasedProfiler = RuleBasedProfiler(
        name="my_simple_rbp",
        config_version=1.0,
        data_context=context,
    )
    my_rbp.add_rule(rule=simple_rule)
    result: RuleBasedProfilerResult = my_rbp.run(batch_request=batch_request)
    expectation_configurations: List[ExpectationConfiguration] = result.expectation_configurations
    assert len(expectation_configurations) == 4


@pytest.mark.big
def test_profiler_parameter_builder_added(data_context_with_taxi_data):
    """
    What does this test and why?

    This test now adds a simple ParameterBuilder to our Rule. More specifically,
    we use a MetricMultiBatchParameterBuilder to pass in the min_value parameter to
    "expect_column_values_to_be_between".
    """
    context = data_context_with_taxi_data
    batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_multibatch_datasource_other_possibility",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="yellow_tripdata_sample_2018",
        data_connector_query={"index": -1},
    )
    domain_builder: DomainBuilder = ColumnDomainBuilder(
        include_column_name_suffixes=["_amount"],
        data_context=context,
    )
    # parameter_builder
    numeric_range_parameter_builder: MetricMultiBatchParameterBuilder = (
        MetricMultiBatchParameterBuilder(
            data_context=context,
            metric_name="column.min",
            metric_domain_kwargs="$domain.domain_kwargs",
            name="my_column_min",
        )
    )
    config_builder: DefaultExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
        expectation_type="expect_column_values_to_be_between",
        min_value="$parameter.my_column_min.value[-1]",
        column="$domain.domain_kwargs.column",
    )
    simple_rule = Rule(
        name="rule_with_variables_and_parameters",
        variables=None,
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
    result: RuleBasedProfilerResult = my_rbp.run(batch_request=batch_request)
    expectation_configurations: List[ExpectationConfiguration] = result.expectation_configurations
    assert len(expectation_configurations) == 4
