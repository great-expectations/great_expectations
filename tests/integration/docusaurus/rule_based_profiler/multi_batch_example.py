import os
import shutil

import pytest
import yaml
from ruamel import yaml

import great_expectations as ge
from great_expectations import DataContext
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.data_context.util import file_relative_path
from great_expectations.marshmallow__shade.validate import Validator
from great_expectations.rule_based_profiler.profiler import Profiler
from tests.rule_based_profiler.test_profiler_user_workflows import (
    bobby_columnar_table_multi_batch_context,
)

profiler_config = """
# This profiler is meant to be used on the NYC taxi data (yellow_trip_data_sample_2019-*.csv)
variables:
  false_positive_rate: 1.0e-2
  mostly: 1.0

rules:
  row_count_rule:
    domain_builder:
        class_name: ActiveBatchTableDomainBuilder
    parameter_builders:
      - parameter_name: row_count_range
        class_name: NumericMetricRangeMultiBatchParameterBuilder
        batch_request:
            datasource_name: taxi_pandas
            data_connector_name: monthly
            data_asset_name: my_reports
            data_connector_query:
              batch_filter_parameters:
                year: "2020"
        metric_name: table.row_count
        metric_domain_kwargs: $domain.domain_kwargs
        false_positive_rate: $variables.false_positive_rate
        round_decimals: 0
        truncate_distribution:
          lower_bound: 0
    expectation_configuration_builders:
      - expectation_type: expect_table_row_count_to_be_between
        class_name: DefaultExpectationConfigurationBuilder
        module_name: great_expectations.rule_based_profiler.expectation_configuration_builder
        min_value: $parameter.row_count_range.value.min_value
        max_value: $parameter.row_count_range.value.max_value
        mostly: $variables.mostly
        meta:
          profiler_details: $parameter.row_count_range.details




"""

data_context = DataContext()


# Instantiate Profiler
full_profiler_config_dict: dict = yaml.load(profiler_config)
rules_configs: dict = full_profiler_config_dict.get("rules")
variables_configs: dict = full_profiler_config_dict.get("variables")

datasource_name: str = "taxi_pandas"
data_connector_name: str = "monthly"
data_asset_name: str = "my_reports"

validator: Validator = data_context.get_validator(
    datasource_name=datasource_name,
    data_connector_name=data_connector_name,
    data_asset_name=data_asset_name,
    data_connector_query={
        "index": -1,
    },
    create_expectation_suite_with_name="abc",
)

profiler: Profiler = Profiler(
    validator=validator,
    rules_configs=rules_configs,
    variables_configs=variables_configs,
    data_context=data_context,
)

suite = profiler.profile()
print(suite)
#
# expectation_suite: ExpectationSuite = profiler.profile(
#     expectation_suite_name=bobby_columnar_table_multi_batch[
#         "expected_expectation_suite_name"
#     ],
# )
#
# assert (
#     expectation_suite
#     == bobby_columnar_table_multi_batch["expected_expectation_suite"]
# )
