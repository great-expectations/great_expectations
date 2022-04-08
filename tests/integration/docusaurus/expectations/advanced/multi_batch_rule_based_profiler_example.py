from ruamel import yaml

from great_expectations import DataContext
from great_expectations.core import ExpectationSuite
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler

profiler_config = r"""
# This profiler is meant to be used on the NYC taxi data (yellow_tripdata_sample_<YEAR>-<MONTH>.csv)
# located in tests/test_sets/taxi_yellow_tripdata_samples/

name: My Profiler
config_version: 1.0

variables:
  false_positive_rate: 0.01
  mostly: 1.0

rules:
  row_count_rule:
    domain_builder:
        class_name: TableDomainBuilder
    parameter_builders:
      - name: row_count_range
        class_name: NumericMetricRangeMultiBatchParameterBuilder
        batch_request:
            datasource_name: taxi_pandas
            data_connector_name: monthly
            data_asset_name: my_reports
            data_connector_query:
              index: "-6:-1"
        metric_name: table.row_count
        metric_domain_kwargs: $domain.domain_kwargs
        false_positive_rate: $variables.false_positive_rate
        round_decimals: 0
        truncate_values:
          lower_bound: 0
    expectation_configuration_builders:
      - expectation_type: expect_table_row_count_to_be_between
        class_name: DefaultExpectationConfigurationBuilder
        module_name: great_expectations.rule_based_profiler.expectation_configuration_builder
        min_value: $parameter.row_count_range.value[0]
        max_value: $parameter.row_count_range.value[1]
        mostly: $variables.mostly
        meta:
          profiler_details: $parameter.row_count_range.details
  column_ranges_rule:
    domain_builder:
      class_name: ColumnDomainBuilder
      include_semantic_types:
        - numeric
      # BatchRequest yielding exactly one batch (March, 2019 trip data)
      batch_request:
        datasource_name: taxi_pandas
        data_connector_name: monthly
        data_asset_name: my_reports
        data_connector_query:
          index: -1
    parameter_builders:
      - name: min_range
        class_name: NumericMetricRangeMultiBatchParameterBuilder
        batch_request:
            datasource_name: taxi_pandas
            data_connector_name: monthly
            data_asset_name: my_reports
            data_connector_query:
              index: "-6:-1"
        metric_name: column.min
        metric_domain_kwargs: $domain.domain_kwargs
        false_positive_rate: $variables.false_positive_rate
        round_decimals: 2
      - name: max_range
        class_name: NumericMetricRangeMultiBatchParameterBuilder
        batch_request:
            datasource_name: taxi_pandas
            data_connector_name: monthly
            data_asset_name: my_reports
            data_connector_query:
              index: "-6:-1"
        metric_name: column.max
        metric_domain_kwargs: $domain.domain_kwargs
        false_positive_rate: $variables.false_positive_rate
        round_decimals: 2
    expectation_configuration_builders:
      - expectation_type: expect_column_min_to_be_between
        class_name: DefaultExpectationConfigurationBuilder
        module_name: great_expectations.rule_based_profiler.expectation_configuration_builder
        column: $domain.domain_kwargs.column
        min_value: $parameter.min_range.value[0]
        max_value: $parameter.min_range.value[1]
        mostly: $variables.mostly
        meta:
          profiler_details: $parameter.min_range.details
      - expectation_type: expect_column_max_to_be_between
        class_name: DefaultExpectationConfigurationBuilder
        module_name: great_expectations.rule_based_profiler.expectation_configuration_builder
        column: $domain.domain_kwargs.column
        min_value: $parameter.max_range.value[0]
        max_value: $parameter.max_range.value[1]
        mostly: $variables.mostly
        meta:
          profiler_details: $parameter.max_range.details
"""

data_context = DataContext()

# Instantiate RuleBasedProfiler
full_profiler_config_dict: dict = yaml.load(profiler_config)
rule_based_profiler: RuleBasedProfiler = RuleBasedProfiler(
    name=full_profiler_config_dict["name"],
    config_version=full_profiler_config_dict["config_version"],
    rules=full_profiler_config_dict["rules"],
    variables=full_profiler_config_dict["variables"],
    data_context=data_context,
)

rule_based_profiler.run()
suite: ExpectationSuite = rule_based_profiler.get_expectation_suite(
    expectation_suite_name="test_suite_name"
)
print(suite)

# Please note that this docstring is here to demonstrate output for docs. It is not needed for normal use.
first_rule_suite = """
    {
        "meta": {"great_expectations_version": "0.13.19+58.gf8a650720.dirty"},
        "data_asset_type": None,
        "expectations": [
            {
                "kwargs": {"min_value": 10000, "max_value": 10000, "mostly": 1.0},
                "expectation_type": "expect_table_row_count_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "table.row_count",
                            "metric_domain_kwargs": {},
                        }
                    }
                },
            }
        ],
        "expectation_suite_name": "tmp_suite_Profiler_e66f7cbb",
    }
"""
