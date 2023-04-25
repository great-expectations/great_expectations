from typing import List

import great_expectations as gx
from great_expectations import DataContext
from great_expectations.core import ExpectationConfiguration
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.rule_based_profiler import RuleBasedProfilerResult
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler

yaml = YAMLHandler()

profiler_config = r"""
# <snippet name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py full profiler_config">
# <snippet name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py full row_count_rule">
# This profiler is meant to be used on the NYC taxi data (yellow_tripdata_sample_<YEAR>-<MONTH>.csv)
# located in tests/test_sets/taxi_yellow_tripdata_samples/

# <snippet name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py name and config_version">
name: My Profiler
config_version: 1.0
# </snippet>

# <snippet name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py variables and rule name">
variables:
  false_positive_rate: 0.01
  mostly: 1.0

rules:
  row_count_rule:
# </snippet>
# <snippet name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py row_count_rule domain_builder">
    domain_builder:
        class_name: TableDomainBuilder
# </snippet>
# <snippet name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py row_count_rule parameter_builders">
    parameter_builders:
      - name: row_count_range
        class_name: NumericMetricRangeMultiBatchParameterBuilder
        metric_name: table.row_count
        metric_domain_kwargs: $domain.domain_kwargs
        false_positive_rate: $variables.false_positive_rate
        truncate_values:
          lower_bound: 0
        round_decimals: 0
# </snippet>
# <snippet name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py row_count_rule expectation_configuration_builders">
    expectation_configuration_builders:
      - expectation_type: expect_table_row_count_to_be_between
        class_name: DefaultExpectationConfigurationBuilder
        module_name: great_expectations.rule_based_profiler.expectation_configuration_builder
        min_value: $parameter.row_count_range.value[0]
        max_value: $parameter.row_count_range.value[1]
        mostly: $variables.mostly
        meta:
          profiler_details: $parameter.row_count_range.details
# </snippet>
# </snippet>
# <snippet name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py column_ranges_rule domain_builder">
  column_ranges_rule:
    domain_builder:
      class_name: ColumnDomainBuilder
      include_semantic_types:
        - numeric
# </snippet>
# <snippet name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py column_ranges_rule parameter_builders">
    parameter_builders:
      - name: min_range
        class_name: NumericMetricRangeMultiBatchParameterBuilder
        metric_name: column.min
        metric_domain_kwargs: $domain.domain_kwargs
        false_positive_rate: $variables.false_positive_rate
        round_decimals: 2
      - name: max_range
        class_name: NumericMetricRangeMultiBatchParameterBuilder
        metric_name: column.max
        metric_domain_kwargs: $domain.domain_kwargs
        false_positive_rate: $variables.false_positive_rate
        round_decimals: 2
# </snippet>
# <snippet name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py column_ranges_rule expectation_configuration_builders">
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
# </snippet>
# </snippet>
"""


# <snippet name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py init">
context = gx.get_context()

context.sources.add_pandas_filesystem(
    "taxi_multi_batch_datasource",
    base_directory="./data",
).add_csv_asset(
    "all_years",
    batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
)

full_profiler_config_dict: dict = yaml.load(profiler_config)
# </snippet>

# Instantiate RuleBasedProfiler
# <snippet name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py instantiate">
full_profiler_config_dict: dict = yaml.load(profiler_config)

rule_based_profiler: RuleBasedProfiler = RuleBasedProfiler(
    name=full_profiler_config_dict["name"],
    config_version=full_profiler_config_dict["config_version"],
    rules=full_profiler_config_dict["rules"],
    variables=full_profiler_config_dict["variables"],
    data_context=context,
)
# </snippet>


# <snippet name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py run">
batch_request: dict = {
    "datasource_name": "taxi_multi_batch_datasource",
    "data_asset_name": "all_years",
    "options": {},
}

result: RuleBasedProfilerResult = rule_based_profiler.run(batch_request=batch_request)
# </snippet>

expectation_configurations: List[
    ExpectationConfiguration
] = result.expectation_configurations

print(expectation_configurations)

# Please note that this docstring is here to demonstrate output for docs. It is not needed for normal use.
# <snippet name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py row_count_rule_suite">
row_count_rule_suite = """
    {
        "meta": {"great_expectations_version": "0.16.7"},
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
# </snippet>
