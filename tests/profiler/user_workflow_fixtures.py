from typing import List

import pytest

from great_expectations.core import ExpectationConfiguration, ExpectationSuite

# TODO: Move these fixtures to integration tests

@pytest.fixture(scope="module")
def alice_columnar_table_single_batch():

    verbose_profiler_config = """
variables:
  max_user_id: 999999999999
  min_timestamp: 2004-10-19 10:23:54
rules:
  my_rule_for_user_ids:
    domain_builder:
      class_name: MyCustomSemanticTypeColumnDomainBuilder
      module_name: plugins.profiler.domain_builder.my_custom_semantic_type_column_domain_builder
      semantic_types:
        - user_id
    parameter_builders:
      - name: my_min_user_id
        class_name: MetricParameterBuilder
        metric_name: column.min
        metric_domain_kwargs: $domain.domain_kwargs
    expectation_configuration_builders:
      - expectation: expect_column_values_to_be_between
        min_value: $my_min_user_id.parameter.min_value
        max_value: $variables.max_user_id
        column: $domain.domain_kwargs.column
      - expectation: expect_column_values_to_not_be_null
        column: $domain.domain_kwargs.column
      - expectation: expect_column_values_to_be_of_type
        column: $domain.domain_kwargs.column
        type_: INTEGER
  my_rule_for_timestamps:
    domain_builder:
      class_name: SimpleColumnSuffixDomainBuilder
      column_name_suffixes:
        - _ts
      user_input_list_of_domain_names:
        - event_ts
        - server_ts
        - device_ts
    parameter_builders:
      - name: my_max_event_ts
        class_name: MetricParameterBuilder
        metric_name: column.max
        metric_domain_kwargs: $domain.event_ts.domain_kwargs
    expectation_configuration_builders:
      - expectation: expect_column_values_to_be_of_type
        column: $domain.domain_kwargs.column
        type_: TIMESTAMP
      - expectation: expect_column_values_to_be_increasing
        column: $domain.domain_kwargs.column
      - expectation: expect_column_values_to_be_dateutil_parseable
        column: $domain.domain_kwargs.column
      - expectation: expect_column_min_to_be_between
        column: $domain.domain_kwargs.column
        min_value: $variables.min_timestamp
        max_value: $variables.min_timestamp
        meta:
          notes:
            format: markdown
            content:
              - ### This expectation confirms no events occur before tracking started **2004-10-19 10:23:54**
      - expectation: expect_column_max_to_be_between
        column: $domain.domain_kwargs.column
        min_value: $variables.min_timestamp
        max_value: $my_max_event_ts.parameter.max_value
        meta:
          notes:
            format: markdown
            content:
              - ### This expectation confirms that the event_ts contains the latest timestamp of all domains
"""

    simplified_profiler_config = """
variables:
  max_user_id: 999999999999
  min_timestamp: 2004-10-19 10:23:54
rules:
  my_rule_for_user_ids:
    domain_builder:
      class_name: MyCustomSemanticTypeColumnDomainBuilder
      module_name: plugins.profiler.domain_builder.my_custom_semantic_type_column_domain_builder
      semantic_types:
        - user_id
    parameter_builders:
      - name: my_min_user_id
        class_name: MetricParameterBuilder
        metric_name: column.min
    expectation_configuration_builders:
      - expectation: expect_column_values_to_be_between
        min_value: $my_min_user_id.parameter.min_value
        max_value: $variables.max_user_id
      - expectation: expect_column_values_to_not_be_null
      - expectation: expect_column_values_to_be_of_type
        type_: INTEGER
  my_rule_for_timestamps:
    domain_builder:
      class_name: SimpleColumnSuffixDomainBuilder
      column_name_suffixes:
        - _ts
      user_input_list_of_domain_names:
        - event_ts
        - server_ts
        - device_ts
    parameter_builders:
      - name: my_max_event_ts
        class_name: MetricParameterBuilder
        metric_name: column.max
        metric_domain_kwargs: $domain.event_ts.domain_kwargs
    expectation_configuration_builders:
      - expectation: expect_column_values_to_be_of_type
        type_: TIMESTAMP
      - expectation: expect_column_values_to_be_increasing
      - expectation: expect_column_values_to_be_dateutil_parseable
      - expectation: expect_column_min_to_be_between
        min_value: $variables.min_timestamp
        max_value: $variables.min_timestamp
        meta:
          notes:
            format: markdown
            content:
              - ### This expectation confirms no events occur before tracking started **2004-10-19 10:23:54**
      - expectation: expect_column_max_to_be_between
        min_value: $variables.min_timestamp
        max_value: $my_max_event_ts.parameter.max_value
        meta:
          notes:
            format: markdown
            content:
              - ### This expectation confirms that the event_ts contains the latest timestamp of all domains
"""

    profiler_configs: List[str] = []
    profiler_configs.append(verbose_profiler_config)
    profiler_configs.append(simplified_profiler_config)

    my_rule_for_user_ids_expectation_configurations = [
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "user_id",
                    "min_value": "19", # From the data
                    "max_value": "999999999999"
                },
                "meta": {},
            }
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {
                    "column": "user_id",
                },
                "meta": {},
            }
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_values_to_be_of_type",
                "kwargs": {
                    "column": "user_id",
                    "type_": "INTEGER",
                },
                "meta": {},
            }
        ),
    ]

    my_rule_for_timestamps_expectation_configurations: List[ExpectationConfiguration] = []
    my_rule_for_timestamps_column_names: List[str] = ["event_ts", "server_ts", "device_ts"]
    for my_rule_for_timestamps_column_name in my_rule_for_timestamps_column_names:
        my_rule_for_timestamps_expectation_configurations.extend(
            [
                ExpectationConfiguration(
                    **{
                        "expectation_type": "expect_column_values_to_be_of_type",
                        "kwargs": {
                            "column": my_rule_for_timestamps_column_name,
                            "type_": "TIMESTAMP",
                        },
                        "meta": {},
                    }
                ),
                ExpectationConfiguration(
                    **{
                        "expectation_type": "expect_column_values_to_be_increasing",
                        "kwargs": {
                            "column": my_rule_for_timestamps_column_name,
                        },
                        "meta": {},
                    }
                ),
                ExpectationConfiguration(
                    **{
                        "expectation_type": "expect_column_values_to_be_dateutil_parseable",
                        "kwargs": {
                            "column": my_rule_for_timestamps_column_name,
                        },
                        "meta": {},
                    }
                ),
                ExpectationConfiguration(
                    **{
                        "expectation_type": "expect_column_min_to_be_between",
                        "kwargs": {
                            "column": my_rule_for_timestamps_column_name,
                            "min_value": "2004-10-19 10:23:54",  # From variables
                            "max_value": "2004-10-19 10:23:54",  # From variables
                        },
                        "meta": {
                            "format": "markdown",
                            "content": [
                                "### This expectation confirms no events occur before tracking started **2004-10-19 10:23:54**"
                            ]
                        },
                    }
                ),
                ExpectationConfiguration(
                    **{
                        "expectation_type": "expect_column_max_to_be_between",
                        "kwargs": {
                            "column": my_rule_for_timestamps_column_name,
                            "min_value": "2004-10-19 10:23:54",  # From variables
                            "max_value": "2004-10-19 11:05:20",  # From data
                        },
                        "meta": {
                            "format": "markdown",
                            "content": [
                                "### This expectation confirms that the event_ts contains the latest timestamp of all domains"
                            ]
                        },
                    }
                ),
            ]
        )

    expectation_configurations: List[ExpectationConfiguration] = my_rule_for_user_ids_expectation_configurations + my_rule_for_timestamps_expectation_configurations
    assert len(expectation_configurations) == 18

    expectation_suite_name: str = "alice_columnar_table_single_batch"
    expected_expectation_suite = ExpectationSuite(expectation_suite_name=expectation_suite_name)
    for expectation_configuration in expectation_configurations:
        expected_expectation_suite.add_expectation(expectation_configuration)

    assert len(expected_expectation_suite.expectations) == 18

    # NOTE that this expectation suite will fail when validated on the data in sample_data_relative_path
    # because the device_ts is ahead of the event_ts for the latest event
    sample_data_relative_path: str = "alice_columnar_table_single_batch_data.csv"

    return {
        "profiler_configs": profiler_configs,
        "expected_expectation_suite": expected_expectation_suite,
        "sample_data_relative_path": sample_data_relative_path,
    }

def test_fixture_generation(alice_columnar_table_single_batch):
    assert alice_columnar_table_single_batch["sample_data_relative_path"] == "alice_columnar_table_single_batch_data.csv"
    assert len(alice_columnar_table_single_batch["profiler_configs"]) > 0
    for profiler_config in alice_columnar_table_single_batch["profiler_configs"]:
        assert len(profiler_config) > 0
        assert isinstance(profiler_config, str)
    assert isinstance(alice_columnar_table_single_batch["expected_expectation_suite"], ExpectationSuite)
    assert len(alice_columnar_table_single_batch["expected_expectation_suite"].expectations) == 18
