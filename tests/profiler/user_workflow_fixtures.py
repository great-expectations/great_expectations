from typing import List

import pytest

from great_expectations.core import ExpectationConfiguration, ExpectationSuite

# TODO: Move these fixtures to integration tests


@pytest.fixture
def alice_columnar_table_single_batch():

    with open("alice_user_workflow_verbose_profiler_config.yml") as f:
        verbose_profiler_config = f.read()

    # TODO: This "simplified" configuration has outstanding questions and proposed configurations that should be
    #  answered before it is considered to be a standard configuration.

    with open("alice_user_workflow_simplified_profiler_config.yml") as f:
        simplified_profiler_config = f.read()

    profiler_configs: List[str] = []
    profiler_configs.append(verbose_profiler_config)
    # profiler_configs.append(simplified_profiler_config)

    my_rule_for_user_ids_expectation_configurations = [
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "user_id",
                    "min_value": "19",  # From the data
                    "max_value": "999999999999",
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

    my_rule_for_timestamps_expectation_configurations: List[
        ExpectationConfiguration
    ] = []
    my_rule_for_timestamps_column_names: List[str] = [
        "event_ts",
        "server_ts",
        "device_ts",
    ]
    for my_rule_for_timestamps_column_name in my_rule_for_timestamps_column_names:
        my_rule_for_timestamps_expectation_configurations.extend(
            [
                # ExpectationConfiguration(
                #     **{
                #         "expectation_type": "expect_column_values_to_be_of_type",
                #         "kwargs": {
                #             "column": my_rule_for_timestamps_column_name,
                #             "type_": "TIMESTAMP",
                #         },
                #         "meta": {},
                #     }
                # ),
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
                # ExpectationConfiguration(
                #     **{
                #         "expectation_type": "expect_column_min_to_be_between",
                #         "kwargs": {
                #             "column": my_rule_for_timestamps_column_name,
                #             "min_value": "2004-10-19 10:23:54",  # From variables
                #             "max_value": "2004-10-19 10:23:54",  # From variables
                #         },
                #         "meta": {
                #             "format": "markdown",
                #             "content": [
                #                 "### This expectation confirms no events occur before tracking started **2004-10-19 10:23:54**"
                #             ],
                #         },
                #     }
                # ),
                # ExpectationConfiguration(
                #     **{
                #         "expectation_type": "expect_column_max_to_be_between",
                #         "kwargs": {
                #             "column": my_rule_for_timestamps_column_name,
                #             "min_value": "2004-10-19 10:23:54",  # From variables
                #             "max_value": "2004-10-19 11:05:20",  # From data
                #         },
                #         "meta": {
                #             "format": "markdown",
                #             "content": [
                #                 "### This expectation confirms that the event_ts contains the latest timestamp of all domains"
                #             ],
                #         },
                #     }
                # ),
            ]
        )

    expectation_configurations: List[ExpectationConfiguration] = []
    # expectation_configurations.extend(my_rule_for_user_ids_expectation_configurations)
    expectation_configurations.extend(my_rule_for_timestamps_expectation_configurations)

    # assert len(expectation_configurations) == 18

    expectation_suite_name: str = "alice_columnar_table_single_batch"
    expected_expectation_suite = ExpectationSuite(
        expectation_suite_name=expectation_suite_name
    )
    for expectation_configuration in expectation_configurations:
        expected_expectation_suite.add_expectation(expectation_configuration)

    # assert len(expected_expectation_suite.expectations) == 18

    # NOTE that this expectation suite will fail when validated on the data in sample_data_relative_path
    # because the device_ts is ahead of the event_ts for the latest event
    sample_data_relative_path: str = "alice_columnar_table_single_batch_data.csv"

    return {
        "profiler_configs": profiler_configs,
        "expected_expectation_suite_name": expectation_suite_name,
        "expected_expectation_suite": expected_expectation_suite,
        "sample_data_relative_path": sample_data_relative_path,
    }
