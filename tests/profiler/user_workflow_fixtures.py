import datetime
from typing import Dict, List

import pytest

from great_expectations.core import ExpectationConfiguration, ExpectationSuite

# TODO: Move these fixtures to integration tests
from great_expectations.data_context.util import file_relative_path


@pytest.fixture
def alice_columnar_table_single_batch():

    verbose_profiler_config_file_path: str = file_relative_path(
        __file__, "alice_user_workflow_verbose_profiler_config.yml"
    )
    with open(verbose_profiler_config_file_path) as f:
        verbose_profiler_config = f.read()

    # TODO: This "simplified" configuration has outstanding questions and proposed configurations that should be
    #  answered before it is considered to be a standard configuration.

    simplified_profiler_config_file_path: str = file_relative_path(
        __file__, "alice_user_workflow_simplified_profiler_config.yml"
    )
    with open(simplified_profiler_config_file_path) as f:
        simplified_profiler_config = f.read()

    profiler_configs: List[str] = []
    profiler_configs.append(verbose_profiler_config)
    # profiler_configs.append(simplified_profiler_config)

    my_rule_for_user_ids_expectation_configurations = [
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "min_value": 397433,  # From the data
                    "max_value": 999999999999,
                    "column": "user_id",
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
        # TODO: Enable this ExpectationConfiguration
        # ExpectationConfiguration(
        #     **{
        #         "expectation_type": "expect_column_values_to_be_of_type",
        #         "kwargs": {
        #             "column": "user_id",
        #             "type_": "INTEGER",
        #         },
        #         "meta": {},
        #     }
        # ),
    ]

    my_rule_for_timestamps_expectation_configurations: List[
        ExpectationConfiguration
    ] = []
    my_rule_for_timestamps_column_data: List[Dict[str, str]] = [
        {
            "column_name": "event_ts",
            "observed_max_time_str": "2004-10-19 11:05:20",
        },
        {
            "column_name": "server_ts",
            "observed_max_time_str": "2004-10-19 11:05:20",
        },
        {
            "column_name": "device_ts",
            "observed_max_time_str": "2004-10-19 11:05:22",
        },
    ]
    for column_data in my_rule_for_timestamps_column_data:
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
                            "column": column_data["column_name"],
                        },
                        "meta": {},
                    }
                ),
                ExpectationConfiguration(
                    **{
                        "expectation_type": "expect_column_values_to_be_dateutil_parseable",
                        "kwargs": {
                            "column": column_data["column_name"],
                        },
                        "meta": {},
                    }
                ),
                ExpectationConfiguration(
                    **{
                        "expectation_type": "expect_column_min_to_be_between",
                        "kwargs": {
                            "column": column_data["column_name"],
                            "min_value": "2004-10-19T10:23:54",  # From variables
                            "max_value": "2004-10-19T10:23:54",  # From variables
                        },
                        "meta": {},
                        # TODO: meta field handling is not yet working
                        # "meta": {
                        #     "format": "markdown",
                        #     "content": [
                        #         "### This expectation confirms no events occur before tracking started **2004-10-19 10:23:54**"
                        #     ],
                        # },
                    }
                ),
                ExpectationConfiguration(
                    **{
                        "expectation_type": "expect_column_max_to_be_between",
                        "kwargs": {
                            "column": column_data["column_name"],
                            "min_value": "2004-10-19T10:23:54",  # From variables
                            "max_value": column_data[
                                "observed_max_time_str"
                            ],  # From data
                        },
                        "meta": {},
                        # TODO: meta field handling is not yet working
                        # "meta": {
                        #     "format": "markdown",
                        #     "content": [
                        #         "### This expectation should be replaced with the below expectation"
                        #     ],
                        # },
                    }
                ),
            ]
        )

    expectation_configurations: List[ExpectationConfiguration] = []
    expectation_configurations.extend(my_rule_for_user_ids_expectation_configurations)
    expectation_configurations.extend(my_rule_for_timestamps_expectation_configurations)
    #
    # # assert len(expectation_configurations) == 18

    expectation_suite_name: str = "alice_columnar_table_single_batch"
    expected_expectation_suite = ExpectationSuite(
        expectation_suite_name=expectation_suite_name
    )
    for expectation_configuration in expectation_configurations:
        expected_expectation_suite.add_expectation(expectation_configuration)

    # TODO: <Alex>ALEX</Alex>
    # assert len(expected_expectation_suite.expectations) == 18

    # NOTE that this expectation suite should fail when validated on the data in sample_data_relative_path
    # because the device_ts is ahead of the event_ts for the latest event
    sample_data_relative_path: str = "alice_columnar_table_single_batch_data.csv"

    return {
        "profiler_configs": profiler_configs,
        "expected_expectation_suite_name": expectation_suite_name,
        "expected_expectation_suite": expected_expectation_suite,
        "sample_data_relative_path": sample_data_relative_path,
    }
