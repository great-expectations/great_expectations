from typing import Dict, List

import pytest
from freezegun import freeze_time
from ruamel.yaml import YAML

from great_expectations.core import ExpectationConfiguration, ExpectationSuite

# TODO: Move these fixtures to integration tests
from great_expectations.data_context.util import file_relative_path


@pytest.fixture
@freeze_time("09/26/2019 13:42:41")
def alice_columnar_table_single_batch():
    """
    About the "Alice" User Workflow Fixture

    Alice has a single table of columnar data called user_events (DataAsset) that she wants to check periodically as new data is added.

      - She knows what some of the columns mean, but not all - and there are MANY of them (only a subset currently shown in examples and fixtures).

      - She has organized other tables similarly so that for example column name suffixes indicate which are for user ids (_id) and which timestamps are for versioning (_ts).

    She wants to use a configurable profiler to generate a description (ExpectationSuite) about the table so that she can:

        1. use it to validate the user_events table periodically and set up alerts for when things change

        2. have a place to add her domain knowledge of the data (that can also be validated against new data)

        3. if all goes well, generalize some of the Profiler to use on her other tables

    Alice configures her Profiler using the yaml configurations and data file locations captured in this fixture.
    """

    verbose_profiler_config_file_path: str = file_relative_path(
        __file__, "alice_user_workflow_verbose_profiler_config.yml"
    )
    verbose_profiler_config: str
    with open(verbose_profiler_config_file_path) as f:
        verbose_profiler_config = f.read()

    my_rule_for_user_ids_expectation_configurations: List[ExpectationConfiguration] = [
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
    ]

    event_ts_column_data: Dict[str, str] = {
        "column_name": "event_ts",
        "observed_max_time_str": "2004-10-19 11:05:20",
    }

    my_rule_for_timestamps_column_data: List[Dict[str, str]] = [
        event_ts_column_data,
        {
            "column_name": "server_ts",
            "observed_max_time_str": "2004-10-19 11:05:20",
        },
        {
            "column_name": "device_ts",
            "observed_max_time_str": "2004-10-19 11:05:22",
        },
    ]
    my_rule_for_timestamps_expectation_configurations: List[
        ExpectationConfiguration
    ] = []
    column_data: Dict[str, str]
    for column_data in my_rule_for_timestamps_column_data:
        my_rule_for_timestamps_expectation_configurations.extend(
            [
                ExpectationConfiguration(
                    **{
                        "expectation_type": "expect_column_values_to_be_of_type",
                        "kwargs": {
                            "column": column_data["column_name"],
                            "type_": "TIMESTAMP",
                        },
                        "meta": {},
                    }
                ),
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
                        "meta": {
                            "notes": {
                                "format": "markdown",
                                "content": [
                                    "### This expectation confirms no events occur before tracking started **2004-10-19 10:23:54**"
                                ],
                            }
                        },
                    }
                ),
                ExpectationConfiguration(
                    **{
                        "expectation_type": "expect_column_max_to_be_between",
                        "kwargs": {
                            "column": column_data["column_name"],
                            "min_value": "2004-10-19T10:23:54",  # From variables
                            "max_value": event_ts_column_data[
                                "observed_max_time_str"
                            ],  # Pin to event_ts column
                        },
                        "meta": {
                            "notes": {
                                "format": "markdown",
                                "content": [
                                    "### This expectation confirms that the event_ts contains the latest timestamp of all domains"
                                ],
                            }
                        },
                    }
                ),
            ]
        )

    expectation_configurations: List[ExpectationConfiguration] = []

    expectation_configurations.extend(my_rule_for_user_ids_expectation_configurations)
    expectation_configurations.extend(my_rule_for_timestamps_expectation_configurations)

    expectation_suite_name: str = "alice_columnar_table_single_batch"
    expected_expectation_suite: ExpectationSuite = ExpectationSuite(
        expectation_suite_name=expectation_suite_name
    )
    expectation_configuration: ExpectationConfiguration
    for expectation_configuration in expectation_configurations:
        expected_expectation_suite.add_expectation(expectation_configuration)

    # NOTE that this expectation suite should fail when validated on the data in "sample_data_relative_path"
    # because the device_ts is ahead of the event_ts for the latest event
    sample_data_relative_path: str = "alice_columnar_table_single_batch_data.csv"

    yaml = YAML()
    profiler_config: dict = yaml.load(verbose_profiler_config)
    expected_expectation_suite.add_citation(
        comment="Suite created by Rule-Based Profiler with the configuration included.",
        profiler_config=profiler_config,
    )

    return {
        "profiler_config": verbose_profiler_config,
        "expected_expectation_suite_name": expectation_suite_name,
        "expected_expectation_suite": expected_expectation_suite,
        "sample_data_relative_path": sample_data_relative_path,
    }
