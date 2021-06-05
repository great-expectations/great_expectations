from typing import List

import pytest

from great_expectations.core import ExpectationConfiguration, ExpectationSuite

# TODO: Move these fixtures to integration tests
from great_expectations.data_context.util import file_relative_path


@pytest.fixture
def bobby_columnar_table_multi_batch():
    """
    # TODO: <Alex>ALEX -- Add DocString</Alex>
    """

    verbose_profiler_config_file_path: str = file_relative_path(
        __file__, "bobby_user_workflow_verbose_profiler_config.yml"
    )
    verbose_profiler_config: str
    with open(verbose_profiler_config_file_path) as f:
        verbose_profiler_config = f.read()

    my_row_count_rule_expectation_configurations: List[ExpectationConfiguration] = [
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {
                    "min_value": 6712,
                    "max_value": 9288,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "table.row_count",
                            "metric_domain_kwargs": {
                                "batch_id": "021563e94d7866f395288f6e306aed9b"
                            },
                            # TODO: <Alex>ALEX -- checking to see if removing the NULL keyes below with "filter_properties_dict" caused pandas == None</Alex>
                            "metric_value_kwargs": None,
                            "metric_dependencies": None
                            # TODO: <Alex>ALEX -- checking to see if removing the NULL keyes above with "filter_properties_dict" caused pandas == None</Alex>
                        }
                    }
                },
            }
        ),
    ]

    expectation_configurations: List[ExpectationConfiguration] = []

    expectation_configurations.extend(my_row_count_rule_expectation_configurations)

    expectation_suite_name: str = "bobby_columnar_table_multi_batch"
    expected_expectation_suite: ExpectationSuite = ExpectationSuite(
        expectation_suite_name=expectation_suite_name
    )
    expectation_configuration: ExpectationConfiguration
    for expectation_configuration in expectation_configurations:
        expected_expectation_suite.add_expectation(expectation_configuration)

    return {
        "profiler_config": verbose_profiler_config,
        "expected_expectation_suite_name": expectation_suite_name,
        "expected_expectation_suite": expected_expectation_suite,
    }
