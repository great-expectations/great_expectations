import pytest

# TODO: Move these fixtures to integration tests
from great_expectations.data_context.util import file_relative_path


@pytest.fixture
def bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000():
    verbose_profiler_config_file_path: str = file_relative_path(
        __file__, "bobster_user_workflow_verbose_profiler_config.yml"
    )
    verbose_profiler_config: str
    with open(verbose_profiler_config_file_path) as f:
        verbose_profiler_config = f.read()

    expectation_suite_name_bootstrap_sampling_method: str = (
        "bobby_columnar_table_multi_batch_bootstrap_sampling_method"
    )

    my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_mean_value: int = (
        5000
    )
    my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_std_value: float = (
        1.0e3
    )
    my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_num_stds: float = (
        3.00
    )

    my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_min_value_mean_value: int = round(
        float(
            my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_mean_value
        )
        - (
            my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_num_stds
            * my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_std_value
        )
    )

    my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_max_value_mean_value: int = round(
        float(
            my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_mean_value
        )
        + (
            my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_num_stds
            * my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_std_value
        )
    )

    return {
        "profiler_config": verbose_profiler_config,
        "test_configuration_bootstrap_sampling_method": {
            "expectation_suite_name": expectation_suite_name_bootstrap_sampling_method,
            "expect_table_row_count_to_be_between_mean_value": my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_mean_value,
            "expect_table_row_count_to_be_between_min_value_mean_value": my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_min_value_mean_value,
            "expect_table_row_count_to_be_between_max_value_mean_value": my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_max_value_mean_value,
        },
    }
