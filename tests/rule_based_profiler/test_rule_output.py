from typing import Any, Dict, List

from great_expectations.core import ExpectationConfiguration
from great_expectations.rule_based_profiler.rule import RuleOutput
from great_expectations.rule_based_profiler.types import Domain


def test_rule_output_get_get_expectation_configurations(
    rule_output_for_rule_state_with_domains_and_parameters,
):
    expected_expectation_configurations: List[ExpectationConfiguration] = [
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_my_validation",
                "kwargs": {
                    "column": "Age",
                },
                "meta": {},
            }
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_my_validation",
                "kwargs": {
                    "column": "Date",
                },
                "meta": {},
            }
        ),
    ]

    rule_output: RuleOutput = rule_output_for_rule_state_with_domains_and_parameters
    expectation_configurations: List[
        ExpectationConfiguration
    ] = rule_output.get_expectation_configurations()

    assert expectation_configurations == expected_expectation_configurations


def test_rule_output_get_fully_qualified_parameter_names(
    rule_output_for_rule_state_with_domains_and_parameters,
):
    expected_fully_qualified_parameter_names: Dict[Domain, List[str]] = {
        Domain(domain_type="column", domain_kwargs={"column": "Age",}, details={},): [
            "$mean",
        ],
        Domain(domain_type="column", domain_kwargs={"column": "Date",}, details={},): [
            "$mean",
            "$parameter.daily_taxi_fairs.mean_values.details",
            "$parameter.daily_taxi_fairs.mean_values.value",
            "$parameter.date_strings.mm_yyyy_dd_date_format.details",
            "$parameter.date_strings.mm_yyyy_dd_date_format.value",
            "$parameter.date_strings.mm_yyyy_dd_hh_mm_ss_tz_date_format.details",
            "$parameter.date_strings.mm_yyyy_dd_hh_mm_ss_tz_date_format.value",
            "$parameter.date_strings.tolerances.max_abs_error_time_milliseconds",
            "$parameter.date_strings.tolerances.max_num_conversion_attempts",
            "$parameter.date_strings.yyyy_mm_dd_date_format.details",
            "$parameter.date_strings.yyyy_mm_dd_date_format.value",
            "$parameter.date_strings.yyyy_mm_dd_hh_mm_ss_tz_date_format.details",
            "$parameter.date_strings.yyyy_mm_dd_hh_mm_ss_tz_date_format.value",
            "$parameter.monthly_taxi_fairs.mean_values.details",
            "$parameter.monthly_taxi_fairs.mean_values.value",
            "$parameter.tolerances.financial.usd",
            "$parameter.tolerances.mostly",
            "$parameter.weekly_taxi_fairs.mean_values.details",
            "$parameter.weekly_taxi_fairs.mean_values.value",
        ],
    }

    rule_output: RuleOutput = rule_output_for_rule_state_with_domains_and_parameters
    fully_qualified_parameter_names: Dict[
        Domain, List[str]
    ] = rule_output.get_fully_qualified_parameter_names()

    assert fully_qualified_parameter_names == expected_fully_qualified_parameter_names
