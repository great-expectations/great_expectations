from typing import Any

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.rule_based_profiler.types import (
    get_parameter_value_by_fully_qualified_parameter_name,
)


# noinspection PyPep8Naming
def test_get_parameter_value_by_fully_qualified_parameter_name_invalid_parameter_name(
    column_Age_domain,
    variables_multi_part_name_parameter_container,
    rule_state_with_domains_and_parameters,
):
    with pytest.raises(
        ge_exceptions.ProfilerExecutionError, match=r".+start with \$.*"
    ):
        # noinspection PyUnusedLocal
        parameter_value: Any = get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name="mean",
            domain=column_Age_domain,
            variables=variables_multi_part_name_parameter_container,
            parameters=rule_state_with_domains_and_parameters.parameters,
        )


# noinspection PyPep8Naming
@pytest.mark.parametrize(
    "domain_name,fully_qualified_parameter_name,value,details,use_value_suffix,value_accessor,test_details,",
    [
        pytest.param(
            "age",
            "$variables.false_positive_threshold",
            1.0e-2,
            None,
            False,
            None,
            False,
        ),
        pytest.param(
            "age",
            "$mean",
            5.0,
            None,
            False,
            None,
            False,
        ),
        pytest.param(
            "date",
            "$variables.false_positive_threshold",
            1.0e-2,
            None,
            False,
            None,
            False,
        ),
        pytest.param(
            "date",
            "$parameter.date_strings.yyyy_mm_dd_hh_mm_ss_tz_date_format",
            "%Y-%m-%d %H:%M:%S %Z",
            {
                "confidence": 7.8e-1,
            },
            True,
            None,
            True,
        ),
        pytest.param(
            "date",
            "$parameter.date_strings.yyyy_mm_dd_date_format",
            "%Y-%m-%d",
            {
                "confidence": 7.8e-1,
            },
            True,
            None,
            True,
        ),
        pytest.param(
            "date",
            "$parameter.date_strings.mm_yyyy_dd_hh_mm_ss_tz_date_format",
            "%m-%Y-%d %H:%M:%S %Z",
            {
                "confidence": 7.8e-1,
            },
            True,
            None,
            True,
        ),
        pytest.param(
            "date",
            "$parameter.date_strings.mm_yyyy_dd_date_format",
            "%m-%Y-%d",
            {
                "confidence": 7.8e-1,
            },
            True,
            None,
            True,
        ),
        pytest.param(
            "date",
            "$parameter.date_strings.tolerances.max_abs_error_time_milliseconds",
            1.0e2,
            None,
            False,
            None,
            False,
        ),
        pytest.param(
            "date",
            "$parameter.date_strings.tolerances.max_num_conversion_attempts",
            5,
            None,
            False,
            None,
            False,
        ),
        pytest.param(
            "date",
            "$parameter.tolerances.mostly",
            9.1e-1,
            None,
            False,
            None,
            False,
        ),
        pytest.param(
            "date",
            "$mean",
            6.5e-1,
            None,
            False,
            None,
            False,
        ),
        pytest.param(
            "date",
            "$parameter.monthly_taxi_fairs.mean_values",
            2.3,
            {
                "confidence": "low",
            },
            True,
            "[0]",
            True,
        ),
        pytest.param(
            "date",
            "$parameter.monthly_taxi_fairs.mean_values",
            9.8,
            {
                "confidence": "low",
            },
            True,
            "[1]",
            True,
        ),
        pytest.param(
            "date",
            "$parameter.monthly_taxi_fairs.mean_values",
            42.3,
            {
                "confidence": "low",
            },
            True,
            "[2]",
            True,
        ),
        pytest.param(
            "date",
            "$parameter.monthly_taxi_fairs.mean_values",
            8.1,
            {
                "confidence": "low",
            },
            True,
            "[3]",
            True,
        ),
        pytest.param(
            "date",
            "$parameter.daily_taxi_fairs.mean_values",
            78.78,
            {
                "confidence": "medium",
            },
            True,
            "['friday']",
            True,
        ),
        pytest.param(
            "date",
            "$parameter.daily_taxi_fairs.mean_values",
            91.39,
            {
                "confidence": "medium",
            },
            True,
            "['saturday']",
            True,
        ),
        pytest.param(
            "date",
            "$parameter.daily_taxi_fairs.mean_values",
            71.43,
            {
                "confidence": "medium",
            },
            True,
            "['sunday']",
            True,
        ),
        pytest.param(
            "date",
            "$parameter.daily_taxi_fairs.mean_values",
            74.35,
            {
                "confidence": "medium",
            },
            True,
            "['monday']",
            True,
        ),
        pytest.param(
            "date",
            "$parameter.daily_taxi_fairs.mean_values",
            {
                "sunday": 71.43,
                "monday": 74.35,
                "tuesday": 42.3,
                "wednesday": 42.3,
                "thursday": 82.2,
                "friday": 78.78,
                "saturday": 91.39,
            },
            {
                "confidence": "medium",
            },
            True,
            None,
            True,
        ),
        pytest.param(
            "date",
            "$parameter.weekly_taxi_fairs.mean_values",
            98.78,
            {
                "confidence": "high",
            },
            True,
            '[1]["friday"]',
            True,
        ),
        pytest.param(
            "date",
            "$parameter.weekly_taxi_fairs.mean_values",
            81.39,
            {
                "confidence": "high",
            },
            True,
            "[18]['saturday']",
            True,
        ),
        pytest.param(
            "date",
            "$parameter.weekly_taxi_fairs.mean_values",
            72.43,
            {
                "confidence": "high",
            },
            True,
            '[20]["sunday"]',
            True,
        ),
        pytest.param(
            "date",
            "$parameter.weekly_taxi_fairs.mean_values",
            94.35,
            {
                "confidence": "high",
            },
            True,
            "[21]['monday']",
            True,
        ),
    ],
)
def test_get_parameter_value_by_fully_qualified_parameter_name_valid_parameter_name(
    column_Age_domain,
    column_Date_domain,
    rule_state_with_domains_and_parameters,
    variables_multi_part_name_parameter_container,
    domain_name,
    fully_qualified_parameter_name,
    value,
    value_accessor,
    details,
    use_value_suffix,
    test_details,
):
    if domain_name == "age":
        domain = column_Age_domain
    elif domain_name == "date":
        domain = column_Date_domain
    else:
        raise ValueError(
            f'Supported "domain_name" parameter values are "age" and "date".'
        )

    if value_accessor is None:
        value_accessor = ""

    if use_value_suffix:
        fully_qualified_parameter_name_for_value = (
            f"{fully_qualified_parameter_name}.value{value_accessor}"
        )
    else:
        fully_qualified_parameter_name_for_value = (
            f"{fully_qualified_parameter_name}{value_accessor}"
        )

    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
            domain=domain,
            variables=variables_multi_part_name_parameter_container,
            parameters=rule_state_with_domains_and_parameters.parameters,
        )
        == value
    )

    if test_details:
        fully_qualified_parameter_name_for_details = (
            f"{fully_qualified_parameter_name}.details"
        )

        assert (
            get_parameter_value_by_fully_qualified_parameter_name(
                fully_qualified_parameter_name=fully_qualified_parameter_name_for_details,
                domain=domain,
                variables=variables_multi_part_name_parameter_container,
                parameters=rule_state_with_domains_and_parameters.parameters,
            )
            == details
        )
