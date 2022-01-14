from typing import Any

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.rule_based_profiler.parameter_builder import (
    get_parameter_value_by_fully_qualified_parameter_name,
)


# noinspection PyPep8Naming
def test_get_parameter_value_by_fully_qualified_parameter_name_invalid_parameter_name(
    rule_with_variables_with_parameters, column_Age_domain
):
    with pytest.raises(
        ge_exceptions.ProfilerExecutionError, match=r".+start with \$.*"
    ):
        # noinspection PyUnusedLocal
        parameter_value: Any = get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name="mean",
            domain=column_Age_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )


# noinspection PyPep8Naming
def test_get_parameter_value_by_fully_qualified_parameter_name_valid_parameter_name(
    rule_with_variables_with_parameters,
    column_Age_domain,
    column_Date_domain,
):
    fully_qualified_parameter_name: str

    fully_qualified_parameter_name = "$variables.false_positive_threshold"
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name,
            domain=column_Age_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )
        == 1.0e-2
    )

    fully_qualified_parameter_name = "$mean"
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name,
            domain=column_Age_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )
        == 5.0
    )

    fully_qualified_parameter_name = "$variables.false_positive_threshold"
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name,
            domain=column_Date_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )
        == 1.0e-2
    )

    fully_qualified_parameter_name = (
        "$parameter.date_strings.yyyy_mm_dd_hh_mm_ss_tz_date_format"
    )
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.value",
            domain=column_Date_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )
        == "%Y-%m-%d %H:%M:%S %Z"
    )
    assert get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.details",
        domain=column_Date_domain,
        variables=rule_with_variables_with_parameters.variables,
        parameters=rule_with_variables_with_parameters.parameters,
    ) == {
        "confidence": 7.8e-1,
    }

    fully_qualified_parameter_name = "$parameter.date_strings.yyyy_mm_dd_date_format"
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.value",
            domain=column_Date_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )
        == "%Y-%m-%d"
    )
    assert get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.details",
        domain=column_Date_domain,
        variables=rule_with_variables_with_parameters.variables,
        parameters=rule_with_variables_with_parameters.parameters,
    ) == {
        "confidence": 7.8e-1,
    }

    fully_qualified_parameter_name = (
        "$parameter.date_strings.mm_yyyy_dd_hh_mm_ss_tz_date_format"
    )
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.value",
            domain=column_Date_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )
        == "%m-%Y-%d %H:%M:%S %Z"
    )
    assert get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.details",
        domain=column_Date_domain,
        variables=rule_with_variables_with_parameters.variables,
        parameters=rule_with_variables_with_parameters.parameters,
    ) == {
        "confidence": 7.8e-1,
    }

    fully_qualified_parameter_name = "$parameter.date_strings.mm_yyyy_dd_date_format"
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.value",
            domain=column_Date_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )
        == "%m-%Y-%d"
    )
    assert get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.details",
        domain=column_Date_domain,
        variables=rule_with_variables_with_parameters.variables,
        parameters=rule_with_variables_with_parameters.parameters,
    ) == {
        "confidence": 7.8e-1,
    }

    fully_qualified_parameter_name = (
        "$parameter.date_strings.tolerances.max_abs_error_time_milliseconds"
    )
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name,
            domain=column_Date_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )
        == 100
    )

    fully_qualified_parameter_name = (
        "$parameter.date_strings.tolerances.max_num_conversion_attempts"
    )
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name,
            domain=column_Date_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )
        == 5
    )

    fully_qualified_parameter_name = "$parameter.tolerances.mostly"
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name,
            domain=column_Date_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )
        == 9.1e-1
    )

    fully_qualified_parameter_name = "$mean"
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name,
            domain=column_Date_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )
        == 6.5e-1
    )

    fully_qualified_parameter_name = "$parameter.daily_taxi_fairs.mean_values"
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.value[0]",
            domain=column_Date_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )
        == 2.3
    )
    assert get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.details",
        domain=column_Date_domain,
        variables=rule_with_variables_with_parameters.variables,
        parameters=rule_with_variables_with_parameters.parameters,
    ) == {
        "confidence": "low",
    }

    fully_qualified_parameter_name = "$parameter.daily_taxi_fairs.mean_values"
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.value[1]",
            domain=column_Date_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )
        == 9.8
    )
    assert get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.details",
        domain=column_Date_domain,
        variables=rule_with_variables_with_parameters.variables,
        parameters=rule_with_variables_with_parameters.parameters,
    ) == {
        "confidence": "low",
    }

    fully_qualified_parameter_name = "$parameter.daily_taxi_fairs.mean_values"
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.value[2]",
            domain=column_Date_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )
        == 42.3
    )
    assert get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.details",
        domain=column_Date_domain,
        variables=rule_with_variables_with_parameters.variables,
        parameters=rule_with_variables_with_parameters.parameters,
    ) == {
        "confidence": "low",
    }

    fully_qualified_parameter_name = "$parameter.daily_taxi_fairs.mean_values"
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.value[3]",
            domain=column_Date_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )
        == 8.1
    )
    assert get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.details",
        domain=column_Date_domain,
        variables=rule_with_variables_with_parameters.variables,
        parameters=rule_with_variables_with_parameters.parameters,
    ) == {
        "confidence": "low",
    }

    fully_qualified_parameter_name = "$parameter.weekly_taxi_fairs.mean_values"
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=f'{fully_qualified_parameter_name}.value["friday"]',
            domain=column_Date_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )
        == 78.78
    )
    assert get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.details",
        domain=column_Date_domain,
        variables=rule_with_variables_with_parameters.variables,
        parameters=rule_with_variables_with_parameters.parameters,
    ) == {
        "confidence": "medium",
    }

    fully_qualified_parameter_name = "$parameter.weekly_taxi_fairs.mean_values"
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=f'{fully_qualified_parameter_name}.value["saturday"]',
            domain=column_Date_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )
        == 91.39
    )
    assert get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.details",
        domain=column_Date_domain,
        variables=rule_with_variables_with_parameters.variables,
        parameters=rule_with_variables_with_parameters.parameters,
    ) == {
        "confidence": "medium",
    }

    fully_qualified_parameter_name = "$parameter.weekly_taxi_fairs.mean_values"
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=f'{fully_qualified_parameter_name}.value["sunday"]',
            domain=column_Date_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )
        == 71.43
    )
    assert get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.details",
        domain=column_Date_domain,
        variables=rule_with_variables_with_parameters.variables,
        parameters=rule_with_variables_with_parameters.parameters,
    ) == {
        "confidence": "medium",
    }

    fully_qualified_parameter_name = "$parameter.weekly_taxi_fairs.mean_values"
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=f'{fully_qualified_parameter_name}.value["monday"]',
            domain=column_Date_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )
        == 74.35
    )
    assert get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.details",
        domain=column_Date_domain,
        variables=rule_with_variables_with_parameters.variables,
        parameters=rule_with_variables_with_parameters.parameters,
    ) == {
        "confidence": "medium",
    }

    fully_qualified_parameter_name = "$parameter.monthly_taxi_fairs.mean_values"
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=f'{fully_qualified_parameter_name}.value[1]["friday"]',
            domain=column_Date_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )
        == 98.78
    )
    assert get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.details",
        domain=column_Date_domain,
        variables=rule_with_variables_with_parameters.variables,
        parameters=rule_with_variables_with_parameters.parameters,
    ) == {
        "confidence": "high",
    }

    fully_qualified_parameter_name = "$parameter.monthly_taxi_fairs.mean_values"
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=f'{fully_qualified_parameter_name}.value[18]["saturday"]',
            domain=column_Date_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )
        == 81.39
    )
    assert get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.details",
        domain=column_Date_domain,
        variables=rule_with_variables_with_parameters.variables,
        parameters=rule_with_variables_with_parameters.parameters,
    ) == {
        "confidence": "high",
    }

    fully_qualified_parameter_name = "$parameter.monthly_taxi_fairs.mean_values"
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=f'{fully_qualified_parameter_name}.value[20]["sunday"]',
            domain=column_Date_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )
        == 72.43
    )
    assert get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.details",
        domain=column_Date_domain,
        variables=rule_with_variables_with_parameters.variables,
        parameters=rule_with_variables_with_parameters.parameters,
    ) == {
        "confidence": "high",
    }

    fully_qualified_parameter_name = "$parameter.monthly_taxi_fairs.mean_values"
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=f'{fully_qualified_parameter_name}.value[21]["monday"]',
            domain=column_Date_domain,
            variables=rule_with_variables_with_parameters.variables,
            parameters=rule_with_variables_with_parameters.parameters,
        )
        == 94.35
    )
    assert get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=f"{fully_qualified_parameter_name}.details",
        domain=column_Date_domain,
        variables=rule_with_variables_with_parameters.variables,
        parameters=rule_with_variables_with_parameters.parameters,
    ) == {
        "confidence": "high",
    }
