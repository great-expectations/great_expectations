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
