from typing import Any

import pytest

import great_expectations.exceptions as ge_exceptions


# TODO: <Alex>ALEX -- build test to ensure that id property and access works for a list of domains of various types.</Alex>
# noinspection PyPep8Naming
def test_id_property_of_active_domain(
    rule_state_with_no_parameters,
    rule_state_with_parameters,
    column_Age_storage_type_domain,
    column_Date_storage_type_domain,
):
    assert (
        rule_state_with_no_parameters.active_domain.id
        == column_Age_storage_type_domain.id
    )

    assert (
        rule_state_with_parameters.active_domain.id == column_Age_storage_type_domain.id
    )

    rule_state_with_parameters.active_domain = column_Date_storage_type_domain
    assert (
        rule_state_with_parameters.active_domain.id
        == column_Date_storage_type_domain.id
    )


def test_get_parameter_value_invalid_parameter_name(rule_state_with_parameters):
    with pytest.raises(
        ge_exceptions.ProfilerExecutionError, match=r".+start with \$.*"
    ):
        # noinspection PyUnusedLocal
        parameter_value: Any = rule_state_with_parameters.get_parameter_value(
            fully_qualified_parameter_name="mean"
        )


# noinspection PyPep8Naming
def test_get_parameter_value_valid_parameter_name(
    rule_state_with_parameters, column_Date_storage_type_domain
):
    assert (
        rule_state_with_parameters.get_parameter_value(
            fully_qualified_parameter_name="$variables.false_positive_threshold"
        )
        == 0.01
    )

    assert (
        rule_state_with_parameters.get_parameter_value(
            fully_qualified_parameter_name="$mean"
        )
        == 5.0
    )

    rule_state_with_parameters.active_domain = column_Date_storage_type_domain
    assert (
        rule_state_with_parameters.get_parameter_value(
            fully_qualified_parameter_name="$parameter.date_strings.yyyy_mm_dd_hh_mm_ss_tz_date_format"
        )
        == "%Y-%m-%d %H:%M:%S %Z"
    )
    assert (
        rule_state_with_parameters.get_parameter_value(
            fully_qualified_parameter_name="$parameter.date_strings.yyyy_mm_dd_date_format"
        )
        == "%Y-%m-%d"
    )
    assert (
        rule_state_with_parameters.get_parameter_value(
            fully_qualified_parameter_name="$parameter.date_strings.mm_yyyy_dd_hh_mm_ss_tz_date_format"
        )
        == "%m-%Y-%d %H:%M:%S %Z"
    )
    assert (
        rule_state_with_parameters.get_parameter_value(
            fully_qualified_parameter_name="$parameter.date_strings.mm_yyyy_dd_date_format"
        )
        == "%m-%Y-%d"
    )
    assert (
        rule_state_with_parameters.get_parameter_value(
            fully_qualified_parameter_name="$parameter.date_strings.tolerances.max_abs_error_time_milliseconds"
        )
        == 100
    )
    assert (
        rule_state_with_parameters.get_parameter_value(
            fully_qualified_parameter_name="$parameter.date_strings.tolerances.max_num_conversion_attempts"
        )
        == 5
    )
    assert (
        rule_state_with_parameters.get_parameter_value(
            fully_qualified_parameter_name="$parameter.tolerances.mostly"
        )
        == 9.1e-1
    )
    assert (
        rule_state_with_parameters.get_parameter_value(
            fully_qualified_parameter_name="$mean"
        )
        == 6.5e-1
    )
