from typing import Any

import pytest

import great_expectations.exceptions as ge_exceptions

# TODO: <Alex>ALEX -- build test to ensure that id property and access works for a list of domains of various types.</Alex>
from great_expectations.profiler.domain_builder.domain import (
    Domain,
    SemanticDomainTypes,
    StorageDomainTypes,
)
from great_expectations.profiler.parameter_builder.parameter_container import (
    ParameterContainer,
)
from great_expectations.profiler.rule.rule_state import RuleState


# noinspection PyPep8Naming
@pytest.fixture
def column_Age_storage_type_domain():
    return Domain(
        domain_kwargs={"column": "Age", "batch_id": "1234567890"},
        domain_type=StorageDomainTypes.COLUMN,
    )


# noinspection PyPep8Naming
@pytest.fixture
def column_Date_storage_type_domain():
    return Domain(
        domain_kwargs={"column": "Date", "batch_id": "1234567890"},
        domain_type=StorageDomainTypes.COLUMN,
    )


@pytest.fixture
def single_syllable_parameter_container():
    return ParameterContainer(parameters={"mean": 5.0}, details=None, descendants=None)


@pytest.fixture
def multi_syllable_parameter_container():
    """
    $parameter.date_strings.yyyy_mm_dd_hh_mm_ss_tz_date_format
    $parameter.date_strings.yyyy_mm_dd_date_format
    $parameter.date_strings.mm_yyyy_dd_hh_mm_ss_tz_date_format
    $parameter.date_strings.mm_yyyy_dd_date_format
    $parameter.date_strings.tolerances.max_abs_error_time_milliseconds
    $parameter.date_strings.tolerances.max_num_conversion_attempts
    $parameter.tolerances.mostly
    """
    tolerances_parameter_container: ParameterContainer = ParameterContainer(
        parameters={
            "mostly": 9.3e-1,
        },
        details=None,
        descendants=None,
    )
    date_strings_tolerances_parameter_container: ParameterContainer = (
        ParameterContainer(
            parameters={
                "max_abs_error_time_milliseconds": 100,
                "max_num_conversion_attempts": 5,
            },
            details=None,
            descendants=None,
        )
    )
    date_strings_parameter_container: ParameterContainer = ParameterContainer(
        parameters={
            "yyyy_mm_dd_hh_mm_ss_tz_date_format": "%Y-%m-%d %H:%M:%S %Z",
            "yyyy_mm_dd_date_format": "%Y-%m-%d",
            "mm_yyyy_dd_hh_mm_ss_tz_date_format": "%m-%Y-%d %H:%M:%S %Z",
            "mm_yyyy_dd_date_format": "%m-%Y-%d",
        },
        details={
            "confidence": {
                "yyyy_mm_dd_hh_mm_ss_tz_date_format": 1.0,
                "yyyy_mm_dd_date_format": 1.0,
                "mm_yyyy_dd_hh_mm_ss_tz_date_format": 8.0e-1,
                "mm_yyyy_dd_date_format": 8.0e-1,
            }
        },
        descendants={
            "tolerances": date_strings_tolerances_parameter_container,
        },
    )
    parameter_syllable_level_parameter_container: ParameterContainer = (
        ParameterContainer(
            parameters=None,
            descendants={
                "date_strings": date_strings_parameter_container,
                "tolerances": tolerances_parameter_container,
            },
        )
    )
    root_parameter_container: ParameterContainer = ParameterContainer(
        parameters=None,
        descendants={
            "parameter": parameter_syllable_level_parameter_container,
        },
    )
    return root_parameter_container


# noinspection PyPep8Naming
@pytest.fixture
def rule_state_with_no_parameters(
    column_Age_storage_type_domain, column_Date_storage_type_domain
):
    """Simple rule_state with one domain, currently set to active"""
    return RuleState(
        active_domain=column_Age_storage_type_domain,
        domains=[
            column_Age_storage_type_domain,
            column_Date_storage_type_domain,
        ],
        parameters={},
    )


# noinspection PyPep8Naming
@pytest.fixture
def rule_state_with_parameters(
    column_Age_storage_type_domain,
    column_Date_storage_type_domain,
    single_syllable_parameter_container,
    multi_syllable_parameter_container,
):
    """Simple rule_state with one domain, currently set to active"""
    return RuleState(
        active_domain=column_Age_storage_type_domain,
        domains=[
            column_Age_storage_type_domain,
            column_Date_storage_type_domain,
        ],
        parameters={
            column_Age_storage_type_domain.id: single_syllable_parameter_container,
            column_Date_storage_type_domain.id: multi_syllable_parameter_container,
        },
        variables=ParameterContainer(
            parameters={"false_positive_threshold": 0.01},
            details=None,
            descendants=None,
        ),
    )


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
        == 9.3e-1
    )
