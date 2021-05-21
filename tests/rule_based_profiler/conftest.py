from typing import Any, Dict

import pytest

from great_expectations.rule_based_profiler.domain_builder.domain import Domain

# noinspection PyUnresolvedReferences
from great_expectations.rule_based_profiler.parameter_builder.parameter_container import (
    ParameterContainer,
    ParameterNode,
)
from great_expectations.rule_based_profiler.rule.rule import Rule

# noinspection PyUnresolvedReferences
from tests.rule_based_profiler.alice_user_workflow_fixture import (
    alice_columnar_table_single_batch,
)

# noinspection PyUnresolvedReferences
from tests.rule_based_profiler.bob_user_workflow_fixture import (
    bob_columnar_table_multi_batch,
)


# noinspection PyPep8Naming
@pytest.fixture
def column_Age_structured_type_domain():
    return Domain(
        domain_kwargs={"column": "Age", "batch_id": "1234567890"},
    )


# noinspection PyPep8Naming
@pytest.fixture
def column_Date_structured_type_domain():
    return Domain(
        domain_kwargs={"column": "Date", "batch_id": "1234567890"},
    )


@pytest.fixture
def single_part_name_parameter_container():
    return ParameterContainer(
        parameter_nodes={
            "mean": ParameterNode(
                attributes={
                    "mean": 5.0,
                },
                details=None,
                descendants=None,
            ),
        }
    )


@pytest.fixture
def multi_part_name_parameter_container():
    """
    $parameter.date_strings.yyyy_mm_dd_hh_mm_ss_tz_date_format
    $parameter.date_strings.yyyy_mm_dd_date_format
    $parameter.date_strings.mm_yyyy_dd_hh_mm_ss_tz_date_format
    $parameter.date_strings.mm_yyyy_dd_date_format
    $parameter.date_strings.tolerances.max_abs_error_time_milliseconds
    $parameter.date_strings.tolerances.max_num_conversion_attempts
    $parameter.tolerances.mostly
    $mean
    """
    root_mean_node: ParameterNode = ParameterNode(
        attributes={
            "mean": 6.5e-1,
        },
        details=None,
        descendants=None,
    )
    financial_tolerances_parameter_node: ParameterNode = ParameterNode(
        attributes={
            "usd": 1.0,
        },
        details=None,
        descendants=None,
    )
    tolerances_parameter_node: ParameterNode = ParameterNode(
        attributes={
            "mostly": 9.1e-1,
        },
        details=None,
        descendants={
            "financial": financial_tolerances_parameter_node,
        },
    )
    date_strings_tolerances_parameter_node: ParameterNode = ParameterNode(
        attributes={
            "max_abs_error_time_milliseconds": 100,
            "max_num_conversion_attempts": 5,
        },
        details=None,
        descendants=None,
    )
    date_strings_parameter_node: ParameterNode = ParameterNode(
        attributes={
            "yyyy_mm_dd_hh_mm_ss_tz_date_format": "%Y-%m-%d %H:%M:%S %Z",
            "yyyy_mm_dd_date_format": "%Y-%m-%d",
            "mm_yyyy_dd_hh_mm_ss_tz_date_format": "%m-%Y-%d %H:%M:%S %Z",
            "mm_yyyy_dd_date_format": "%m-%Y-%d",
        },
        details={
            "yyyy_mm_dd_hh_mm_ss_tz_date_format": {
                "confidence": 7.8e-1,
            },
            "yyyy_mm_dd_date_format": {
                "confidence": 7.8e-1,
            },
            "mm_yyyy_dd_hh_mm_ss_tz_date_format": {
                "confidence": 7.8e-1,
            },
            "mm_yyyy_dd_date_format": {
                "confidence": 7.8e-1,
            },
        },
        descendants={
            "tolerances": date_strings_tolerances_parameter_node,
        },
    )
    parameter_multi_part_name_parameter_node: ParameterNode = ParameterNode(
        attributes=None,
        descendants={
            "date_strings": date_strings_parameter_node,
            "tolerances": tolerances_parameter_node,
        },
    )
    root_parameter_node: ParameterNode = ParameterNode(
        attributes=None,
        descendants={
            "parameter": parameter_multi_part_name_parameter_node,
        },
    )
    return ParameterContainer(
        parameter_nodes={
            "parameter": root_parameter_node,
            "mean": root_mean_node,
        }
    )


@pytest.fixture
def parameter_values_eight_parameters_multiple_depths():
    parameter_values: Dict[str, Dict[str, Any]] = {
        "$parameter.date_strings.yyyy_mm_dd_hh_mm_ss_tz_date_format": {
            "value": "%Y-%m-%d %H:%M:%S %Z",
            "details": {"confidence": 7.8e-1},
        },
        "$parameter.date_strings.yyyy_mm_dd_date_format": {
            "value": "%Y-%m-%d",
            "details": {"confidence": 7.8e-1},
        },
        "$parameter.date_strings.mm_yyyy_dd_hh_mm_ss_tz_date_format": {
            "value": "%m-%Y-%d %H:%M:%S %Z",
            "details": {"confidence": 7.8e-1},
        },
        "$parameter.date_strings.mm_yyyy_dd_date_format": {
            "value": "%m-%Y-%d",
            "details": {"confidence": 7.8e-1},
        },
        "$parameter.date_strings.tolerances.max_abs_error_time_milliseconds": {
            "value": 100,
            "details": None,
        },
        "$parameter.date_strings.tolerances.max_num_conversion_attempts": {
            "value": 5,
            "details": None,
        },
        "$parameter.tolerances.mostly": {"value": 9.1e-1, "details": None},
        "$parameter.tolerances.financial.usd": {
            "value": 1.0,
            "details": None,
        },
        "$mean": {"value": 6.5e-1, "details": None},
    }
    return parameter_values


# noinspection PyPep8Naming
@pytest.fixture
def rule_without_variables_without_parameters():
    rule: Rule = Rule(
        name="rule_with_no_variables_no_parameters",
        domain_builder=None,
        parameter_builders=None,
        expectation_configuration_builders=None,
        variables=None,
    )
    return rule


# noinspection PyPep8Naming
@pytest.fixture
def rule_with_variables_with_parameters(
    column_Age_structured_type_domain,
    column_Date_structured_type_domain,
    single_part_name_parameter_container,
    multi_part_name_parameter_container,
):
    variables_multi_part_name_parameter_node: ParameterNode = ParameterNode(
        attributes={
            "false_positive_threshold": 1.0e-2,
        },
        details=None,
        descendants=None,
    )
    root_variables_node: ParameterNode = ParameterNode(
        attributes=None,
        descendants={
            "variables": variables_multi_part_name_parameter_node,  # $variables.false_positive_threshold
        },
    )
    rule: Rule = Rule(
        name="rule_with_variables_with_parameters",
        domain_builder=None,
        parameter_builders=None,
        expectation_configuration_builders=None,
        variables=ParameterContainer(
            parameter_nodes={
                "variables": root_variables_node,
            }
        ),
    )
    rule._parameters = {
        column_Age_structured_type_domain.id: single_part_name_parameter_container,
        column_Date_structured_type_domain.id: multi_part_name_parameter_container,
    }
    return rule
