from typing import Dict, List

import pytest

from great_expectations.core.domain import Domain
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,
    ParameterNode,
    build_parameter_container,
    build_parameter_container_for_variables,
    get_fully_qualified_parameter_names,
    get_parameter_values_for_fully_qualified_parameter_names,
)


@pytest.mark.unit
def test_build_parameter_container(
    parameters_with_different_depth_level_values,
    multi_part_name_parameter_container,
):
    parameter_container = ParameterContainer(parameter_nodes=None)
    build_parameter_container(
        parameter_container=parameter_container,
        parameter_values=parameters_with_different_depth_level_values,
    )
    assert parameter_container == multi_part_name_parameter_container


@pytest.mark.unit
def test_get_fully_qualified_parameter_names(
    parameters_with_different_depth_level_values,
):
    parameter_container = ParameterContainer(parameter_nodes=None)
    build_parameter_container(
        parameter_container=parameter_container,
        parameter_values=parameters_with_different_depth_level_values,
    )

    domain = Domain(
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs=None,
        details=None,
        rule_name="my_rule",
    )
    # Convert variables argument to ParameterContainer
    variables: ParameterContainer = build_parameter_container_for_variables(
        variables_configs={
            "my_int": 9,
            "my_float": 3.38,
            "my_string": "hello",
        }
    )
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    expected_fully_qualified_parameter_names: List[str] = [
        "$variables",
        "$parameter.date_strings.yyyy_mm_dd_hh_mm_ss_tz_date_format",
        "$parameter.date_strings.yyyy_mm_dd_date_format",
        "$parameter.date_strings.mm_yyyy_dd_hh_mm_ss_tz_date_format",
        "$parameter.date_strings.mm_yyyy_dd_date_format",
        "$parameter.date_strings.tolerances.max_abs_error_time_milliseconds",
        "$parameter.date_strings.tolerances.max_num_conversion_attempts",
        "$parameter.tolerances.mostly",
        "$parameter.tolerances.financial.usd",
        "$parameter.monthly_taxi_fairs.mean_values",
        "$parameter.daily_taxi_fairs.mean_values",
        "$parameter.weekly_taxi_fairs.mean_values",
        "$mean",
    ]

    fully_qualified_parameter_names: List[str] = get_fully_qualified_parameter_names(
        domain=domain,
        variables=variables,
        parameters=parameters,
    )
    assert len(fully_qualified_parameter_names) == len(
        expected_fully_qualified_parameter_names
    )
    assert sorted(fully_qualified_parameter_names) == sorted(
        expected_fully_qualified_parameter_names
    )


@pytest.mark.unit
def test_get_parameter_values_for_fully_qualified_parameter_names(
    parameters_with_different_depth_level_values,
):
    parameter_container = ParameterContainer(parameter_nodes=None)
    build_parameter_container(
        parameter_container=parameter_container,
        parameter_values=parameters_with_different_depth_level_values,
    )

    domain = Domain(
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs=None,
        details=None,
        rule_name="my_rule",
    )
    # Convert variables argument to ParameterContainer
    variables: ParameterContainer = build_parameter_container_for_variables(
        variables_configs={
            "my_int": 9,
            "my_float": 3.38,
            "my_string": "hello",
        }
    )
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    # fmt: off
    expected_parameter_values_for_fully_qualified_parameter_names: Dict[str, ParameterNode] = {
        "$variables": {
            "my_int": 9,
            "my_float": 3.38,
            "my_string": "hello",
        },
        "$parameter.weekly_taxi_fairs.mean_values": {
            "value": [
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
                    "sunday": 81.43,
                    "monday": 84.35,
                    "tuesday": 52.3,
                    "wednesday": 43.3,
                    "thursday": 22.2,
                    "friday": 98.78,
                    "saturday": 81.39,
                },
                {
                    "sunday": 61.43,
                    "monday": 34.35,
                    "tuesday": 82.3,
                    "wednesday": 72.3,
                    "thursday": 22.2,
                    "friday": 38.78,
                    "saturday": 51.39,
                },
                {
                    "sunday": 51.43,
                    "monday": 64.35,
                    "tuesday": 72.3,
                    "wednesday": 82.3,
                    "thursday": 22.2,
                    "friday": 98.78,
                    "saturday": 31.39,
                },
                {
                    "sunday": 72.43,
                    "monday": 77.35,
                    "tuesday": 46.3,
                    "wednesday": 47.3,
                    "thursday": 88.2,
                    "friday": 79.78,
                    "saturday": 93.39,
                },
                {
                    "sunday": 72.43,
                    "monday": 73.35,
                    "tuesday": 41.3,
                    "wednesday": 49.3,
                    "thursday": 80.2,
                    "friday": 78.78,
                    "saturday": 93.39,
                },
                {
                    "sunday": 74.43,
                    "monday": 78.35,
                    "tuesday": 49.3,
                    "wednesday": 43.3,
                    "thursday": 88.2,
                    "friday": 72.78,
                    "saturday": 97.39,
                },
                {
                    "sunday": 73.43,
                    "monday": 72.35,
                    "tuesday": 40.3,
                    "wednesday": 40.3,
                    "thursday": 89.2,
                    "friday": 77.78,
                    "saturday": 90.39,
                },
                {
                    "sunday": 72.43,
                    "monday": 73.35,
                    "tuesday": 45.3,
                    "wednesday": 44.3,
                    "thursday": 89.2,
                    "friday": 77.78,
                    "saturday": 96.39,
                },
                {
                    "sunday": 75.43,
                    "monday": 74.25,
                    "tuesday": 42.33,
                    "wednesday": 42.23,
                    "thursday": 82.21,
                    "friday": 78.76,
                    "saturday": 91.37,
                },
                {
                    "sunday": 71.43,
                    "monday": 74.37,
                    "tuesday": 42.3,
                    "wednesday": 42.32,
                    "thursday": 82.23,
                    "friday": 78.77,
                    "saturday": 91.49,
                },
                {
                    "sunday": 71.63,
                    "monday": 74.37,
                    "tuesday": 42.2,
                    "wednesday": 42.1,
                    "thursday": 82.29,
                    "friday": 78.79,
                    "saturday": 91.39,
                },
                {
                    "sunday": 71.42,
                    "monday": 74.33,
                    "tuesday": 42.33,
                    "wednesday": 42.34,
                    "thursday": 82.25,
                    "friday": 78.77,
                    "saturday": 91.69,
                },
                {
                    "sunday": 71.44,
                    "monday": 72.35,
                    "tuesday": 42.33,
                    "wednesday": 42.31,
                    "thursday": 82.29,
                    "friday": 78.68,
                    "saturday": 91.49,
                },
                {
                    "sunday": 71.44,
                    "monday": 74.32,
                    "tuesday": 42.32,
                    "wednesday": 42.32,
                    "thursday": 82.29,
                    "friday": 78.77,
                    "saturday": 91.49,
                },
                {
                    "sunday": 71.44,
                    "monday": 74.33,
                    "tuesday": 42.21,
                    "wednesday": 42.31,
                    "thursday": 82.27,
                    "friday": 78.74,
                    "saturday": 91.49,
                },
                {
                    "sunday": 71.33,
                    "monday": 74.25,
                    "tuesday": 42.31,
                    "wednesday": 42.03,
                    "thursday": 82.02,
                    "friday": 78.08,
                    "saturday": 91.38,
                },
                {
                    "sunday": 71.41,
                    "monday": 74.31,
                    "tuesday": 42.39,
                    "wednesday": 42.93,
                    "thursday": 82.92,
                    "friday": 78.75,
                    "saturday": 91.49,
                },
                {
                    "sunday": 72.43,
                    "monday": 73.35,
                    "tuesday": 42.3,
                    "wednesday": 32.3,
                    "thursday": 52.2,
                    "friday": 88.78,
                    "saturday": 81.39,
                },
                {
                    "sunday": 71.43,
                    "monday": 74.35,
                    "tuesday": 32.3,
                    "wednesday": 92.3,
                    "thursday": 72.2,
                    "friday": 74.78,
                    "saturday": 51.39,
                },
                {
                    "sunday": 72.43,
                    "monday": 64.35,
                    "tuesday": 52.3,
                    "wednesday": 42.39,
                    "thursday": 82.28,
                    "friday": 78.77,
                    "saturday": 91.36,
                },
                {
                    "sunday": 81.43,
                    "monday": 94.35,
                    "tuesday": 62.3,
                    "wednesday": 52.3,
                    "thursday": 92.2,
                    "friday": 88.78,
                    "saturday": 51.39,
                },
                {
                    "sunday": 21.43,
                    "monday": 34.35,
                    "tuesday": 42.34,
                    "wednesday": 62.3,
                    "thursday": 52.2,
                    "friday": 98.78,
                    "saturday": 81.39,
                },
                {
                    "sunday": 71.33,
                    "monday": 74.25,
                    "tuesday": 42.13,
                    "wednesday": 42.93,
                    "thursday": 82.82,
                    "friday": 78.78,
                    "saturday": 91.39,
                },
                {
                    "sunday": 72.43,
                    "monday": 73.35,
                    "tuesday": 44.3,
                    "wednesday": 45.3,
                    "thursday": 86.2,
                    "friday": 77.78,
                    "saturday": 98.39,
                },
                {
                    "sunday": 79.43,
                    "monday": 78.35,
                    "tuesday": 47.3,
                    "wednesday": 46.3,
                    "thursday": 85.2,
                    "friday": 74.78,
                    "saturday": 93.39,
                },
                {
                    "sunday": 71.42,
                    "monday": 74.31,
                    "tuesday": 42.0,
                    "wednesday": 42.1,
                    "thursday": 82.23,
                    "friday": 65.78,
                    "saturday": 91.26,
                },
                {
                    "sunday": 91.43,
                    "monday": 84.35,
                    "tuesday": 42.37,
                    "wednesday": 42.36,
                    "thursday": 82.25,
                    "friday": 78.74,
                    "saturday": 91.32,
                },
                {
                    "sunday": 71.33,
                    "monday": 74.45,
                    "tuesday": 42.35,
                    "wednesday": 42.36,
                    "thursday": 82.27,
                    "friday": 26.78,
                    "saturday": 71.39,
                },
                {
                    "sunday": 71.53,
                    "monday": 73.35,
                    "tuesday": 43.32,
                    "wednesday": 42.23,
                    "thursday": 82.32,
                    "friday": 78.18,
                    "saturday": 91.49,
                },
                {
                    "sunday": 71.53,
                    "monday": 74.25,
                    "tuesday": 52.3,
                    "wednesday": 52.3,
                    "thursday": 81.23,
                    "friday": 78.78,
                    "saturday": 78.39,
                },
            ],
            "details": {
                "confidence": "high",
            },
        },
        "$parameter.tolerances.mostly": 0.91,
        "$parameter.tolerances.financial.usd": 1.0,
        "$parameter.monthly_taxi_fairs.mean_values": {
            "value": [
                2.3,
                9.8,
                42.3,
                8.1,
                38.5,
                53.7,
                71.43,
                16.34,
                49.43,
                74.35,
                51.98,
                46.42,
                20.01,
                69.44,
                65.32,
                8.83,
                55.79,
                82.2,
                36.93,
                83.78,
                31.13,
                76.93,
                67.67,
                25.12,
                58.04,
                79.78,
                90.91,
                15.26,
                61.65,
                78.78,
                12.99,
            ],
            "details": {
                "confidence": "low",
            },
        },
        "$parameter.date_strings.yyyy_mm_dd_hh_mm_ss_tz_date_format": {
            "value": "%Y-%m-%d %H:%M:%S %Z",
            "details": {
                "confidence": 0.78,
            },
        },
        "$parameter.date_strings.yyyy_mm_dd_date_format": {
            "value": "%Y-%m-%d",
            "details": {
                "confidence": 0.78,
            },
        },
        "$parameter.date_strings.tolerances.max_num_conversion_attempts": 5,
        "$parameter.date_strings.tolerances.max_abs_error_time_milliseconds": 100,
        "$parameter.date_strings.mm_yyyy_dd_hh_mm_ss_tz_date_format": {
            "value": "%m-%Y-%d %H:%M:%S %Z",
            "details": {
                "confidence": 0.78,
            },
        },
        "$parameter.date_strings.mm_yyyy_dd_date_format": {
            "value": "%m-%Y-%d",
            "details": {
                "confidence": 0.78,
            },
        },
        "$parameter.daily_taxi_fairs.mean_values": {
            "value": {
                "sunday": 71.43,
                "monday": 74.35,
                "tuesday": 42.3,
                "wednesday": 42.3,
                "thursday": 82.2,
                "friday": 78.78,
                "saturday": 91.39,
            },
            "details": {
                "confidence": "medium",
            },
        },
        "$mean": 0.65,
    }
    # fmt: on

    parameter_values_for_fully_qualified_parameter_names: Dict[
        str, ParameterNode
    ] = get_parameter_values_for_fully_qualified_parameter_names(
        domain=domain,
        variables=variables,
        parameters=parameters,
    )
    assert (
        parameter_values_for_fully_qualified_parameter_names
        == expected_parameter_values_for_fully_qualified_parameter_names
    )
