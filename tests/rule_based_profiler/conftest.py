import datetime
import os
from typing import Any, Dict, List, Optional

import pandas as pd
import pytest
from ruamel.yaml import YAML

from great_expectations import DataContext
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler import RuleBasedProfiler
from great_expectations.rule_based_profiler.domain_builder import ColumnDomainBuilder
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    DefaultExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.rule import Rule
from great_expectations.rule_based_profiler.types import (
    Domain,
    ParameterContainer,
    ParameterNode,
)
from tests.conftest import skip_if_python_below_minimum_version

yaml = YAML()


@pytest.fixture
def pandas_test_df():
    skip_if_python_below_minimum_version()

    df: pd.DataFrame = pd.DataFrame(
        {
            "Age": pd.Series(
                [
                    7,
                    15,
                    21,
                    39,
                    None,
                ],
                dtype="float64",
            ),
            "Date": pd.Series(
                [
                    datetime.date(2020, 12, 31),
                    datetime.date(2021, 1, 1),
                    datetime.date(2021, 2, 21),
                    datetime.date(2021, 3, 20),
                    None,
                ],
                dtype="object",
            ),
            "Description": pd.Series(
                [
                    "child",
                    "teenager",
                    "young adult",
                    "adult",
                    None,
                ],
                dtype="object",
            ),
        }
    )
    df["Date"] = pd.to_datetime(df["Date"])
    return df


# noinspection PyPep8Naming
@pytest.fixture
def table_Users_domain():
    skip_if_python_below_minimum_version()

    return Domain(
        domain_type=MetricDomainTypes.TABLE,
        domain_kwargs=None,
        details=None,
    )


# noinspection PyPep8Naming
@pytest.fixture
def column_Age_domain():
    skip_if_python_below_minimum_version()

    return Domain(
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs={
            "column": "Age",
            "batch_id": "c260e179bb1bc81d84bba72a8110d8e2",
        },
        details=None,
    )


# noinspection PyPep8Naming
@pytest.fixture
def column_Date_domain():
    skip_if_python_below_minimum_version()

    return Domain(
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs={
            "column": "Date",
            "batch_id": "c260e179bb1bc81d84bba72a8110d8e2",
        },
        details=None,
    )


# noinspection PyPep8Naming
@pytest.fixture
def column_Description_domain():
    skip_if_python_below_minimum_version()

    return Domain(
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs={
            "column": "Description",
            "batch_id": "c260e179bb1bc81d84bba72a8110d8e2",
        },
        details=None,
    )


@pytest.fixture
def single_part_name_parameter_container():
    skip_if_python_below_minimum_version()

    return ParameterContainer(
        parameter_nodes={
            "mean": ParameterNode(
                {
                    "mean": 5.0,
                }
            ),
        }
    )


@pytest.fixture
def multi_part_name_parameter_container():
    """
    $parameter.date_strings.yyyy_mm_dd_hh_mm_ss_tz_date_format.value
    $parameter.date_strings.yyyy_mm_dd_hh_mm_ss_tz_date_format.details
    $parameter.date_strings.yyyy_mm_dd_date_format.value
    $parameter.date_strings.yyyy_mm_dd_date_format.details
    $parameter.date_strings.mm_yyyy_dd_hh_mm_ss_tz_date_format.value
    $parameter.date_strings.mm_yyyy_dd_hh_mm_ss_tz_date_format.details
    $parameter.date_strings.mm_yyyy_dd_date_format.value
    $parameter.date_strings.mm_yyyy_dd_date_format.details
    $parameter.date_strings.tolerances.max_abs_error_time_milliseconds
    $parameter.date_strings.tolerances.max_num_conversion_attempts
    $parameter.tolerances.mostly
    $mean
    $parameter.monthly_taxi_fairs.mean_values.value[0]
    $parameter.monthly_taxi_fairs.mean_values.value[1]
    $parameter.monthly_taxi_fairs.mean_values.value[2]
    $parameter.monthly_taxi_fairs.mean_values.value[3]
    $parameter.monthly_taxi_fairs.mean_values.details
    $parameter.daily_taxi_fairs.mean_values.value["friday"]
    $parameter.daily_taxi_fairs.mean_values.value["saturday"]
    $parameter.daily_taxi_fairs.mean_values.value["sunday"]
    $parameter.daily_taxi_fairs.mean_values.value["monday"]
    $parameter.daily_taxi_fairs.mean_values.details
    $parameter.weekly_taxi_fairs.mean_values.value[1]['friday']
    $parameter.weekly_taxi_fairs.mean_values.value[18]['saturday']
    $parameter.weekly_taxi_fairs.mean_values.value[20]['sunday']
    $parameter.weekly_taxi_fairs.mean_values.value[21]['monday']
    $parameter.weekly_taxi_fairs.mean_values.details
    """
    skip_if_python_below_minimum_version()

    root_mean_node: ParameterNode = ParameterNode(
        {
            "mean": 6.5e-1,
        }
    )
    financial_tolerances_parameter_node: ParameterNode = ParameterNode(
        {
            "usd": 1.0,
        }
    )
    tolerances_parameter_node: ParameterNode = ParameterNode(
        {
            "mostly": 9.1e-1,
            "financial": financial_tolerances_parameter_node,
        }
    )
    date_strings_tolerances_parameter_node: ParameterNode = ParameterNode(
        {
            "max_abs_error_time_milliseconds": 100,
            "max_num_conversion_attempts": 5,
        }
    )
    date_strings_parameter_node: ParameterNode = ParameterNode(
        {
            "yyyy_mm_dd_hh_mm_ss_tz_date_format": ParameterNode(
                {
                    "value": "%Y-%m-%d %H:%M:%S %Z",
                    "details": ParameterNode(
                        {
                            "confidence": 7.8e-1,
                        },
                    ),
                }
            ),
            "yyyy_mm_dd_date_format": ParameterNode(
                {
                    "value": "%Y-%m-%d",
                    "details": ParameterNode(
                        {
                            "confidence": 7.8e-1,
                        },
                    ),
                }
            ),
            "mm_yyyy_dd_hh_mm_ss_tz_date_format": ParameterNode(
                {
                    "value": "%m-%Y-%d %H:%M:%S %Z",
                    "details": ParameterNode(
                        {
                            "confidence": 7.8e-1,
                        },
                    ),
                }
            ),
            "mm_yyyy_dd_date_format": ParameterNode(
                {
                    "value": "%m-%Y-%d",
                    "details": ParameterNode(
                        {
                            "confidence": 7.8e-1,
                        },
                    ),
                }
            ),
            "tolerances": date_strings_tolerances_parameter_node,
        }
    )
    monthly_taxi_fairs_parameter_node: ParameterNode = ParameterNode(
        {
            "mean_values": ParameterNode(
                {
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
                    "details": ParameterNode(
                        {
                            "confidence": "low",
                        },
                    ),
                }
            ),
        }
    )
    daily_taxi_fairs_parameter_node: ParameterNode = ParameterNode(
        {
            "mean_values": ParameterNode(
                {
                    "value": {
                        "sunday": 71.43,
                        "monday": 74.35,
                        "tuesday": 42.3,
                        "wednesday": 42.3,
                        "thursday": 82.2,
                        "friday": 78.78,
                        "saturday": 91.39,
                    },
                    "details": ParameterNode(
                        {
                            "confidence": "medium",
                        },
                    ),
                }
            ),
        }
    )
    weekly_taxi_fairs_parameter_node: ParameterNode = ParameterNode(
        {
            "mean_values": ParameterNode(
                {
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
                    "details": ParameterNode(
                        {
                            "confidence": "high",
                        },
                    ),
                }
            ),
        }
    )
    parameter_multi_part_name_parameter_node: ParameterNode = ParameterNode(
        {
            "date_strings": date_strings_parameter_node,
            "tolerances": tolerances_parameter_node,
            "monthly_taxi_fairs": monthly_taxi_fairs_parameter_node,
            "daily_taxi_fairs": daily_taxi_fairs_parameter_node,
            "weekly_taxi_fairs": weekly_taxi_fairs_parameter_node,
        }
    )
    root_parameter_node: ParameterNode = ParameterNode(
        {
            "parameter": parameter_multi_part_name_parameter_node,
        }
    )
    return ParameterContainer(
        parameter_nodes={
            "parameter": root_parameter_node,
            "mean": root_mean_node,
        }
    )


@pytest.fixture
def parameters_with_different_depth_level_values():
    skip_if_python_below_minimum_version()

    parameter_values: Dict[str, Any] = {
        "$parameter.date_strings.yyyy_mm_dd_hh_mm_ss_tz_date_format.value": "%Y-%m-%d %H:%M:%S %Z",
        "$parameter.date_strings.yyyy_mm_dd_hh_mm_ss_tz_date_format.details": {
            "confidence": 7.8e-1,
        },
        "$parameter.date_strings.yyyy_mm_dd_date_format.value": "%Y-%m-%d",
        "$parameter.date_strings.yyyy_mm_dd_date_format.details": {
            "confidence": 7.8e-1,
        },
        "$parameter.date_strings.mm_yyyy_dd_hh_mm_ss_tz_date_format.value": "%m-%Y-%d %H:%M:%S %Z",
        "$parameter.date_strings.mm_yyyy_dd_hh_mm_ss_tz_date_format.details": {
            "confidence": 7.8e-1,
        },
        "$parameter.date_strings.mm_yyyy_dd_date_format.value": "%m-%Y-%d",
        "$parameter.date_strings.mm_yyyy_dd_date_format.details": {
            "confidence": 7.8e-1,
        },
        "$parameter.date_strings.tolerances.max_abs_error_time_milliseconds": 100,
        "$parameter.date_strings.tolerances.max_num_conversion_attempts": 5,
        "$parameter.tolerances.mostly": 9.1e-1,
        "$parameter.tolerances.financial.usd": 1.0,
        "$mean": 6.5e-1,
        "$parameter.monthly_taxi_fairs.mean_values.value": [
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
        "$parameter.monthly_taxi_fairs.mean_values.details": {
            "confidence": "low",
        },
        "$parameter.daily_taxi_fairs.mean_values.value": {
            "sunday": 71.43,
            "monday": 74.35,
            "tuesday": 42.3,
            "wednesday": 42.3,
            "thursday": 82.2,
            "friday": 78.78,
            "saturday": 91.39,
        },
        "$parameter.daily_taxi_fairs.mean_values.details": {
            "confidence": "medium",
        },
        "$parameter.weekly_taxi_fairs.mean_values.value": [
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
        "$parameter.weekly_taxi_fairs.mean_values.details": {
            "confidence": "high",
        },
    }

    return parameter_values


@pytest.fixture
def variables_multi_part_name_parameter_container():
    skip_if_python_below_minimum_version()

    variables_multi_part_name_parameter_node: ParameterNode = ParameterNode(
        {
            "false_positive_threshold": 1.0e-2,
        }
    )
    root_variables_node: ParameterNode = ParameterNode(
        {
            "variables": variables_multi_part_name_parameter_node,  # $variables.false_positive_threshold
        }
    )
    variables: ParameterContainer = ParameterContainer(
        parameter_nodes={
            "variables": root_variables_node,
        }
    )
    return variables


@pytest.fixture
def rule_without_parameters(
    empty_data_context,
):
    skip_if_python_below_minimum_version()

    rule: Rule = Rule(
        name="rule_with_no_variables_no_parameters",
        domain_builder=ColumnDomainBuilder(data_context=empty_data_context),
        expectation_configuration_builders=[
            DefaultExpectationConfigurationBuilder(
                expectation_type="expect_my_validation"
            )
        ],
    )
    return rule


# noinspection PyPep8Naming
@pytest.fixture
def rule_with_parameters(
    empty_data_context,
    column_Age_domain,
    column_Date_domain,
    variables_multi_part_name_parameter_container,
    single_part_name_parameter_container,
    multi_part_name_parameter_container,
):
    skip_if_python_below_minimum_version()

    rule: Rule = Rule(
        name="rule_with_parameters",
        domain_builder=ColumnDomainBuilder(data_context=empty_data_context),
        expectation_configuration_builders=[
            DefaultExpectationConfigurationBuilder(
                expectation_type="expect_my_validation"
            )
        ],
    )
    rule._parameters = {
        column_Age_domain.id: single_part_name_parameter_container,
        column_Date_domain.id: multi_part_name_parameter_container,
    }
    return rule


@pytest.fixture
def profiler_with_placeholder_args(
    empty_data_context,
    profiler_config_with_placeholder_args,
):
    skip_if_python_below_minimum_version()

    profiler_config_dict: dict = profiler_config_with_placeholder_args.to_json_dict()
    profiler_config_dict.pop("class_name")
    profiler_config_dict.pop("module_name")
    profiler: RuleBasedProfiler = RuleBasedProfiler(
        **profiler_config_dict, data_context=empty_data_context
    )
    return profiler
