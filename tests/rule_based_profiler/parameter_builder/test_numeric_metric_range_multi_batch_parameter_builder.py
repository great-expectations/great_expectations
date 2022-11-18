import re
from typing import Dict, List, Optional

import numpy as np
import pytest
import scipy.stats as stats

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.data_context import DataContext
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.domain import Domain
from great_expectations.rule_based_profiler.helpers.util import NP_EPSILON
from great_expectations.rule_based_profiler.parameter_builder import (
    NumericMetricRangeMultiBatchParameterBuilder,
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_container import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
    ParameterContainer,
    ParameterNode,
    get_parameter_value_by_fully_qualified_parameter_name,
)


@pytest.mark.integration
def test_bootstrap_numeric_metric_range_multi_batch_parameter_builder_bobby(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    # BatchRequest yielding three batches
    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    numeric_metric_range_parameter_builder: ParameterBuilder = (
        NumericMetricRangeMultiBatchParameterBuilder(
            name="row_count_range",
            metric_name="table.row_count",
            metric_multi_batch_parameter_builder_name=None,
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            estimator="bootstrap",
            include_estimator_samples_histogram_in_details=True,
            false_positive_rate=1.0e-2,
            round_decimals=0,
            evaluation_parameter_builder_configs=None,
            data_context=data_context,
        )
    )

    variables: Optional[ParameterContainer] = None

    domain = Domain(
        domain_type=MetricDomainTypes.TABLE,
        rule_name="my_rule",
    )
    parameter_container = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    assert parameter_container.parameter_nodes is None

    numeric_metric_range_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=batch_request,
    )

    parameter_nodes: Optional[Dict[str, ParameterNode]] = (
        parameter_container.parameter_nodes or {}
    )
    assert len(parameter_nodes) == 1

    fully_qualified_parameter_name_for_value: str = "$parameter.row_count_range"
    expected_value_dict: Dict[str, Optional[str]] = {
        "value": None,
        "details": {
            "metric_configuration": {
                "domain_kwargs": {},
                "metric_name": "table.row_count",
                "metric_value_kwargs": None,
            },
            "num_batches": 3,
        },
    }

    parameter_node: ParameterNode = (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
            domain=domain,
            parameters=parameters,
        )
    )

    actual_value: np.ndarray = parameter_node.pop("value")
    parameter_node["value"] = None

    actual_estimation_histogram: np.ndarray = parameter_node.details.pop(
        "estimation_histogram"
    )

    assert parameter_node == expected_value_dict

    expected_value: np.ndarray = np.asarray([7510, 8806])

    # Measure of "closeness" between "actual" and "desired" is computed as: atol + rtol * abs(desired)
    # (see "https://numpy.org/doc/stable/reference/generated/numpy.testing.assert_allclose.html" for details).
    rtol: float = 1.0e-2
    atol: float = 0

    # bootstrap results should be stable +/- 1%
    np.testing.assert_allclose(
        actual=actual_value,
        desired=expected_value,
        rtol=rtol,
        atol=atol,
        err_msg=f"Actual value of {actual_value} differs from expected value of {expected_value} by more than {atol + rtol * abs(expected_value)} tolerance.",
    )

    expected_estimation_histogram: np.ndarray = np.asarray(
        [
            1.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            1.0,
            0.0,
            0.0,
            1.0,
            0.0,
        ]
    )

    # Assert no significant difference between expected (null hypothesis) and actual estimation histograms.
    ks_result: tuple = stats.ks_2samp(
        data1=actual_estimation_histogram[0], data2=expected_estimation_histogram
    )
    p_value: float = ks_result[1]
    assert p_value > 9.5e-1


@pytest.mark.integration
@pytest.mark.slow  # 1.10s
def test_quantiles_numeric_metric_range_multi_batch_parameter_builder_bobby(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    metric_domain_kwargs: dict = {"column": "fare_amount"}

    fully_qualified_parameter_name_for_value: str = "$parameter.column_min_range"

    numeric_metric_range_parameter_builder: ParameterBuilder = (
        NumericMetricRangeMultiBatchParameterBuilder(
            name="column_min_range",
            metric_name="column.min",
            metric_multi_batch_parameter_builder_name=None,
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            estimator="quantiles",
            include_estimator_samples_histogram_in_details=True,
            false_positive_rate=1.0e-2,
            round_decimals=1,
            evaluation_parameter_builder_configs=None,
            data_context=data_context,
        )
    )

    variables: Optional[ParameterContainer] = None

    domain = Domain(
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs=metric_domain_kwargs,
        rule_name="my_rule",
    )
    parameter_container = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    assert parameter_container.parameter_nodes is None

    numeric_metric_range_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=batch_request,
    )

    parameter_nodes: Optional[Dict[str, ParameterNode]] = (
        parameter_container.parameter_nodes or {}
    )
    assert len(parameter_nodes) == 1

    expected_value_dict: Dict[str, Optional[str]] = {
        "value": None,
        "details": {
            "metric_configuration": {
                "domain_kwargs": {"column": "fare_amount"},
                "metric_name": "column.min",
                "metric_value_kwargs": None,
            },
            "num_batches": 3,
        },
    }

    parameter_node: ParameterNode = (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
            domain=domain,
            parameters=parameters,
        )
    )

    actual_values_01: np.ndarray = parameter_node.pop("value")
    parameter_node["value"] = None

    actual_estimation_histogram: np.ndarray = parameter_node.details.pop(
        "estimation_histogram"
    )

    assert parameter_node == expected_value_dict

    actual_value_01_lower: float = actual_values_01[0]
    actual_value_01_upper: float = actual_values_01[1]
    expected_value_01_lower: float = -51.7
    expected_value_01_upper: float = -21.0

    assert actual_value_01_lower == expected_value_01_lower
    assert actual_value_01_upper == expected_value_01_upper

    expected_estimation_histogram: np.ndarray = np.asarray(
        [
            1.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            2.0,
            0.0,
        ]
    )

    # Assert no significant difference between expected (null hypothesis) and actual estimation histograms.
    ks_result: tuple = stats.ks_2samp(
        data1=actual_estimation_histogram[0], data2=expected_estimation_histogram
    )
    p_value: float = ks_result[1]
    assert p_value > 9.5e-1

    numeric_metric_range_parameter_builder = (
        NumericMetricRangeMultiBatchParameterBuilder(
            name="column_min_range",
            metric_name="column.min",
            metric_multi_batch_parameter_builder_name=None,
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            estimator="quantiles",
            include_estimator_samples_histogram_in_details=True,
            false_positive_rate=5.0e-2,
            round_decimals=1,
            evaluation_parameter_builder_configs=None,
            data_context=data_context,
        )
    )

    numeric_metric_range_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        recompute_existing_parameter_values=True,
        batch_request=batch_request,
    )

    parameter_node: ParameterNode = (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
            domain=domain,
            parameters=parameters,
        )
    )

    actual_values_05 = parameter_node.pop("value")
    parameter_node["value"] = None

    actual_estimation_histogram: np.ndarray = parameter_node.details.pop(
        "estimation_histogram"
    )

    assert parameter_node == expected_value_dict

    actual_value_05_lower: float = actual_values_05[0]
    actual_value_05_upper: float = actual_values_05[1]
    expected_value_05_lower: float = -50.5
    expected_value_05_upper: float = -21.1

    assert actual_value_05_lower == expected_value_05_lower
    assert actual_value_05_upper == expected_value_05_upper

    # if false positive rate is higher, our range should be more narrow
    assert actual_value_01_lower < actual_value_05_lower
    assert actual_value_01_upper > actual_value_05_upper

    expected_estimation_histogram: np.ndarray = np.asarray(
        [
            1.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            2.0,
            0.0,
        ]
    )

    # Assert no significant difference between expected (null hypothesis) and actual estimation histograms.
    ks_result = stats.ks_2samp(
        data1=actual_estimation_histogram[0], data2=expected_estimation_histogram
    )
    p_value: float = ks_result[1]
    assert p_value > 9.5e-1


@pytest.mark.integration
def test_exact_numeric_metric_range_multi_batch_parameter_builder_bobby(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    metric_domain_kwargs: dict = {"column": "fare_amount"}

    fully_qualified_parameter_name_for_value: str = "$parameter.column_min_range"

    numeric_metric_range_parameter_builder: ParameterBuilder = (
        NumericMetricRangeMultiBatchParameterBuilder(
            name="column_min_range",
            metric_name="column.min",
            metric_multi_batch_parameter_builder_name=None,
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            estimator="exact",
            include_estimator_samples_histogram_in_details=True,
            false_positive_rate=None,
            round_decimals=1,
            evaluation_parameter_builder_configs=None,
            data_context=data_context,
        )
    )

    variables: Optional[ParameterContainer] = None

    domain = Domain(
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs=metric_domain_kwargs,
        rule_name="my_rule",
    )
    parameter_container = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    assert parameter_container.parameter_nodes is None

    numeric_metric_range_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=batch_request,
    )

    parameter_nodes: Optional[Dict[str, ParameterNode]] = (
        parameter_container.parameter_nodes or {}
    )
    assert len(parameter_nodes) == 1

    expected_value_dict: Dict[str, Optional[str]] = {
        "value": None,
        "details": {
            "metric_configuration": {
                "domain_kwargs": {"column": "fare_amount"},
                "metric_name": "column.min",
                "metric_value_kwargs": None,
            },
            "num_batches": 3,
        },
    }

    parameter_node: ParameterNode = (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
            domain=domain,
            parameters=parameters,
        )
    )

    actual_values_01: np.ndarray = parameter_node.pop("value")
    parameter_node["value"] = None

    actual_estimation_histogram: np.ndarray = parameter_node.details.pop(
        "estimation_histogram"
    )

    assert parameter_node == expected_value_dict

    actual_value_01_lower: float = actual_values_01[0]
    actual_value_01_upper: float = actual_values_01[1]
    expected_value_01_lower: float = -52.0
    expected_value_01_upper: float = -21.0

    assert actual_value_01_lower == expected_value_01_lower
    assert actual_value_01_upper == expected_value_01_upper

    expected_estimation_histogram: np.ndarray = np.asarray(
        [
            1.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            2.0,
            0.0,
        ],
    )

    # Assert no significant difference between expected (null hypothesis) and actual estimation histograms.
    ks_result: tuple = stats.ks_2samp(
        data1=actual_estimation_histogram[0], data2=expected_estimation_histogram
    )
    p_value: float = ks_result[1]
    assert p_value > 9.5e-1


@pytest.mark.integration
def test_quantiles_numeric_metric_range_multi_batch_parameter_builder_with_evaluation_dependency_bobby(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    batch_request: Dict[str, str] = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    metric_domain_kwargs: Dict[str, str] = {"column": "fare_amount"}

    fully_qualified_parameter_name_for_value: str = "$parameter.column_min_range"

    my_column_min_metric_multi_batch_parameter_builder_config = ParameterBuilderConfig(
        module_name="great_expectations.rule_based_profiler.parameter_builder",
        class_name="MetricMultiBatchParameterBuilder",
        name="my_column_min",
        metric_name="column.min",
        metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
        metric_value_kwargs=None,
        enforce_numeric_metric=True,
        replace_nan_with_zero=True,
        reduce_scalar_metric=True,
        evaluation_parameter_builder_configs=None,
    )
    evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]] = [
        my_column_min_metric_multi_batch_parameter_builder_config,
    ]
    numeric_metric_range_parameter_builder: ParameterBuilder = (
        NumericMetricRangeMultiBatchParameterBuilder(
            name="column_min_range",
            metric_name=None,
            metric_multi_batch_parameter_builder_name="my_column_min",
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            estimator="quantiles",
            include_estimator_samples_histogram_in_details=True,
            false_positive_rate=1.0e-2,
            round_decimals=1,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            data_context=data_context,
        )
    )

    variables: Optional[ParameterContainer] = None

    domain = Domain(
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs=metric_domain_kwargs,
        rule_name="my_rule",
    )
    parameter_container = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    assert parameter_container.parameter_nodes is None

    numeric_metric_range_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=batch_request,
    )

    parameter_nodes: Optional[Dict[str, ParameterNode]] = (
        parameter_container.parameter_nodes or {}
    )
    assert len(parameter_nodes) == 1

    expected_value_dict: Dict[str, Optional[str]] = {
        "value": None,
        "details": {
            "metric_configuration": {
                "domain_kwargs": {"column": "fare_amount"},
                "metric_name": "column.min",
                "metric_value_kwargs": None,
            },
            "num_batches": 3,
        },
    }

    parameter_node: ParameterNode = (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
            domain=domain,
            parameters=parameters,
        )
    )

    actual_values_01: np.ndarray = parameter_node.pop("value")
    parameter_node["value"] = None

    actual_estimation_histogram: np.ndarray = parameter_node.details.pop(
        "estimation_histogram"
    )

    assert parameter_node == expected_value_dict

    actual_value_01_lower: float = actual_values_01[0]
    actual_value_01_upper: float = actual_values_01[1]
    expected_value_01_lower: float = -51.7
    expected_value_01_upper: float = -21.0

    assert actual_value_01_lower == expected_value_01_lower
    assert actual_value_01_upper == expected_value_01_upper

    expected_estimation_histogram: np.ndarray = np.asarray(
        [
            1.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            2.0,
            0.0,
        ]
    )

    # Assert no significant difference between expected (null hypothesis) and actual estimation histograms.
    ks_result: tuple = stats.ks_2samp(
        data1=actual_estimation_histogram[0], data2=expected_estimation_histogram
    )
    p_value: float = ks_result[1]
    assert p_value > 9.5e-1

    numeric_metric_range_parameter_builder = (
        NumericMetricRangeMultiBatchParameterBuilder(
            name="column_min_range",
            metric_name="column.min",
            metric_multi_batch_parameter_builder_name="my_column_min",
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            estimator="quantiles",
            include_estimator_samples_histogram_in_details=True,
            false_positive_rate=5.0e-2,
            round_decimals=1,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            data_context=data_context,
        )
    )

    numeric_metric_range_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        recompute_existing_parameter_values=True,
        batch_request=batch_request,
    )

    parameter_node: ParameterNode = (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
            domain=domain,
            parameters=parameters,
        )
    )

    actual_values_05 = parameter_node.pop("value")
    parameter_node["value"] = None

    actual_estimation_histogram: np.ndarray = parameter_node.details.pop(
        "estimation_histogram"
    )

    assert parameter_node == expected_value_dict

    actual_value_05_lower: float = actual_values_05[0]
    actual_value_05_upper: float = actual_values_05[1]
    expected_value_05_lower: float = -50.5
    expected_value_05_upper: float = -21.1

    assert actual_value_05_lower == expected_value_05_lower
    assert actual_value_05_upper == expected_value_05_upper

    # if false positive rate is higher, our range should be more narrow
    assert actual_value_01_lower < actual_value_05_lower
    assert actual_value_01_upper > actual_value_05_upper

    expected_estimation_histogram: np.ndarray = np.asarray(
        [
            1.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            2.0,
            0.0,
        ]
    )

    # Assert no significant difference between expected (null hypothesis) and actual estimation histograms.
    ks_result = stats.ks_2samp(
        data1=actual_estimation_histogram[0], data2=expected_estimation_histogram
    )
    p_value: float = ks_result[1]
    assert p_value > 9.5e-1


@pytest.mark.integration
def test_bootstrap_numeric_metric_range_multi_batch_parameter_builder_bobby_false_positive_rate_one(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    # BatchRequest yielding three batches
    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    numeric_metric_range_parameter_builder: ParameterBuilder = (
        NumericMetricRangeMultiBatchParameterBuilder(
            name="row_count_range",
            metric_name="table.row_count",
            metric_multi_batch_parameter_builder_name=None,
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            estimator="bootstrap",
            false_positive_rate=1.0,
            round_decimals=0,
            evaluation_parameter_builder_configs=None,
            data_context=data_context,
        )
    )

    variables: Optional[ParameterContainer] = None

    domain = Domain(
        domain_type=MetricDomainTypes.TABLE,
        rule_name="my_rule",
    )
    parameter_container = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    assert parameter_container.parameter_nodes is None

    error_message: str = re.escape(
        f"""You have chosen a false_positive_rate of 1.0, which is too close to 1.  A false_positive_rate of \
{1 - NP_EPSILON} has been selected instead.
"""
    )

    with pytest.warns(UserWarning, match=error_message):
        numeric_metric_range_parameter_builder.build_parameters(
            domain=domain,
            variables=variables,
            parameters=parameters,
            batch_request=batch_request,
        )


@pytest.mark.integration
def test_bootstrap_numeric_metric_range_multi_batch_parameter_builder_bobby_false_positive_rate_negative(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    # BatchRequest yielding three batches
    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    numeric_metric_range_parameter_builder: ParameterBuilder = (
        NumericMetricRangeMultiBatchParameterBuilder(
            name="row_count_range",
            metric_name="table.row_count",
            metric_multi_batch_parameter_builder_name=None,
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            estimator="bootstrap",
            false_positive_rate=-0.05,
            round_decimals=0,
            evaluation_parameter_builder_configs=None,
            data_context=data_context,
        )
    )

    variables: Optional[ParameterContainer] = None

    domain = Domain(
        domain_type=MetricDomainTypes.TABLE,
        rule_name="my_rule",
    )
    parameter_container = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    assert parameter_container.parameter_nodes is None

    error_message: str = re.escape(
        """false_positive_rate must be a positive decimal number between 0 and 1 inclusive [0, 1], but -0.05 was \
provided.
"""
    )

    with pytest.raises(ge_exceptions.ProfilerExecutionError, match=error_message):
        numeric_metric_range_parameter_builder.build_parameters(
            domain=domain,
            variables=variables,
            parameters=parameters,
            batch_request=batch_request,
        )


@pytest.mark.integration
@pytest.mark.slow  # 2.51s
def test_bootstrap_numeric_metric_range_multi_batch_parameter_builder_bobby_false_positive_rate_zero(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    # BatchRequest yielding three batches
    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    numeric_metric_range_parameter_builder: ParameterBuilder = (
        NumericMetricRangeMultiBatchParameterBuilder(
            name="row_count_range",
            metric_name="table.row_count",
            metric_multi_batch_parameter_builder_name=None,
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            estimator="bootstrap",
            false_positive_rate=0.0,
            round_decimals=0,
            evaluation_parameter_builder_configs=None,
            data_context=data_context,
        )
    )

    variables: Optional[ParameterContainer] = None

    domain = Domain(
        domain_type=MetricDomainTypes.TABLE,
        rule_name="my_rule",
    )
    parameter_container = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    assert parameter_container.parameter_nodes is None

    warning_message: str = re.escape(
        f"""You have chosen a false_positive_rate of 0.0, which is too close to 0.  A false_positive_rate of \
{NP_EPSILON} has been selected instead.
"""
    )

    with pytest.warns(UserWarning, match=warning_message):
        numeric_metric_range_parameter_builder.build_parameters(
            domain=domain,
            variables=variables,
            parameters=parameters,
            batch_request=batch_request,
        )


@pytest.mark.integration
def test_bootstrap_numeric_metric_range_multi_batch_parameter_builder_bobby_false_positive_rate_very_small(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    # BatchRequest yielding three batches
    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    # a commonly used defect rate in quality control that equates to 3.4 defects per million opportunities
    six_sigma_false_positive_rate: float = 3.4 / 1000000.0
    assert six_sigma_false_positive_rate > NP_EPSILON

    # what if user tries a false positive rate smaller than NP_EPSILON (by an order of magnitude in this case)?
    smaller_than_np_epsilon_false_positive_rate: float = NP_EPSILON / 10

    numeric_metric_range_parameter_builder: ParameterBuilder = (
        NumericMetricRangeMultiBatchParameterBuilder(
            name="row_count_range",
            metric_name="table.row_count",
            metric_multi_batch_parameter_builder_name=None,
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            estimator="bootstrap",
            false_positive_rate=smaller_than_np_epsilon_false_positive_rate,
            round_decimals=0,
            evaluation_parameter_builder_configs=None,
            data_context=data_context,
        )
    )

    variables: Optional[ParameterContainer] = None

    domain = Domain(
        domain_type=MetricDomainTypes.TABLE,
        rule_name="my_rule",
    )
    parameter_container = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    assert parameter_container.parameter_nodes is None

    warning_message: str = re.escape(
        f"""You have chosen a false_positive_rate of {smaller_than_np_epsilon_false_positive_rate}, which is too close \
to 0.  A false_positive_rate of {NP_EPSILON} has been selected instead.
"""
    )

    with pytest.warns(UserWarning, match=warning_message):
        numeric_metric_range_parameter_builder.build_parameters(
            domain=domain,
            variables=variables,
            parameters=parameters,
            batch_request=batch_request,
        )


@pytest.mark.integration
def test_kde_numeric_metric_range_multi_batch_parameter_builder_bobby(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    # BatchRequest yielding three batches
    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    numeric_metric_range_parameter_builder: ParameterBuilder = (
        NumericMetricRangeMultiBatchParameterBuilder(
            name="row_count_range",
            metric_name="table.row_count",
            metric_multi_batch_parameter_builder_name=None,
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            estimator="kde",
            include_estimator_samples_histogram_in_details=True,
            false_positive_rate=1.0e-2,
            round_decimals=0,
            evaluation_parameter_builder_configs=None,
            data_context=data_context,
        )
    )

    variables: Optional[ParameterContainer] = None

    domain = Domain(
        rule_name="my_rule",
        domain_type=MetricDomainTypes.TABLE,
    )
    parameter_container = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    assert parameter_container.parameter_nodes is None

    numeric_metric_range_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=batch_request,
    )

    parameter_nodes: Optional[Dict[str, ParameterNode]] = (
        parameter_container.parameter_nodes or {}
    )
    assert len(parameter_nodes) == 1

    fully_qualified_parameter_name_for_value: str = "$parameter.row_count_range"
    expected_value_dict: Dict[str, Optional[str]] = {
        "value": None,
        "details": {
            "metric_configuration": {
                "domain_kwargs": {},
                "metric_name": "table.row_count",
                "metric_value_kwargs": None,
            },
            "num_batches": 3,
        },
    }

    parameter_node: ParameterNode = (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
            domain=domain,
            parameters=parameters,
        )
    )

    actual_value: np.ndarray = parameter_node.pop("value")
    parameter_node["value"] = None

    actual_estimation_histogram: np.ndarray = parameter_node.details.pop(
        "estimation_histogram"
    )

    assert parameter_node == expected_value_dict

    expected_value: np.ndarray = np.asarray([6180, 10277])

    # Measure of "closeness" between "actual" and "desired" is computed as: atol + rtol * abs(desired)
    # (see "https://numpy.org/doc/stable/reference/generated/numpy.testing.assert_allclose.html" for details).
    rtol: float = 1.0e-2
    atol: float = 0

    # kde results should be stable +/- 1%
    np.testing.assert_allclose(
        actual=actual_value,
        desired=expected_value,
        rtol=rtol,
        atol=atol,
        err_msg=f"Actual value of {actual_value} differs from expected value of {expected_value} by more than {atol + rtol * abs(expected_value)} tolerance.",
    )

    expected_estimation_histogram: np.ndarray = np.asarray(
        [
            1.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            1.0,
            0.0,
            0.0,
            1.0,
            0.0,
        ]
    )

    # Assert no significant difference between expected (null hypothesis) and actual estimation histograms.
    ks_result: tuple = stats.ks_2samp(
        data1=actual_estimation_histogram[0], data2=expected_estimation_histogram
    )
    p_value: float = ks_result[1]
    assert p_value > 9.5e-1


@pytest.mark.integration
@pytest.mark.slow  # 1.12s
def test_numeric_metric_range_multi_batch_parameter_builder_bobby_kde_vs_bootstrap_marginal_info_at_boundary(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    """
    This tests whether kde gives a wider estimate for the max
    """

    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    # BatchRequest yielding three batches
    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    metric_domain_kwargs: dict = {"column": "fare_amount"}

    numeric_metric_range_parameter_builder: ParameterBuilder = (
        NumericMetricRangeMultiBatchParameterBuilder(
            name="column_max_range",
            metric_name="column.max",
            metric_multi_batch_parameter_builder_name=None,
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            estimator="bootstrap",
            false_positive_rate=5.0e-2,
            round_decimals=0,
            evaluation_parameter_builder_configs=None,
            data_context=data_context,
        )
    )

    variables: Optional[ParameterContainer] = None

    domain = Domain(
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs=metric_domain_kwargs,
        rule_name="my_rule",
    )
    parameter_container = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    assert parameter_container.parameter_nodes is None

    numeric_metric_range_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=batch_request,
    )

    parameter_nodes: Optional[Dict[str, ParameterNode]] = (
        parameter_container.parameter_nodes or {}
    )
    assert len(parameter_nodes) == 1

    fully_qualified_parameter_name_for_value: str = "$parameter.column_max_range"

    parameter_node: ParameterNode = (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
            domain=domain,
            parameters=parameters,
        )
    )

    bootstrap_value: np.ndarray = parameter_node.pop("value")

    numeric_metric_range_parameter_builder: ParameterBuilder = (
        NumericMetricRangeMultiBatchParameterBuilder(
            name="column_max_range",
            metric_name="column.max",
            metric_multi_batch_parameter_builder_name=None,
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            estimator="kde",
            false_positive_rate=5.0e-2,
            round_decimals=0,
            evaluation_parameter_builder_configs=None,
            data_context=data_context,
        )
    )

    parameter_container = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    assert parameter_container.parameter_nodes is None

    numeric_metric_range_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=batch_request,
    )

    parameter_nodes: Optional[Dict[str, ParameterNode]] = (
        parameter_container.parameter_nodes or {}
    )
    assert len(parameter_nodes) == 1

    fully_qualified_parameter_name_for_value: str = "$parameter.column_max_range"

    parameter_node: ParameterNode = (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
            domain=domain,
            parameters=parameters,
        )
    )

    kde_value: np.ndarray = parameter_node.pop("value")

    assert kde_value[1] > bootstrap_value[1]


@pytest.mark.integration
@pytest.mark.slow  # 1.12s
def test_numeric_metric_range_multi_batch_parameter_builder_bobby_kde_bw_method(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    """
    This tests whether a change to bw_method results in a change to the range
    """

    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    # BatchRequest yielding three batches
    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    metric_domain_kwargs: dict = {"column": "fare_amount"}

    numeric_metric_range_parameter_builder: ParameterBuilder = (
        NumericMetricRangeMultiBatchParameterBuilder(
            name="column_min_range",
            metric_name="column.min",
            metric_multi_batch_parameter_builder_name=None,
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            estimator="kde",
            false_positive_rate=5.0e-2,
            round_decimals=0,
            evaluation_parameter_builder_configs=None,
            data_context=data_context,
        )
    )

    variables: Optional[ParameterContainer] = None

    domain = Domain(
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs=metric_domain_kwargs,
        rule_name="my_rule",
    )
    parameter_container = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    assert parameter_container.parameter_nodes is None

    numeric_metric_range_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=batch_request,
    )

    parameter_nodes: Optional[Dict[str, ParameterNode]] = (
        parameter_container.parameter_nodes or {}
    )
    assert len(parameter_nodes) == 1

    fully_qualified_parameter_name_for_value: str = "$parameter.column_min_range"

    parameter_node: ParameterNode = (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
            domain=domain,
            parameters=parameters,
        )
    )

    default_bw_method_value: np.ndarray = parameter_node.pop("value")

    numeric_metric_range_parameter_builder: ParameterBuilder = (
        NumericMetricRangeMultiBatchParameterBuilder(
            name="column_min_range",
            metric_name="column.min",
            metric_multi_batch_parameter_builder_name=None,
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            estimator="kde",
            bw_method=0.5,
            false_positive_rate=5.0e-2,
            round_decimals=0,
            evaluation_parameter_builder_configs=None,
            data_context=data_context,
        )
    )

    parameter_container = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    assert parameter_container.parameter_nodes is None

    numeric_metric_range_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=batch_request,
    )

    parameter_nodes: Optional[Dict[str, ParameterNode]] = (
        parameter_container.parameter_nodes or {}
    )
    assert len(parameter_nodes) == 1

    fully_qualified_parameter_name_for_value: str = "$parameter.column_min_range"

    parameter_node: ParameterNode = (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
            domain=domain,
            parameters=parameters,
        )
    )

    other_bw_method_value: np.ndarray = parameter_node.pop("value")

    assert default_bw_method_value[0] != other_bw_method_value[0]
