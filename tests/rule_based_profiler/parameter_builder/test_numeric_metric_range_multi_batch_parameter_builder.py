from typing import Dict, Optional

import numpy as np

from great_expectations.data_context import DataContext
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.parameter_builder import (
    NumericMetricRangeMultiBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.types import (
    Domain,
    ParameterContainer,
    ParameterNode,
    get_parameter_value_by_fully_qualified_parameter_name,
)


def test_bootstrap_numeric_metric_range_multi_batch_parameter_builder_bobby(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    # BatchRequest yielding two batches (January, 2019 and February, 2019 trip data)
    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    numeric_metric_range_parameter_builder: NumericMetricRangeMultiBatchParameterBuilder = NumericMetricRangeMultiBatchParameterBuilder(
        name="row_count_range",
        metric_name="table.row_count",
        estimator="bootstrap",
        false_positive_rate=1.0e-2,
        round_decimals=0,
        data_context=data_context,
        batch_request=batch_request,
    )

    variables: Optional[ParameterContainer] = None

    domain: Domain = Domain(
        domain_type=MetricDomainTypes.TABLE,
    )
    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    assert parameter_container.parameter_nodes is None

    numeric_metric_range_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
    )

    parameter_nodes: Optional[Dict[str, ParameterNode]] = (
        parameter_container.parameter_nodes or {}
    )
    assert len(parameter_nodes) == 1

    fully_qualified_parameter_name_for_value: str = "$parameter.row_count_range"
    expected_value_dict: dict = {
        "value": None,
        "details": {
            "metric_configuration": {
                "domain_kwargs": {},
                "metric_name": "table.row_count",
                "metric_value_kwargs": None,
                "metric_dependencies": None,
            },
            "num_batches": 3,
        },
    }

    actual_value_dict: dict = get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
        domain=domain,
        parameters=parameters,
    )

    actual_value = actual_value_dict.pop("value")
    actual_value_dict["value"] = None

    assert actual_value_dict == expected_value_dict

    expected_value = np.array([7510, 8806])

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


def test_oneshot_numeric_metric_range_multi_batch_parameter_builder_bobby(
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

    numeric_metric_range_parameter_builder: NumericMetricRangeMultiBatchParameterBuilder

    fully_qualified_parameter_name_for_value: str = "$parameter.column_min_range"

    expected_value_dict: dict
    actual_value_dict: dict

    numeric_metric_range_parameter_builder = (
        NumericMetricRangeMultiBatchParameterBuilder(
            name="column_min_range",
            metric_name="column.min",
            metric_domain_kwargs=metric_domain_kwargs,
            estimator="oneshot",
            false_positive_rate=1.0e-2,
            round_decimals=1,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    variables: Optional[ParameterContainer] = None

    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs=metric_domain_kwargs,
    )
    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    assert parameter_container.parameter_nodes is None

    numeric_metric_range_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
    )

    parameter_nodes: Optional[Dict[str, ParameterNode]] = (
        parameter_container.parameter_nodes or {}
    )
    assert len(parameter_nodes) == 1

    expected_value_dict = {
        "value": None,
        "details": {
            "metric_configuration": {
                "domain_kwargs": {"column": "fare_amount"},
                "metric_name": "column.min",
                "metric_value_kwargs": None,
                "metric_dependencies": None,
            },
            "num_batches": 3,
        },
    }

    actual_value_dict = get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
        domain=domain,
        parameters=parameters,
    )

    actual_values_01 = actual_value_dict.pop("value")
    actual_value_dict["value"] = None

    assert actual_value_dict == expected_value_dict

    actual_value_01_lower: float = actual_values_01[0]
    actual_value_01_upper: float = actual_values_01[1]
    expected_value_01_lower: float = -51.7
    expected_value_01_upper: float = -21.0

    assert actual_value_01_lower == expected_value_01_lower
    assert actual_value_01_upper == expected_value_01_upper

    numeric_metric_range_parameter_builder = (
        NumericMetricRangeMultiBatchParameterBuilder(
            name="column_min_range",
            metric_name="column.min",
            metric_domain_kwargs=metric_domain_kwargs,
            estimator="oneshot",
            false_positive_rate=5.0e-2,
            round_decimals=1,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    numeric_metric_range_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        recompute_existing_parameter_values=True,
    )

    actual_value_dict = get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
        domain=domain,
        parameters=parameters,
    )

    actual_values_05 = actual_value_dict.pop("value")
    actual_value_dict["value"] = None

    assert actual_value_dict == expected_value_dict

    actual_value_05_lower: float = actual_values_05[0]
    actual_value_05_upper: float = actual_values_05[1]
    expected_value_05_lower: float = -50.5
    expected_value_05_upper: float = -21.1

    assert actual_value_05_lower == expected_value_05_lower
    assert actual_value_05_upper == expected_value_05_upper

    # if false positive rate is higher, our range should be more narrow
    assert actual_value_01_lower < actual_value_05_lower
    assert actual_value_01_upper > actual_value_05_upper
