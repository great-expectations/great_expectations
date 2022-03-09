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
        sampling_method="bootstrap",
        false_positive_rate=1.0e-2,
        round_decimals=0,
        data_context=data_context,
        batch_request=batch_request,
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.TABLE,
    )

    assert parameter_container.parameter_nodes is None

    numeric_metric_range_parameter_builder.build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    parameter_nodes: Optional[Dict[str, ParameterNode]] = (
        parameter_container.parameter_nodes or {}
    )
    assert len(parameter_nodes) == 1

    fully_qualified_parameter_name_for_value: str = "$parameter.row_count_range"
    expected_value_dict: dict = {
        "value": {"value_range": None},
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
        parameters={domain.id: parameter_container},
    )

    actual_value = actual_value_dict.pop("value").pop("value_range")
    actual_value_dict["value"] = {"value_range": None}

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
