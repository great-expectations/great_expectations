from typing import Any, Dict, Optional

import numpy as np
import pytest

from great_expectations import DataContext
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.helpers.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    MetricMultiBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder.mean_unexpected_map_metric_multi_batch_parameter_builder import (
    MeanUnexpectedMapMetricMultiBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer
from tests.rule_based_profiler.conftest import ATOL, RTOL


def test_instantiation_mean_unexpected_map_metric_multi_batch_parameter_builder(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    _: MeanUnexpectedMapMetricMultiBatchParameterBuilder = (
        MeanUnexpectedMapMetricMultiBatchParameterBuilder(
            name="my_name",
            map_metric_name="column_values.nonnull",
            total_count_parameter_builder_name="my_total_count",
            data_context=data_context,
        )
    )


def test_instantiation_mean_unexpected_map_metric_multi_batch_parameter_builder_required_arguments_absent(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    with pytest.raises(TypeError) as excinfo:
        _: MeanUnexpectedMapMetricMultiBatchParameterBuilder = (
            MeanUnexpectedMapMetricMultiBatchParameterBuilder(
                name="my_name",
                map_metric_name="column_values.nonnull",
                data_context=data_context,
            )
        )

    assert (
        "__init__() missing 1 required positional argument: 'total_count_parameter_builder_name'"
        in str(excinfo.value)
    )

    with pytest.raises(TypeError) as excinfo:
        _: MeanUnexpectedMapMetricMultiBatchParameterBuilder = (
            MeanUnexpectedMapMetricMultiBatchParameterBuilder(
                name="my_name",
                total_count_parameter_builder_name="my_total_count",
                data_context=data_context,
            )
        )

    assert (
        "__init__() missing 1 required positional argument: 'map_metric_name'"
        in str(excinfo.value)
    )


def test_mean_unexpected_map_metric_multi_batch_parameter_builder_bobby_numeric(
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

    metric_domain_kwargs_for_parameter_builder: str = "$domain.domain_kwargs"

    my_total_count_metric_multi_batch_parameter_builder: MetricMultiBatchParameterBuilder = MetricMultiBatchParameterBuilder(
        name="my_total_count",
        metric_name="table.row_count",
        metric_domain_kwargs=metric_domain_kwargs_for_parameter_builder,
        metric_value_kwargs=None,
        batch_list=None,
        batch_request=batch_request,
        json_serialize=False,
        data_context=data_context,
    )
    my_null_count_metric_multi_batch_parameter_builder: MetricMultiBatchParameterBuilder = MetricMultiBatchParameterBuilder(
        name="my_null_count",
        metric_name="column_values.nonnull.unexpected_count",
        metric_domain_kwargs=metric_domain_kwargs_for_parameter_builder,
        metric_value_kwargs=None,
        batch_list=None,
        batch_request=batch_request,
        json_serialize=False,
        data_context=data_context,
    )

    mean_unexpected_map_metric_multi_batch_parameter_builder: MeanUnexpectedMapMetricMultiBatchParameterBuilder = MeanUnexpectedMapMetricMultiBatchParameterBuilder(
        name="my_passenger_count_values_not_null_mean_unexpected_map_metric",
        map_metric_name="column_values.nonnull",
        total_count_parameter_builder_name="my_total_count",
        null_count_parameter_builder_name="my_null_count",
        metric_domain_kwargs=metric_domain_kwargs_for_parameter_builder,
        metric_value_kwargs=None,
        batch_list=None,
        batch_request=batch_request,
        json_serialize=False,
        data_context=data_context,
    )

    metric_domain_kwargs: dict = {"column": "passenger_count"}
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)

    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }
    variables: Optional[ParameterContainer] = None

    my_total_count_metric_multi_batch_parameter_builder.build_parameters(
        parameter_container=parameter_container,
        domain=domain,
        variables=variables,
        parameters=parameters,
    )
    my_null_count_metric_multi_batch_parameter_builder.build_parameters(
        parameter_container=parameter_container,
        domain=domain,
        variables=variables,
        parameters=parameters,
    )

    mean_unexpected_map_metric_multi_batch_parameter_builder.build_parameters(
        parameter_container=parameter_container,
        domain=domain,
        variables=variables,
        parameters=parameters,
    )

    expected_parameter_value: float = 0.0

    actual_parameter_value: Optional[
        Any
    ] = get_parameter_value_and_validate_return_type(
        domain=domain,
        parameter_reference=mean_unexpected_map_metric_multi_batch_parameter_builder.fully_qualified_parameter_name,
        expected_return_type=None,
        variables=variables,
        parameters=parameters,
    )

    rtol: float = RTOL
    atol: float = 5.0e-1 * ATOL
    np.testing.assert_allclose(
        actual=actual_parameter_value.value,
        desired=expected_parameter_value,
        rtol=rtol,
        atol=atol,
        err_msg=f"Actual value of {actual_parameter_value.value} differs from expected value of {expected_parameter_value} by more than {atol + rtol * abs(actual_parameter_value.value)} tolerance.",
    )


def test_mean_unexpected_map_metric_multi_batch_parameter_builder_bobby_datetime(
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

    metric_domain_kwargs_for_parameter_builder: str = "$domain.domain_kwargs"

    my_total_count_metric_multi_batch_parameter_builder: MetricMultiBatchParameterBuilder = MetricMultiBatchParameterBuilder(
        name="my_total_count",
        metric_name="table.row_count",
        metric_domain_kwargs=metric_domain_kwargs_for_parameter_builder,
        metric_value_kwargs=None,
        batch_list=None,
        batch_request=batch_request,
        json_serialize=False,
        data_context=data_context,
    )
    my_null_count_metric_multi_batch_parameter_builder: MetricMultiBatchParameterBuilder = MetricMultiBatchParameterBuilder(
        name="my_null_count",
        metric_name="column_values.nonnull.unexpected_count",
        metric_domain_kwargs=metric_domain_kwargs_for_parameter_builder,
        metric_value_kwargs=None,
        batch_list=None,
        batch_request=batch_request,
        json_serialize=False,
        data_context=data_context,
    )

    mean_unexpected_map_metric_multi_batch_parameter_builder: MeanUnexpectedMapMetricMultiBatchParameterBuilder = MeanUnexpectedMapMetricMultiBatchParameterBuilder(
        name="my_pickup_datetime_count_values_unique_mean_unexpected_map_metric",
        map_metric_name="column_values.unique",
        total_count_parameter_builder_name="my_total_count",
        null_count_parameter_builder_name="my_null_count",
        metric_domain_kwargs=metric_domain_kwargs_for_parameter_builder,
        metric_value_kwargs=None,
        batch_list=None,
        batch_request=batch_request,
        json_serialize=False,
        data_context=data_context,
    )

    metric_domain_kwargs: dict = {"column": "pickup_datetime"}
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)

    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }
    variables: Optional[ParameterContainer] = None

    my_total_count_metric_multi_batch_parameter_builder.build_parameters(
        parameter_container=parameter_container,
        domain=domain,
        variables=variables,
        parameters=parameters,
    )
    my_null_count_metric_multi_batch_parameter_builder.build_parameters(
        parameter_container=parameter_container,
        domain=domain,
        variables=variables,
        parameters=parameters,
    )

    mean_unexpected_map_metric_multi_batch_parameter_builder.build_parameters(
        parameter_container=parameter_container,
        domain=domain,
        variables=variables,
        parameters=parameters,
    )

    expected_parameter_value: float = 3.89e-3

    actual_parameter_value: Optional[
        Any
    ] = get_parameter_value_and_validate_return_type(
        domain=domain,
        parameter_reference=mean_unexpected_map_metric_multi_batch_parameter_builder.fully_qualified_parameter_name,
        expected_return_type=None,
        variables=variables,
        parameters=parameters,
    )

    rtol: float = RTOL
    atol: float = 5.0e-1 * ATOL
    np.testing.assert_allclose(
        actual=actual_parameter_value.value,
        desired=expected_parameter_value,
        rtol=rtol,
        atol=atol,
        err_msg=f"Actual value of {actual_parameter_value.value} differs from expected value of {expected_parameter_value} by more than {atol + rtol * abs(actual_parameter_value.value)} tolerance.",
    )
