from typing import Dict, Optional
from unittest import mock

import pandas as pd
import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.rule_based_profiler.domain import Domain
from great_expectations.rule_based_profiler.helpers.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    HistogramSingleBatchParameterBuilder,
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,
    ParameterNode,
)


@pytest.mark.integration
def test_instantiation_histogram_single_batch_parameter_builder(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    # noinspection PyUnusedLocal
    parameter_builder_0: ParameterBuilder = HistogramSingleBatchParameterBuilder(
        name="my_name_0",
        data_context=data_context,
    )


@pytest.mark.integration
def test_histogram_single_batch_parameter_builder_alice(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    parameter_builder: ParameterBuilder = HistogramSingleBatchParameterBuilder(
        name="my_name",
        bins="auto",
        evaluation_parameter_builder_configs=None,
        data_context=data_context,
    )

    metric_domain_kwargs: dict = {"column": "user_id"}
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

    variables: Optional[ParameterContainer] = None
    parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=batch_request,
    )

    expected_parameter_value: dict = {
        "value": {
            "bins": [397433.0, 4942918.5, 9488404.0],
            "weights": [0.6666666666666666, 0.3333333333333333],
            "tail_weights": [0.0, 0.0],
        },
        "details": {
            "metric_configuration": {
                "metric_name": "column.histogram",
                "domain_kwargs": {"column": "user_id"},
                "metric_value_kwargs": {"bins": [397433.0, 4942918.5, 9488404.0]},
            },
            "num_batches": 1,
        },
    }

    parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
        domain=domain,
        parameter_reference=parameter_builder.json_serialized_fully_qualified_parameter_name,
        expected_return_type=None,
        variables=variables,
        parameters=parameters,
    )

    assert parameter_node == expected_parameter_value


@pytest.mark.integration
def test_histogram_single_batch_parameter_builder_alice_null_bins(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    parameter_builder: ParameterBuilder = HistogramSingleBatchParameterBuilder(
        name="my_name",
        bins="auto",
        evaluation_parameter_builder_configs=None,
        data_context=data_context,
    )

    metric_domain_kwargs: dict = {"column": "my_column"}
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

    with mock.patch(
        "great_expectations.expectations.metrics.column_aggregate_metrics.column_partition._get_column_partition_using_metrics",
        return_value=None,
    ):
        with pytest.raises(ge_exceptions.ProfilerExecutionError) as excinfo:
            variables: Optional[ParameterContainer] = None
            # noinspection PyUnusedLocal
            parameter_builder.build_parameters(
                domain=domain,
                variables=variables,
                parameters=parameters,
                batch_request=batch_request,
            )

        assert (
            "Partitioning values for HistogramSingleBatchParameterBuilder by column_partition_metric_single_batch_parameter_builder into bins encountered empty or non-existent elements."
            in str(excinfo.value)
        )


@pytest.mark.integration
def test_histogram_single_batch_parameter_builder_alice_nan_valued_bins(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    parameter_builder: ParameterBuilder = HistogramSingleBatchParameterBuilder(
        name="my_name",
        bins="auto",
        evaluation_parameter_builder_configs=None,
        data_context=data_context,
    )

    metric_domain_kwargs: dict = {"column": "my_column"}
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

    with mock.patch(
        "great_expectations.execution_engine.pandas_batch_data.PandasBatchData.dataframe",
        new_callable=mock.PropertyMock,
        return_value=pd.DataFrame(
            {
                "my_column": [
                    None,
                    None,
                    None,
                    None,
                    None,
                ],
            }
        ),
    ):
        variables: Optional[ParameterContainer] = None
        parameter_builder.build_parameters(
            domain=domain,
            variables=variables,
            parameters=parameters,
            batch_request=batch_request,
        )

        expected_parameter_value: dict = {
            "value": {"bins": [None], "weights": [], "tail_weights": [0.5, 0.5]},
            "details": {
                "metric_configuration": {
                    "metric_name": "column.histogram",
                    "domain_kwargs": {"column": "my_column"},
                    "metric_value_kwargs": {"bins": [None]},
                },
                "num_batches": 1,
            },
        }

        parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=parameter_builder.json_serialized_fully_qualified_parameter_name,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )

        assert parameter_node == expected_parameter_value


@pytest.mark.integration
def test_histogram_single_batch_parameter_builder_alice_wrong_type_bins(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    parameter_builder: ParameterBuilder = HistogramSingleBatchParameterBuilder(
        name="my_name",
        bins="auto",
        evaluation_parameter_builder_configs=None,
        data_context=data_context,
    )

    metric_domain_kwargs: dict = {"column": "event_ts"}
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

    variables: Optional[ParameterContainer] = None
    with pytest.raises(ge_exceptions.ProfilerExecutionError) as excinfo:
        # noinspection PyUnusedLocal
        parameter_builder.build_parameters(
            domain=domain,
            variables=variables,
            parameters=parameters,
            batch_request=batch_request,
        )

    assert (
        "Partitioning values for HistogramSingleBatchParameterBuilder by column_partition_metric_single_batch_parameter_builder did not yield bins of supported data type."
        in str(excinfo.value)
    )


@pytest.mark.parametrize(
    "column_values,bins,",
    [
        pytest.param(
            [
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ],
            [0.0],
        ),
        pytest.param(
            [
                1,
                1,
                1,
                1,
            ],
            [1.0],
        ),
    ],
)
@pytest.mark.integration
def test_histogram_single_batch_parameter_builder_alice_reduced_bins_count(
    column_values,
    bins,
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    parameter_builder: ParameterBuilder = HistogramSingleBatchParameterBuilder(
        name="my_name",
        bins="auto",
        evaluation_parameter_builder_configs=None,
        data_context=data_context,
    )

    metric_domain_kwargs: dict = {"column": "my_column"}
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

    variables: Optional[ParameterContainer] = None

    expected_parameter_value: dict
    parameter_node: ParameterNode

    with mock.patch(
        "great_expectations.execution_engine.pandas_batch_data.PandasBatchData.dataframe",
        new_callable=mock.PropertyMock,
        return_value=pd.DataFrame(
            {
                "my_column": column_values,
            }
        ),
    ):
        parameter_builder.build_parameters(
            domain=domain,
            variables=variables,
            parameters=parameters,
            batch_request=batch_request,
        )

        expected_parameter_value = {
            "value": {"bins": bins, "weights": [], "tail_weights": [0.5, 0.5]},
            "details": {
                "metric_configuration": {
                    "metric_name": "column.histogram",
                    "domain_kwargs": {"column": "my_column"},
                    "metric_value_kwargs": {
                        "bins": bins,
                    },
                },
                "num_batches": 1,
            },
        }

        parameter_node = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=parameter_builder.json_serialized_fully_qualified_parameter_name,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )

        assert parameter_node == expected_parameter_value


@pytest.mark.integration
def test_histogram_single_batch_parameter_builder_alice_check_serialized_keys(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    parameter_builder: ParameterBuilder = HistogramSingleBatchParameterBuilder(
        name="my_name",
        bins="auto",
        evaluation_parameter_builder_configs=None,
        data_context=data_context,
    )

    # Note: "evaluation_parameter_builder_configs" is not one of "ParameterBuilder" formal property attributes.
    assert set(parameter_builder.to_json_dict().keys()) == {
        "class_name",
        "module_name",
        "name",
        "evaluation_parameter_builder_configs",
    }
