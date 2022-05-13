from typing import Dict, Optional

from great_expectations import DataContext
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.helpers.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    ParameterBuilder,
    PartitionParameterBuilder,
)
from great_expectations.rule_based_profiler.types import (
    Domain,
    ParameterContainer,
    ParameterNode,
)


def test_instantiation_partition_parameter_builder(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    # noinspection PyUnusedLocal
    parameter_builder_0: ParameterBuilder = PartitionParameterBuilder(
        name="my_name_0",
        bucketize_data=True,
        data_context=data_context,
    )

    # noinspection PyUnusedLocal
    parameter_builder_1: ParameterBuilder = PartitionParameterBuilder(
        name="my_name_1",
        bucketize_data=False,
        data_context=data_context,
    )

    # noinspection PyUnusedLocal
    parameter_builder_2: ParameterBuilder = PartitionParameterBuilder(
        name="my_name_2",
        bucketize_data="$variables.bucketize_data",
        data_context=data_context,
    )


def test_partition_parameter_builder_alice_continuous(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    parameter_builder: ParameterBuilder = PartitionParameterBuilder(
        name="my_name",
        bucketize_data=True,
        data_context=data_context,
    )

    metric_domain_kwargs: dict = {"column": "user_id"}
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs=metric_domain_kwargs,
        rule_name="my_rule",
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
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
                "metric_dependencies": None,
            },
            "num_batches": 1,
        },
    }

    parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
        domain=domain,
        parameter_reference=parameter_builder.fully_qualified_parameter_name,
        expected_return_type=None,
        variables=variables,
        parameters=parameters,
    )

    assert parameter_node == expected_parameter_value


def test_partition_parameter_builder_alice_categorical(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    parameter_builder: ParameterBuilder = PartitionParameterBuilder(
        name="my_name",
        bucketize_data=False,
        data_context=data_context,
    )

    metric_domain_kwargs: dict = {"column": "event_type"}
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs=metric_domain_kwargs,
        rule_name="my_rule",
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
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
            "values": [19, 22, 73],
            "weights": [0.3333333333333333, 0.3333333333333333, 0.3333333333333333],
        },
        "details": {
            "metric_configuration": {
                "metric_name": "column.value_counts",
                "domain_kwargs": {"column": "event_type"},
                "metric_value_kwargs": {"sort": "value"},
                "metric_dependencies": None,
            },
            "num_batches": 1,
        },
    }

    parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
        domain=domain,
        parameter_reference=parameter_builder.fully_qualified_parameter_name,
        expected_return_type=None,
        variables=variables,
        parameters=parameters,
    )

    assert parameter_node == expected_parameter_value


def test_partition_parameter_builder_alice_continuous_changed_to_categorical(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    parameter_builder: ParameterBuilder = PartitionParameterBuilder(
        name="my_name",
        bucketize_data=True,
        data_context=data_context,
    )

    metric_domain_kwargs: dict = {"column": "event_ts"}
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs=metric_domain_kwargs,
        rule_name="my_rule",
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
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
            "values": [
                "2004-10-19 10:23:54",
                "2004-10-19 10:23:55",
                "2004-10-19 11:05:20",
            ],
            "weights": [0.3333333333333333, 0.3333333333333333, 0.3333333333333333],
        },
        "details": {
            "metric_configuration": {
                "metric_name": "column.value_counts",
                "domain_kwargs": {"column": "event_ts"},
                "metric_value_kwargs": {"sort": "value"},
                "metric_dependencies": None,
            },
            "num_batches": 1,
        },
    }

    parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
        domain=domain,
        parameter_reference=parameter_builder.fully_qualified_parameter_name,
        expected_return_type=None,
        variables=variables,
        parameters=parameters,
    )

    assert parameter_node == expected_parameter_value


def test_partition_parameter_builder_alice_continuous_check_serialized_keys(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    parameter_builder: ParameterBuilder = PartitionParameterBuilder(
        name="my_name",
        bucketize_data=True,
        data_context=data_context,
    )

    # Note: "evaluation_parameter_builder_configs" is not one of "ParameterBuilder" formal property attributes.
    assert set(parameter_builder.to_json_dict().keys()) == {
        "class_name",
        "module_name",
        "name",
        "bucketize_data",
        "metric_name",
        "metric_domain_kwargs",
        "metric_value_kwargs",
        "enforce_numeric_metric",
        "replace_nan_with_zero",
        "reduce_scalar_metric",
        "evaluation_parameter_builder_configs",
        "json_serialize",
    }
