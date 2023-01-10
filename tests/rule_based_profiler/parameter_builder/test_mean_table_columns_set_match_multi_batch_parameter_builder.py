from typing import Dict, Optional

import pytest

from great_expectations import DataContext
from great_expectations.core.domain import Domain
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.rule_based_profiler.helpers.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    MeanTableColumnsSetMatchMultiBatchParameterBuilder,
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_container import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    ParameterContainer,
    ParameterNode,
)


@pytest.mark.integration
def test_instantiation_mean_table_columns_set_match_multi_batch_parameter_builder(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    # noinspection PyUnusedLocal
    parameter_builder: ParameterBuilder = (
        MeanTableColumnsSetMatchMultiBatchParameterBuilder(
            name="my_name",
            data_context=data_context,
        )
    )


@pytest.mark.integration
def test_execution_mean_table_columns_set_match_multi_batch_parameter_builder(
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

    mean_table_columns_set_match_multi_batch_parameter_builder: ParameterBuilder = (
        MeanTableColumnsSetMatchMultiBatchParameterBuilder(
            name="my_mean_table_columns_set_match_multi_batch_parameter_builder",
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=None,
            data_context=data_context,
        )
    )

    domain = Domain(
        domain_type=MetricDomainTypes.TABLE,
        domain_kwargs=None,
        rule_name="my_rule",
    )

    variables: Optional[ParameterContainer] = None

    parameter_container = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    expected_parameter_node_as_dict: dict = {
        "value": {
            "VendorID",
            "pickup_datetime",
            "total_amount",
            "congestion_surcharge",
            "dropoff_datetime",
            "mta_tax",
            "store_and_fwd_flag",
            "tip_amount",
            "trip_distance",
            "payment_type",
            "DOLocationID",
            "improvement_surcharge",
            "extra",
            "tolls_amount",
            "RatecodeID",
            "passenger_count",
            "PULocationID",
            "fare_amount",
        },
        "details": {
            "success_ratio": 1.0,
        },
    }

    mean_table_columns_set_match_multi_batch_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=batch_request,
    )

    parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
        domain=domain,
        parameter_reference=mean_table_columns_set_match_multi_batch_parameter_builder.json_serialized_fully_qualified_parameter_name,
        expected_return_type=None,
        variables=variables,
        parameters=parameters,
    )

    assert len(parameter_node[FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY]) == len(
        expected_parameter_node_as_dict[FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY]
    )

    parameter_node[FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY] = set(
        parameter_node[FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY]
    )
    assert parameter_node == expected_parameter_node_as_dict
