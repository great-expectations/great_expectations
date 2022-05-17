from typing import Dict, Optional

from great_expectations import DataContext
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.helpers.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    MeanTableColumnsSetMatchMultiBatchParameterBuilder,
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.types import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
    Domain,
    ParameterContainer,
    ParameterNode,
)


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
            json_serialize=True,
            data_context=data_context,
        )
    )

    domain: Domain = Domain(
        domain_type=MetricDomainTypes.TABLE,
        domain_kwargs=None,
        rule_name="my_rule",
    )

    variables: Optional[ParameterContainer] = None

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    mean_table_columns_set_match_multi_batch_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=batch_request,
    )

    expected_parameter_value: dict = {
        "value": 1.0,
        "details": {
            "metric_configuration": {
                "metric_name": "table.columns",
                "domain_kwargs": {},
                "metric_value_kwargs": None,
                "metric_dependencies": None,
            },
            "num_batches": 3,
        },
    }

    parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
        domain=domain,
        parameter_reference=mean_table_columns_set_match_multi_batch_parameter_builder.fully_qualified_parameter_name,
        expected_return_type=None,
        variables=variables,
        parameters=parameters,
    )

    assert parameter_node == expected_parameter_value
