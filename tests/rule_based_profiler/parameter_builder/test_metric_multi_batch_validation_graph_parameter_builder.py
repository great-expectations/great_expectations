from typing import Dict, List, Optional

import pytest

from great_expectations.core.batch import Batch
from great_expectations.core.domain import Domain
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.data_context import DataContext
from great_expectations.rule_based_profiler.parameter_builder import (
    MetricMultiBatchValidationGraphParameterBuilder,
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_container import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
    ParameterContainer,
    ParameterNode,
    get_parameter_value_by_fully_qualified_parameter_name,
)
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.validation_graph import MetricEdge, ValidationGraph
from great_expectations.validator.validator import Validator


@pytest.mark.integration
def test_metric_multi_batch_validation_graph_parameter_builder_bobby(
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

    validator: Validator = data_context.get_validator(**batch_request)
    batch_list: List[Batch] = list(validator.batch_cache.values())

    batch: Batch
    batch_ids: List[str] = [batch.id for batch in batch_list]

    # Omitting "single_batch_mode" argument in order to exercise default (False) behavior.
    metric_multi_batch_validation_graph_parameter_builder: ParameterBuilder = (
        MetricMultiBatchValidationGraphParameterBuilder(
            name="row_count_graph",
            metric_name="table.row_count",
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=None,
            data_context=data_context,
        )
    )

    domain = Domain(
        domain_type=MetricDomainTypes.TABLE,
        rule_name="my_rule",
    )
    parameter_container = ParameterContainer(parameter_nodes=None)
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    assert parameter_container.parameter_nodes is None

    variables: Optional[ParameterContainer] = None

    metric_multi_batch_validation_graph_parameter_builder.build_parameters(
        domain=domain,
        variables=variables,
        parameters=parameters,
        batch_request=batch_request,
    )

    parameter_nodes: Optional[Dict[str, ParameterNode]] = (
        parameter_container.parameter_nodes or {}
    )
    assert len(parameter_nodes) == 1

    batch_id: str
    edges: List[MetricEdge] = [
        MetricEdge(
            left=MetricConfiguration(
                metric_name="table.row_count",
                metric_domain_kwargs={"batch_id": batch_id},
                metric_value_kwargs=None,
            ),
            right=None,
        )
        for batch_id in batch_ids
    ]
    graph: ValidationGraph = ValidationGraph(
        execution_engine=validator.execution_engine, edges=edges
    )

    expected_parameter_node_as_dict: dict = {
        "details": {
            "metric_configuration": {
                "domain_kwargs": {},
                "metric_name": "table.row_count",
                "metric_value_kwargs": None,
            },
            "num_batches": 3,
            "graph": graph,
        },
    }

    parameter_node: ParameterNode = get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=metric_multi_batch_validation_graph_parameter_builder.raw_fully_qualified_parameter_name,
        domain=domain,
        parameters=parameters,
    )

    assert parameter_node == expected_parameter_node_as_dict
