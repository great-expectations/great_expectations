from typing import Any, Dict, List, Optional

import numpy as np

from great_expectations import DataContext
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.helpers.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    ParameterBuilder,
    PartitionParameterBuilder,
)
from great_expectations.rule_based_profiler.types import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
    Domain,
    ParameterContainer,
    ParameterNode,
)
from tests.rule_based_profiler.conftest import ATOL, RTOL


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

    # TODO: <Alex>ALEX</Alex>
    from great_expectations.core.batch import BatchRequest

    print(
        f"\n[ALEX_TEST] [WOUTPUT] ALICE_HEAD:\n{data_context.get_validator(batch_request=BatchRequest(**batch_request)).head()}"
    )
    # TODO: <Alex>ALEX</Alex>
    parameter_builder: ParameterBuilder = PartitionParameterBuilder(
        name="my_name",
        bucketize_data=True,
        data_context=data_context,
    )

    metric_domain_kwargs: dict = {"column": "event_type"}
    domain: Domain = Domain(
        rule_name="my_rule",
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs=metric_domain_kwargs,
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

    # expected_parameter_value: float = 0.0
    #
    parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
        domain=domain,
        parameter_reference=parameter_builder.fully_qualified_parameter_name,
        expected_return_type=None,
        variables=variables,
        parameters=parameters,
    )
    print(
        f"\n[ALEX_TEST] [WOUTPUT] WOUTPUT-CONTINUOUS-PRAMETER_NODE:\n{parameter_node} ; TYPE: {str(type(parameter_node))}"
    )

    # rtol: float = RTOL
    # atol: float = 5.0e-1 * ATOL
    # np.testing.assert_allclose(
    #     actual=actual_parameter_value.value,
    #     desired=expected_parameter_value,
    #     rtol=rtol,
    #     atol=atol,
    #     err_msg=f"Actual value of {actual_parameter_value.value} differs from expected value of {expected_parameter_value} by more than {atol + rtol * abs(actual_parameter_value.value)} tolerance.",
    # )


def test_partition_parameter_builder_alice_categorical(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    # TODO: <Alex>ALEX</Alex>
    from great_expectations.core.batch import BatchRequest

    print(
        f"\n[ALEX_TEST] [WOUTPUT] ALICE_HEAD:\n{data_context.get_validator(batch_request=BatchRequest(**batch_request)).head()}"
    )
    # TODO: <Alex>ALEX</Alex>
    parameter_builder: ParameterBuilder = PartitionParameterBuilder(
        name="my_name",
        bucketize_data=False,
        data_context=data_context,
    )

    metric_domain_kwargs: dict = {"column": "event_type"}
    domain: Domain = Domain(
        rule_name="my_rule",
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs=metric_domain_kwargs,
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

    # expected_parameter_value: float = 0.0
    #
    parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
        domain=domain,
        parameter_reference=parameter_builder.fully_qualified_parameter_name,
        expected_return_type=None,
        variables=variables,
        parameters=parameters,
    )
    print(
        f"\n[ALEX_TEST] [WOUTPUT] WOUTPUT-CATEGORICAL-PRAMETER_NODE:\n{parameter_node} ; TYPE: {str(type(parameter_node))}"
    )

    # rtol: float = RTOL
    # atol: float = 5.0e-1 * ATOL
    # np.testing.assert_allclose(
    #     actual=actual_parameter_value.value,
    #     desired=expected_parameter_value,
    #     rtol=rtol,
    #     atol=atol,
    #     err_msg=f"Actual value of {actual_parameter_value.value} differs from expected value of {expected_parameter_value} by more than {atol + rtol * abs(actual_parameter_value.value)} tolerance.",
    # )
