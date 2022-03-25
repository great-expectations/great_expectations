from typing import Any, Dict, List, Optional

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context import DataContext
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    DefaultExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    MetricMultiBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.types import (
    Domain,
    ParameterContainer,
    build_parameter_container_for_variables,
    get_parameter_value_by_fully_qualified_parameter_name,
)


def test_meta_not_dict_exception(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    metric_domain_kwargs: dict = {"column": "user_id"}

    min_user_id_parameter: MetricMultiBatchParameterBuilder = (
        MetricMultiBatchParameterBuilder(
            name="my_min_user_id",
            metric_name="column.min",
            metric_domain_kwargs=metric_domain_kwargs,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    min_user_id_parameter.build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    fully_qualified_parameter_name_for_value: str = "$parameter.my_min_user_id"
    parameter_value: Any = get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
        domain=domain,
        parameters={domain.id: parameter_container},
    )

    condition = None
    max_user_id: int = 999999999999

    with pytest.raises(ge_exceptions.ProfilerExecutionError) as e:
        # noinspection PyTypeChecker
        DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_values_to_be_between",
            condition=condition,
            min_value=parameter_value.value[0],
            max_value=max_user_id,
            meta="Strings are not acceptable",
        )

    assert (
        str(e.value)
        == 'Argument "Strings are not acceptable" in "DefaultExpectationConfigurationBuilder" must be of type "dictionary" (value of type "<class \'str\'>" was encountered).\n'
    )


def test_condition_not_string_exception(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    metric_domain_kwargs: dict = {"column": "user_id"}

    min_user_id_parameter: MetricMultiBatchParameterBuilder = (
        MetricMultiBatchParameterBuilder(
            name="my_min_user_id",
            metric_name="column.min",
            metric_domain_kwargs=metric_domain_kwargs,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    min_user_id_parameter.build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    fully_qualified_parameter_name_for_value: str = "$parameter.my_min_user_id"
    parameter_value: Any = get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
        domain=domain,
        parameters={domain.id: parameter_container},
    )

    condition: dict = {"condition": "$variables.tolerance<0.8"}
    max_user_id: int = 999999999999

    with pytest.raises(ge_exceptions.ProfilerExecutionError) as e:
        # noinspection PyTypeChecker
        DefaultExpectationConfigurationBuilder(
            expectation_type="expect_column_values_to_be_between",
            condition=condition,
            min_value=parameter_value.value[0],
            max_value=max_user_id,
        )

    assert (
        str(e.value)
        == 'Argument "{\'condition\': \'$variables.tolerance<0.8\'}" in "DefaultExpectationConfigurationBuilder" must be of type "string" (value of type "<class \'dict\'>" was encountered).\n'
    )


def test_default_expectation_configuration_builder_alice_null_condition_parameter_builder_validation_dependency_separate(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    metric_domain_kwargs: dict = {"column": "user_id"}

    min_user_id_parameter: MetricMultiBatchParameterBuilder = (
        MetricMultiBatchParameterBuilder(
            name="my_min_user_id",
            metric_name="column.min",
            metric_domain_kwargs=metric_domain_kwargs,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    min_user_id_parameter.build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    fully_qualified_parameter_name_for_value: str = "$parameter.my_min_user_id"
    parameter_value: Any = get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
        domain=domain,
        parameters=parameters,
    )

    condition: Optional[str] = None
    max_user_id: int = 999999999999

    default_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
        expectation_type="expect_column_values_to_be_between",
        condition=condition,
        min_value=parameter_value.value[0],
        max_value=max_user_id,
    )

    expectation_configuration: Optional[
        ExpectationConfiguration
    ] = default_expectation_configuration_builder.build_expectation_configuration(
        parameter_container=parameter_container,
        domain=domain,
        parameters=parameters,
    )

    assert expectation_configuration.kwargs["min_value"] == 397433


def test_default_expectation_configuration_builder_alice_null_condition_parameter_builder_validation_dependency_included(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    metric_domain_kwargs: dict = {"column": "user_id"}

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    fully_qualified_parameter_name_for_value: str = "$parameter.my_min_user_id.value[0]"

    condition: Optional[str] = None
    max_user_id: int = 999999999999

    min_user_id_parameter_builder_config: ParameterBuilderConfig = (
        ParameterBuilderConfig(
            module_name="great_expectations.rule_based_profiler.parameter_builder",
            class_name="MetricMultiBatchParameterBuilder",
            name="my_min_user_id",
            metric_name="column.min",
            metric_domain_kwargs=metric_domain_kwargs,
            batch_request=batch_request,
        )
    )
    validation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]] = [
        min_user_id_parameter_builder_config,
    ]
    default_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
        expectation_type="expect_column_values_to_be_between",
        condition=condition,
        min_value=fully_qualified_parameter_name_for_value,
        max_value=max_user_id,
        validation_parameter_builder_configs=validation_parameter_builder_configs,
        batch_request=batch_request,
        data_context=data_context,
    )

    expectation_configuration: Optional[
        ExpectationConfiguration
    ] = default_expectation_configuration_builder.build_expectation_configuration(
        parameter_container=parameter_container,
        domain=domain,
        parameters=parameters,
    )

    assert expectation_configuration.kwargs["min_value"] == 397433


def test_default_expectation_configuration_builder_alice_single_term_parameter_condition_true(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    metric_domain_kwargs: dict = {"column": "user_id"}

    min_user_id_parameter: MetricMultiBatchParameterBuilder = (
        MetricMultiBatchParameterBuilder(
            name="my_min_user_id",
            metric_name="column.min",
            metric_domain_kwargs=metric_domain_kwargs,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    min_user_id_parameter.build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    fully_qualified_parameter_name_for_value: str = "$parameter.my_min_user_id"
    parameter_value: Any = get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
        domain=domain,
        parameters=parameters,
    )

    condition: str = "$parameter.my_min_user_id.value[0]>0"
    max_user_id: int = 999999999999

    default_expectation_configuration_builder: DefaultExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
        expectation_type="expect_column_values_to_be_between",
        condition=condition,
        min_value=parameter_value.value[0],
        max_value=max_user_id,
    )

    expectation_configuration: Optional[
        ExpectationConfiguration
    ] = default_expectation_configuration_builder.build_expectation_configuration(
        parameter_container=parameter_container,
        domain=domain,
        parameters=parameters,
    )

    assert expectation_configuration.kwargs["min_value"] == 397433


def test_default_expectation_configuration_builder_alice_single_term_parameter_condition_false(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    metric_domain_kwargs: dict = {"column": "user_id"}

    min_user_id_parameter: MetricMultiBatchParameterBuilder = (
        MetricMultiBatchParameterBuilder(
            name="my_min_user_id",
            metric_name="column.min",
            metric_domain_kwargs=metric_domain_kwargs,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)

    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    min_user_id_parameter.build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    fully_qualified_parameter_name_for_value: str = "$parameter.my_min_user_id"
    parameter_value: Any = get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
        domain=domain,
        parameters=parameters,
    )

    condition: str = "$parameter.my_min_user_id.value[0]<0"
    max_user_id: int = 999999999999

    default_expectation_configuration_builder: DefaultExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
        expectation_type="expect_column_values_to_be_between",
        condition=condition,
        min_value=parameter_value.value[0],
        max_value=max_user_id,
    )

    expectation_configuration: Optional[
        ExpectationConfiguration
    ] = default_expectation_configuration_builder.build_expectation_configuration(
        parameter_container=parameter_container,
        domain=domain,
        parameters=parameters,
    )

    assert expectation_configuration is None


def test_default_expectation_configuration_builder_alice_single_term_variable_condition_true(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    metric_domain_kwargs: dict = {"column": "user_id"}

    min_user_id_parameter: MetricMultiBatchParameterBuilder = (
        MetricMultiBatchParameterBuilder(
            name="my_min_user_id",
            metric_name="column.min",
            metric_domain_kwargs=metric_domain_kwargs,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    min_user_id_parameter.build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    variables: ParameterContainer = build_parameter_container_for_variables(
        {"max_user_id": 999999999999}
    )
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    fully_qualified_parameter_name_for_value: str = "$parameter.my_min_user_id"
    parameter_value: Any = get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
        domain=domain,
        parameters=parameters,
    )

    condition: str = "$variables.max_user_id>0"
    max_value: str = "$variables.max_user_id"

    default_expectation_configuration_builder: DefaultExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
        expectation_type="expect_column_values_to_be_between",
        condition=condition,
        min_value=parameter_value.value[0],
        max_value=max_value,
    )

    expectation_configuration: Optional[
        ExpectationConfiguration
    ] = default_expectation_configuration_builder.build_expectation_configuration(
        parameter_container=parameter_container,
        domain=domain,
        variables=variables,
        parameters=parameters,
    )

    assert expectation_configuration.kwargs["min_value"] == 397433


def test_default_expectation_configuration_builder_alice_single_term_variable_condition_false(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    metric_domain_kwargs: dict = {"column": "user_id"}

    min_user_id_parameter: MetricMultiBatchParameterBuilder = (
        MetricMultiBatchParameterBuilder(
            name="my_min_user_id",
            metric_name="column.min",
            metric_domain_kwargs=metric_domain_kwargs,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    min_user_id_parameter.build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    variables: ParameterContainer = build_parameter_container_for_variables(
        {"max_user_id": 999999999999}
    )
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    fully_qualified_parameter_name_for_value: str = "$parameter.my_min_user_id"
    parameter_value: Any = get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
        domain=domain,
        parameters=parameters,
    )

    condition: str = "$variables.max_user_id<0"
    max_value: str = "$variables.max_user_id"

    default_expectation_configuration_builder: DefaultExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
        expectation_type="expect_column_values_to_be_between",
        condition=condition,
        min_value=parameter_value.value[0],
        max_value=max_value,
    )

    expectation_configuration: Optional[
        ExpectationConfiguration
    ] = default_expectation_configuration_builder.build_expectation_configuration(
        parameter_container=parameter_container,
        domain=domain,
        variables=variables,
        parameters=parameters,
    )

    assert expectation_configuration is None


def test_default_expectation_configuration_builder_alice_two_term_and_parameter_variable_condition_true(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    metric_domain_kwargs: dict = {"column": "user_id"}

    min_user_id_parameter: MetricMultiBatchParameterBuilder = (
        MetricMultiBatchParameterBuilder(
            name="my_min_user_id",
            metric_name="column.min",
            metric_domain_kwargs=metric_domain_kwargs,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    min_user_id_parameter.build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    variables: ParameterContainer = build_parameter_container_for_variables(
        {"max_user_id": 999999999999}
    )
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    fully_qualified_parameter_name_for_value: str = "$parameter.my_min_user_id.value[0]"
    parameter_value: Any = get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
        domain=domain,
        parameters=parameters,
    )

    condition: str = "$variables.max_user_id>0 & $parameter.my_min_user_id.value[0]>0"
    max_value: str = "$variables.max_user_id"

    default_expectation_configuration_builder: DefaultExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
        expectation_type="expect_column_values_to_be_between",
        condition=condition,
        min_value=parameter_value,
        max_value=max_value,
    )

    expectation_configuration: Optional[
        ExpectationConfiguration
    ] = default_expectation_configuration_builder.build_expectation_configuration(
        parameter_container=parameter_container,
        domain=domain,
        variables=variables,
        parameters=parameters,
    )

    assert expectation_configuration.kwargs["min_value"] == 397433


def test_default_expectation_configuration_builder_alice_two_term_and_parameter_variable_condition_false(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    metric_domain_kwargs: dict = {"column": "user_id"}

    min_user_id_parameter: MetricMultiBatchParameterBuilder = (
        MetricMultiBatchParameterBuilder(
            name="my_min_user_id",
            metric_name="column.min",
            metric_domain_kwargs=metric_domain_kwargs,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    min_user_id_parameter.build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    variables: ParameterContainer = build_parameter_container_for_variables(
        {"max_user_id": 999999999999}
    )
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    fully_qualified_parameter_name_for_value: str = "$parameter.my_min_user_id.value[0]"
    parameter_value: Any = get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
        domain=domain,
        parameters=parameters,
    )

    condition: str = "$variables.max_user_id<0 & $parameter.my_min_user_id.value[0]<0"
    max_value: str = "$variables.max_user_id"

    default_expectation_configuration_builder: DefaultExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
        expectation_type="expect_column_values_to_be_between",
        condition=condition,
        min_value=parameter_value,
        max_value=max_value,
    )

    expectation_configuration: Optional[
        ExpectationConfiguration
    ] = default_expectation_configuration_builder.build_expectation_configuration(
        parameter_container=parameter_container,
        domain=domain,
        variables=variables,
        parameters=parameters,
    )

    assert expectation_configuration is None


def test_default_expectation_configuration_builder_alice_two_term_or_parameter_variable_condition_true(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    metric_domain_kwargs: dict = {"column": "user_id"}

    min_user_id_parameter: MetricMultiBatchParameterBuilder = (
        MetricMultiBatchParameterBuilder(
            name="my_min_user_id",
            metric_name="column.min",
            metric_domain_kwargs=metric_domain_kwargs,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    min_user_id_parameter.build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    variables: ParameterContainer = build_parameter_container_for_variables(
        {"max_user_id": 999999999999}
    )
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    fully_qualified_parameter_name_for_value: str = "$parameter.my_min_user_id.value[0]"
    parameter_value: Any = get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
        domain=domain,
        parameters=parameters,
    )

    condition: str = "$variables.max_user_id>0 | $parameter.my_min_user_id.value[0]<0"
    max_value: str = "$variables.max_user_id"

    default_expectation_configuration_builder: DefaultExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
        expectation_type="expect_column_values_to_be_between",
        condition=condition,
        min_value=parameter_value,
        max_value=max_value,
    )

    expectation_configuration: Optional[
        ExpectationConfiguration
    ] = default_expectation_configuration_builder.build_expectation_configuration(
        parameter_container=parameter_container,
        domain=domain,
        variables=variables,
        parameters=parameters,
    )

    assert expectation_configuration.kwargs["min_value"] == 397433


def test_default_expectation_configuration_builder_alice_two_term_or_parameter_variable_condition_false(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    metric_domain_kwargs: dict = {"column": "user_id"}

    min_user_id_parameter: MetricMultiBatchParameterBuilder = (
        MetricMultiBatchParameterBuilder(
            name="my_min_user_id",
            metric_name="column.min",
            metric_domain_kwargs=metric_domain_kwargs,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    min_user_id_parameter.build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    variables: ParameterContainer = build_parameter_container_for_variables(
        {"max_user_id": 999999999999}
    )
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    fully_qualified_parameter_name_for_value: str = "$parameter.my_min_user_id.value[0]"
    parameter_value: Any = get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
        domain=domain,
        parameters=parameters,
    )

    condition: str = "$variables.max_user_id<0 | $parameter.my_min_user_id.value[0]<0"
    max_value: str = "$variables.max_user_id"

    default_expectation_configuration_builder: DefaultExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
        expectation_type="expect_column_values_to_be_between",
        condition=condition,
        min_value=parameter_value,
        max_value=max_value,
    )

    expectation_configuration: Optional[
        ExpectationConfiguration
    ] = default_expectation_configuration_builder.build_expectation_configuration(
        parameter_container=parameter_container,
        domain=domain,
        variables=variables,
        parameters=parameters,
    )

    assert expectation_configuration is None


def test_default_expectation_configuration_builder_alice_more_than_two_term_parameter_variable_condition_true(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    metric_domain_kwargs: dict = {"column": "user_id"}

    min_user_id_parameter: MetricMultiBatchParameterBuilder = (
        MetricMultiBatchParameterBuilder(
            name="my_min_user_id",
            metric_name="column.min",
            metric_domain_kwargs=metric_domain_kwargs,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    min_user_id_parameter.build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    variables: ParameterContainer = build_parameter_container_for_variables(
        {"max_user_id": 999999999999, "answer": 42}
    )
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    fully_qualified_parameter_name_for_value: str = "$parameter.my_min_user_id.value[0]"
    parameter_value: Any = get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
        domain=domain,
        parameters=parameters,
    )

    condition: str = "$variables.max_user_id>0 & $variables.answer==42 | $parameter.my_min_user_id.value[0]<0"
    max_value: str = "$variables.max_user_id"

    default_expectation_configuration_builder: DefaultExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
        expectation_type="expect_column_values_to_be_between",
        condition=condition,
        min_value=parameter_value,
        max_value=max_value,
    )

    expectation_configuration: Optional[
        ExpectationConfiguration
    ] = default_expectation_configuration_builder.build_expectation_configuration(
        parameter_container=parameter_container,
        domain=domain,
        variables=variables,
        parameters=parameters,
    )

    assert expectation_configuration.kwargs["min_value"] == 397433


def test_default_expectation_configuration_builder_alice_more_than_two_term_parameter_variable_condition_false(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    metric_domain_kwargs: dict = {"column": "user_id"}

    min_user_id_parameter: MetricMultiBatchParameterBuilder = (
        MetricMultiBatchParameterBuilder(
            name="my_min_user_id",
            metric_name="column.min",
            metric_domain_kwargs=metric_domain_kwargs,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    min_user_id_parameter.build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    variables: ParameterContainer = build_parameter_container_for_variables(
        {"max_user_id": 999999999999, "answer": 42}
    )
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    fully_qualified_parameter_name_for_value: str = "$parameter.my_min_user_id.value[0]"
    parameter_value: Any = get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
        domain=domain,
        parameters=parameters,
    )

    condition: str = "$variables.max_user_id<0 | $variables.answer!=42 | $parameter.my_min_user_id.value[0]<0"
    max_value: str = "$variables.max_user_id"

    default_expectation_configuration_builder: DefaultExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
        expectation_type="expect_column_values_to_be_between",
        condition=condition,
        min_value=parameter_value,
        max_value=max_value,
    )

    expectation_configuration: Optional[
        ExpectationConfiguration
    ] = default_expectation_configuration_builder.build_expectation_configuration(
        parameter_container=parameter_container,
        domain=domain,
        variables=variables,
        parameters=parameters,
    )

    assert expectation_configuration is None


def test_default_expectation_configuration_builder_alice_parentheses_parameter_variable_condition_true(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    metric_domain_kwargs: dict = {"column": "user_id"}

    min_user_id_parameter: MetricMultiBatchParameterBuilder = (
        MetricMultiBatchParameterBuilder(
            name="my_min_user_id",
            metric_name="column.min",
            metric_domain_kwargs=metric_domain_kwargs,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    min_user_id_parameter.build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    variables: ParameterContainer = build_parameter_container_for_variables(
        {"max_user_id": 999999999999, "answer": 42}
    )
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    fully_qualified_parameter_name_for_value: str = "$parameter.my_min_user_id.value[0]"
    parameter_value: Any = get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
        domain=domain,
        parameters=parameters,
    )

    condition: str = "($variables.max_user_id>0 & $variables.answer==42) | $parameter.my_min_user_id.value[0]<0"
    max_value: str = "$variables.max_user_id"

    default_expectation_configuration_builder: DefaultExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
        expectation_type="expect_column_values_to_be_between",
        condition=condition,
        min_value=parameter_value,
        max_value=max_value,
    )

    expectation_configuration: Optional[
        ExpectationConfiguration
    ] = default_expectation_configuration_builder.build_expectation_configuration(
        parameter_container=parameter_container,
        domain=domain,
        variables=variables,
        parameters=parameters,
    )

    assert expectation_configuration.kwargs["min_value"] == 397433


def test_default_expectation_configuration_builder_alice_parentheses_parameter_variable_condition_false(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    metric_domain_kwargs: dict = {"column": "user_id"}

    min_user_id_parameter: MetricMultiBatchParameterBuilder = (
        MetricMultiBatchParameterBuilder(
            name="my_min_user_id",
            metric_name="column.min",
            metric_domain_kwargs=metric_domain_kwargs,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    min_user_id_parameter.build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    variables: ParameterContainer = build_parameter_container_for_variables(
        {"max_user_id": 999999999999, "answer": 42}
    )
    parameters: Dict[str, ParameterContainer] = {
        domain.id: parameter_container,
    }

    fully_qualified_parameter_name_for_value: str = "$parameter.my_min_user_id.value[0]"
    parameter_value: Any = get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
        domain=domain,
        parameters=parameters,
    )

    condition: str = "($variables.max_user_id<0 | $variables.answer!=42) | $parameter.my_min_user_id.value[0]<0"
    max_value: str = "$variables.max_user_id"

    default_expectation_configuration_builder: DefaultExpectationConfigurationBuilder = DefaultExpectationConfigurationBuilder(
        expectation_type="expect_column_values_to_be_between",
        condition=condition,
        min_value=parameter_value,
        max_value=max_value,
    )

    expectation_configuration: Optional[
        ExpectationConfiguration
    ] = default_expectation_configuration_builder.build_expectation_configuration(
        parameter_container=parameter_container,
        domain=domain,
        variables=variables,
        parameters=parameters,
    )

    assert expectation_configuration is None
