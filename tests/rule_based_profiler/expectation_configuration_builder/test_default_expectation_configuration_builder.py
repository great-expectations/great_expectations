import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context import DataContext
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    DefaultExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    MetricMultiBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.types import (
    Domain,
    ParameterContainer,
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

    metric_domain_kwargs = {"column": "user_id"}

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

    min_user_id_parameter._build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    fully_qualified_parameter_name_for_value: str = "$parameter.my_min_user_id"
    value = get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
        domain=domain,
        parameters={domain.id: parameter_container},
    )

    max_user_id = 999999999999

    with pytest.raises(ge_exceptions.ProfilerExecutionError) as e:
        default_expectation_configuration_builder = (
            DefaultExpectationConfigurationBuilder(
                expectation_type="expect_column_values_to_be_between",
                min_value=value.value[0],
                max_value=max_user_id,
                meta="Strings are not acceptable",
            )
        )

    assert (
        str(e.value)
        == 'Argument "Strings are not acceptable" in "DefaultExpectationConfigurationBuilder" must be of type "dictionary" (value of type "<class \'str\'>" was encountered).\n'
    )


def test_default_expectation_configuration_builder_alice(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    metric_domain_kwargs = {"column": "user_id"}

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

    min_user_id_parameter._build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    fully_qualified_parameter_name_for_value: str = "$parameter.my_min_user_id"
    value = get_parameter_value_by_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
        domain=domain,
        parameters={domain.id: parameter_container},
    )

    max_user_id = 999999999999

    default_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
        expectation_type="expect_column_values_to_be_between",
        min_value=value.value[0],
        max_value=max_user_id,
    )

    expectation_configuration: ExpectationConfiguration = (
        default_expectation_configuration_builder._build_expectation_configuration(
            domain=domain, parameters={domain.id: parameter_container}
        )
    )

    assert expectation_configuration.kwargs["min_value"] == 397433
