import pytest

import great_expectations.exceptions.exceptions as ge_exceptions
from great_expectations.data_context import DataContext
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.parameter_builder import (
    SimpleDateFormatStringParameterBuilder,
)
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer


def test_simple_date_format_parameter_builder_instantiation():
    candidate_strings: set[str] = {
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%m-%d-%Y",
        "%Y-%m-%dT%z",
        "%y-%m-%d",
    }

    date_format_string_parameter: SimpleDateFormatStringParameterBuilder = (
        SimpleDateFormatStringParameterBuilder(
            name="my_simple_date_format_string_parameter_builder",
        )
    )

    assert date_format_string_parameter.CANDIDATE_STRINGS == candidate_strings
    assert date_format_string_parameter._candidate_strings == candidate_strings
    assert date_format_string_parameter._threshold == 1.0


def test_simple_date_format_parameter_builder_zero_batch_id_error():
    date_format_string_parameter: SimpleDateFormatStringParameterBuilder = (
        SimpleDateFormatStringParameterBuilder(
            name="my_simple_date_format_string_parameter_builder",
        )
    )
    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(domain_type=MetricDomainTypes.COLUMN)

    with pytest.raises(ge_exceptions.ProfilerExecutionError) as e:
        date_format_string_parameter._build_parameters(
            parameter_container=parameter_container, domain=domain
        )

    assert (
        str(e.value)
        == "Utilizing a SimpleDateFormatStringParameterBuilder requires a non-empty list of batch identifiers."
    )


def test_simple_date_format_parameter_builder_alice(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    candidate_strings: set[str] = {
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%m-%d-%Y",
        "%Y-%m-%dT%z",
        "%y-%m-%d",
    }
    metric_domain_kwargs = {"column": "event_ts"}

    date_format_string_parameter: SimpleDateFormatStringParameterBuilder = (
        SimpleDateFormatStringParameterBuilder(
            name="my_simple_date_format_string_parameter_builder",
            metric_domain_kwargs=metric_domain_kwargs,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    assert date_format_string_parameter.CANDIDATE_STRINGS == candidate_strings
    assert date_format_string_parameter._candidate_strings == candidate_strings
    assert date_format_string_parameter._threshold == 1.0

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    assert parameter_container.parameter_nodes is None

    date_format_string_parameter._build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    assert len(parameter_container.parameter_nodes) == 1
    assert parameter_container.parameter_nodes[
        "parameter"
    ].parameter.my_simple_date_format_string_parameter_builder == {
        "value": "%Y-%m-%d %H:%M:%S",
        "details": {"success_ratio": 1.0},
    }


def test_simple_date_format_parameter_builder_bobby(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    metric_domain_kwargs: dict = {"column": "pickup_datetime"}
    candidate_strings: set[str] = {
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
    }
    threshold: float = 0.9
    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    date_format_string_parameter: SimpleDateFormatStringParameterBuilder = (
        SimpleDateFormatStringParameterBuilder(
            name="my_simple_date_format_string_parameter_builder",
            metric_domain_kwargs=metric_domain_kwargs,
            candidate_strings=candidate_strings,
            threshold=threshold,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    assert date_format_string_parameter.CANDIDATE_STRINGS != candidate_strings
    assert date_format_string_parameter._candidate_strings == candidate_strings
    assert date_format_string_parameter._threshold == 0.9

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    assert parameter_container.parameter_nodes is None

    date_format_string_parameter._build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    assert len(parameter_container.parameter_nodes) == 1
    assert parameter_container.parameter_nodes[
        "parameter"
    ].parameter.my_simple_date_format_string_parameter_builder == {
        "value": "%Y-%m-%d %H:%M:%S",
        "details": {"success_ratio": 1.0},
    }
