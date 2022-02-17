from typing import Set

import pytest

import great_expectations.exceptions.exceptions as ge_exceptions
from great_expectations.data_context import DataContext
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.parameter_builder import (
    RegexPatternStringParameterBuilder,
    regex_pattern_string_parameter_builder,
)
from great_expectations.rule_based_profiler.types import (
    Domain,
    ParameterContainer,
    get_parameter_value_by_fully_qualified_parameter_name,
)


def test_regex_pattern_string_parameter_builder_instantiation():
    candidate_regexes: Set[str] = {
        r"/\d+/",  # whole number with 1 or more digits ExpectValuesToBeNumeric? (.. youw oudl want to emit that expectation)?
        r"/-?\d+/",  # negative whole numbers
        r"/-?\d+(\.\d*)?/",  # decimal numbers with . (period) separator
        r"/[A-Za-z0-9\.,;:!?()\"'%\-]+/",  # general text
        r"^ +/",  # leading space
        r" +/$",  # trailing space
        r"/https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#()?&//=]*)/",  # Matching URL (including http(s) protocol)
        r"/<\/?(?:p|a|b|img)(?: \/)?>/",  # HTML tags
        r"/(?:25[0-5]|2[0-4]\d|[01]\d{2}|\d{1,2})(?:.(?:25[0-5]|2[0-4]\d|[01]\d{2}|\d{1,2})){3}/",  # IPv4 IP address
        r"/(?:[A-Fa-f0-9]){0,4}(?: ?:? ?(?:[A-Fa-f0-9]){0,4}){0,7}/",  # IPv6 IP address,
    }
    regex_pattern_string_parameter: RegexPatternStringParameterBuilder = (
        RegexPatternStringParameterBuilder(
            name="my_simple_regex_string_parameter_builder",
            candidate_regexes=candidate_regexes,
        )
    )

    assert regex_pattern_string_parameter._threshold == 0.9
    assert regex_pattern_string_parameter._candidate_regexes == candidate_regexes
    assert regex_pattern_string_parameter.CANDIDATE_REGEX == candidate_regexes


def test_regex_pattern_string_parameter_builder_zero_batch_id_error():
    regex_pattern_string_parameter: RegexPatternStringParameterBuilder = (
        RegexPatternStringParameterBuilder(
            name="my_simple_regex_string_parameter_builder",
        )
    )
    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(domain_type=MetricDomainTypes.COLUMN)

    with pytest.raises(ge_exceptions.ProfilerExecutionError) as e:
        regex_pattern_string_parameter.build_parameters(
            parameter_container=parameter_container, domain=domain
        )

    assert (
        str(e.value)
        == "RegexPatternStringParameterBuilder was not able to get Validator using domain, variables and parameters provided."
    )


def test_regex_pattern_string_parameter_builder_alice(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context
    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    candidate_regexes: Set[str] = {
        r"^\d{1}$",
        r"^\d{2}$",
        r"^\S{8}-\S{4}-\S{4}-\S{4}-\S{12}$",
    }
    metric_domain_kwargs = {"column": "id"}

    regex_pattern_string_parameter: RegexPatternStringParameterBuilder = (
        RegexPatternStringParameterBuilder(
            name="my_regex",
            metric_domain_kwargs=metric_domain_kwargs,
            candidate_regexes=candidate_regexes,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )
    assert parameter_container.parameter_nodes is None

    regex_pattern_string_parameter._build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    fully_qualified_parameter_name_for_value: str = "$parameter.my_regex"
    expected_value: dict = {
        "value": r"^\S{8}-\S{4}-\S{4}-\S{4}-\S{12}$",
        "details": {"success_ratio": 1.0},
    }
    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
            domain=domain,
            parameters={domain.id: parameter_container},
        )
        == expected_value
    )


def test_regex_pattern_string_parameter_builder_bobby(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )
    metric_domain_kwargs: dict = {"column": "VendorID"}
    candidate_regexes: Set[str] = {
        r"^\d{1}$",
        r"^\d{3}$",  # won't match
        r"^\d{4}$",  # won't match
    }
    threshold: float = 0.9
    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
        "data_connector_query": {"index": -1},
    }

    regex_parameter: RegexPatternStringParameterBuilder = (
        RegexPatternStringParameterBuilder(
            name="my_regex_pattern_string_parameter_builder",
            metric_domain_kwargs=metric_domain_kwargs,
            candidate_regexes=candidate_regexes,
            threshold=threshold,
            data_context=data_context,
            batch_request=batch_request,
        )
    )

    assert regex_parameter.CANDIDATE_REGEX != candidate_regexes
    assert regex_parameter._candidate_regexes == candidate_regexes
    assert regex_parameter._threshold == 0.9

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    assert parameter_container.parameter_nodes is None

    regex_parameter._build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    fully_qualified_parameter_name_for_value: str = (
        "$parameter.my_regex_pattern_string_parameter_builder"
    )
    expected_value: dict = {
        "value": r"^\d{1}$",
        "details": {"success_ratio": 1.0},
    }

    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
            domain=domain,
            parameters={domain.id: parameter_container},
        )
        == expected_value
    )


def test_regex_pattern_string_parameter_builder_bobby_no_match(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )
    metric_domain_kwargs: dict = {"column": "VendorID"}
    candidate_regexes: Set[str] = {
        r"^\d{3}$",  # won't match
    }
    threshold: float = 0.9
    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
        "data_connector_query": {"index": -1},
    }

    regex_parameter: RegexPatternStringParameterBuilder = (
        RegexPatternStringParameterBuilder(
            name="my_regex_pattern_string_parameter_builder",
            metric_domain_kwargs=metric_domain_kwargs,
            candidate_regexes=candidate_regexes,
            threshold=threshold,
            data_context=data_context,
            batch_request=batch_request,
        )
    )
    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    domain: Domain = Domain(
        domain_type=MetricDomainTypes.COLUMN, domain_kwargs=metric_domain_kwargs
    )

    assert parameter_container.parameter_nodes is None

    regex_parameter._build_parameters(
        parameter_container=parameter_container, domain=domain
    )

    fully_qualified_parameter_name_for_value: str = (
        "$parameter.my_regex_pattern_string_parameter_builder"
    )
    expected_value: dict = {
        "details": {"success_ratio": 0},
    }

    assert (
        get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name_for_value,
            domain=domain,
            parameters={domain.id: parameter_container},
        )
        == expected_value
    )
