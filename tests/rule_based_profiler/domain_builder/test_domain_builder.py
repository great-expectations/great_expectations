from typing import List

import pandas as pd
import pytest
from ruamel.yaml import YAML

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.rule_based_profiler.domain import (
    INFERRED_SEMANTIC_TYPE_KEY,
    Domain,
    SemanticDomainTypes,
)
from great_expectations.rule_based_profiler.domain_builder import (
    ColumnDomainBuilder,
    ColumnPairDomainBuilder,
    DomainBuilder,
    MultiColumnDomainBuilder,
    TableDomainBuilder,
)
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,
    build_parameter_container_for_variables,
)

yaml = YAML(typ="safe")


# noinspection PyPep8Naming
@pytest.mark.integration
@pytest.mark.slow  # 1.15s
def test_table_domain_builder(
    alice_columnar_table_single_batch_context,
    table_Users_domain,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    domain_builder: DomainBuilder = TableDomainBuilder(data_context=data_context)
    domains: List[Domain] = domain_builder.get_domains(rule_name="my_rule")

    assert len(domains) == 1
    assert domains == [
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.TABLE.value,
        }
    ]

    domain: Domain = domains[0]
    # Assert Domain object equivalence.
    assert domain == table_Users_domain

    # Also test that the dot notation is supported properly throughout the dictionary fields of the Domain object.
    assert domain.domain_type.value == "table"
    assert domain.kwargs is None


@pytest.mark.integration
def test_builder_executed_with_runtime_batch_request_does_not_raise_error(
    data_context_with_datasource_pandas_engine,
    alice_columnar_table_single_batch,
):
    data_context: DataContext = data_context_with_datasource_pandas_engine

    profiler_config: str = alice_columnar_table_single_batch["profiler_config"]

    full_profiler_config_dict: dict = yaml.load(profiler_config)

    variables_configs: dict = full_profiler_config_dict.get("variables")
    if variables_configs is None:
        variables_configs = {}

    variables: ParameterContainer = build_parameter_container_for_variables(
        variables_configs=variables_configs
    )

    df: pd.DataFrame = pd.DataFrame(
        {
            "a": [
                "2021-01-01",
                "2021-01-31",
                "2021-02-28",
                "2021-03-20",
                "2021-02-21",
                "2021-05-01",
                "2021-06-18",
            ]
        }
    )

    batch_request: dict = {
        "datasource_name": "my_datasource",
        "data_connector_name": "default_runtime_data_connector_name",
        "data_asset_name": "my_data_asset",
        "runtime_parameters": {
            "batch_data": df,
        },
        "batch_identifiers": {
            "default_identifier_name": "my_identifier",
        },
    }

    domain_builder: DomainBuilder = ColumnDomainBuilder(
        data_context=data_context,
    )
    domains: List[Domain] = domain_builder.get_domains(
        rule_name="my_rule",
        variables=variables,
        batch_request=batch_request,
    )

    assert len(domains) == 1
    assert domains == [
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "a",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "a": SemanticDomainTypes.TEXT.value,
                },
            },
        },
    ]


@pytest.mark.integration
@pytest.mark.slow  # 1.21s
def test_column_domain_builder(
    alice_columnar_table_single_batch_context,
    alice_columnar_table_single_batch,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    profiler_config: str = alice_columnar_table_single_batch["profiler_config"]

    full_profiler_config_dict: dict = yaml.load(profiler_config)

    variables_configs: dict = full_profiler_config_dict.get("variables")
    if variables_configs is None:
        variables_configs = {}

    variables: ParameterContainer = build_parameter_container_for_variables(
        variables_configs=variables_configs
    )

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    domain_builder: DomainBuilder = ColumnDomainBuilder(data_context=data_context)
    domains: List[Domain] = domain_builder.get_domains(
        rule_name="my_rule", variables=variables, batch_request=batch_request
    )

    assert len(domains) == 7
    assert domains == [
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "id",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "id": SemanticDomainTypes.TEXT.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "event_type",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "event_type": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "user_id",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "user_id": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "event_ts",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "event_ts": SemanticDomainTypes.TEXT.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "server_ts",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "server_ts": SemanticDomainTypes.TEXT.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "device_ts",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "device_ts": SemanticDomainTypes.TEXT.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "user_agent",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "user_agent": SemanticDomainTypes.TEXT.value,
                },
            },
        },
    ]


@pytest.mark.integration
@pytest.mark.slow  # 1.20s
def test_column_domain_builder_with_simple_semantic_type_included(
    alice_columnar_table_single_batch_context,
    alice_columnar_table_single_batch,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    profiler_config: str = alice_columnar_table_single_batch["profiler_config"]

    full_profiler_config_dict: dict = yaml.load(profiler_config)

    variables_configs: dict = full_profiler_config_dict.get("variables")
    if variables_configs is None:
        variables_configs = {}

    variables: ParameterContainer = build_parameter_container_for_variables(
        variables_configs=variables_configs
    )

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    domain_builder: DomainBuilder = ColumnDomainBuilder(
        include_semantic_types=[
            "numeric",
        ],
        data_context=data_context,
    )
    domains: List[Domain] = domain_builder.get_domains(
        rule_name="my_rule", variables=variables, batch_request=batch_request
    )

    assert len(domains) == 2
    # Assert Domain object equivalence.
    assert domains == [
        {
            "rule_name": "my_rule",
            "domain_type": "column",
            "domain_kwargs": {
                "column": "event_type",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "event_type": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": "column",
            "domain_kwargs": {
                "column": "user_id",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "user_id": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
    ]


@pytest.mark.integration
@pytest.mark.slow  # 1.19s
def test_column_pair_domain_builder_wrong_column_names(
    alice_columnar_table_single_batch_context,
    alice_columnar_table_single_batch,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    profiler_config: str = alice_columnar_table_single_batch["profiler_config"]

    full_profiler_config_dict: dict = yaml.load(profiler_config)

    variables_configs: dict = full_profiler_config_dict.get("variables")
    if variables_configs is None:
        variables_configs = {}

    variables: ParameterContainer = build_parameter_container_for_variables(
        variables_configs=variables_configs
    )

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    domain_builder: DomainBuilder = ColumnPairDomainBuilder(
        include_column_names=[
            "user_id",
            "event_type",
            "user_agent",
        ],
        data_context=data_context,
    )

    with pytest.raises(ge_exceptions.ProfilerExecutionError) as excinfo:
        # noinspection PyUnusedLocal
        domains: List[Domain] = domain_builder.get_domains(
            rule_name="my_rule", variables=variables, batch_request=batch_request
        )

    assert (
        'Error: Columns specified for ColumnPairDomainBuilder in sorted order must correspond to "column_A" and "column_B" (in this exact order).'
        in str(excinfo.value)
    )


@pytest.mark.integration
@pytest.mark.slow  # 1.19s
def test_column_pair_domain_builder_correct_sorted_column_names(
    alice_columnar_table_single_batch_context,
    alice_columnar_table_single_batch,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    profiler_config: str = alice_columnar_table_single_batch["profiler_config"]

    full_profiler_config_dict: dict = yaml.load(profiler_config)

    variables_configs: dict = full_profiler_config_dict.get("variables")
    if variables_configs is None:
        variables_configs = {}

    variables: ParameterContainer = build_parameter_container_for_variables(
        variables_configs=variables_configs
    )

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    domain_builder: DomainBuilder = ColumnPairDomainBuilder(
        include_column_names=[
            "user_id",
            "event_type",
        ],
        data_context=data_context,
    )
    domains: List[Domain] = domain_builder.get_domains(
        rule_name="my_rule", variables=variables, batch_request=batch_request
    )

    assert len(domains) == 1
    # Assert Domain object equivalence.
    assert domains == [
        {
            "rule_name": "my_rule",
            "domain_type": "column_pair",
            "domain_kwargs": {
                "column_A": "event_type",
                "column_B": "user_id",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "event_type": SemanticDomainTypes.NUMERIC.value,
                    "user_id": SemanticDomainTypes.NUMERIC.value,
                },
            },
        }
    ]

    domain: Domain = domains[0]

    # Also test that the dot notation is supported properly throughout the dictionary fields of the Domain object.
    assert domain.domain_type.value == "column_pair"
    assert domain.domain_kwargs.column_A == "event_type"
    assert domain.domain_kwargs.column_B == "user_id"


@pytest.mark.integration
@pytest.mark.slow  # 1.30s
def test_multi_column_domain_builder_wrong_column_list(
    alice_columnar_table_single_batch_context,
    alice_columnar_table_single_batch,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    profiler_config: str = alice_columnar_table_single_batch["profiler_config"]

    full_profiler_config_dict: dict = yaml.load(profiler_config)

    variables_configs: dict = full_profiler_config_dict.get("variables")
    if variables_configs is None:
        variables_configs = {}

    variables: ParameterContainer = build_parameter_container_for_variables(
        variables_configs=variables_configs
    )

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    domain_builder: DomainBuilder = MultiColumnDomainBuilder(
        include_column_names=None,
        data_context=data_context,
    )

    with pytest.raises(ge_exceptions.ProfilerExecutionError) as excinfo:
        # noinspection PyUnusedLocal
        domains: List[Domain] = domain_builder.get_domains(
            rule_name="my_rule", variables=variables, batch_request=batch_request
        )

    assert 'Error: "column_list" in MultiColumnDomainBuilder must not be empty.' in str(
        excinfo.value
    )

    with pytest.raises(ge_exceptions.ProfilerExecutionError) as excinfo:
        # noinspection PyUnusedLocal
        domains: List[Domain] = domain_builder.get_domains(
            rule_name="my_rule", variables=variables, batch_request=batch_request
        )

    assert 'Error: "column_list" in MultiColumnDomainBuilder must not be empty.' in str(
        excinfo.value
    )


@pytest.mark.integration
@pytest.mark.slow  # 1.18s
def test_multi_column_domain_builder_correct_column_list(
    alice_columnar_table_single_batch_context,
    alice_columnar_table_single_batch,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    profiler_config: str = alice_columnar_table_single_batch["profiler_config"]

    full_profiler_config_dict: dict = yaml.load(profiler_config)

    variables_configs: dict = full_profiler_config_dict.get("variables")
    if variables_configs is None:
        variables_configs = {}

    variables: ParameterContainer = build_parameter_container_for_variables(
        variables_configs=variables_configs
    )

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    domain_builder: DomainBuilder = MultiColumnDomainBuilder(
        include_column_names=[
            "event_type",
            "user_id",
            "user_agent",
        ],
        data_context=data_context,
    )
    domains: List[Domain] = domain_builder.get_domains(
        rule_name="my_rule", variables=variables, batch_request=batch_request
    )

    assert len(domains) == 1
    # Assert Domain object equivalence.
    assert domains == [
        {
            "rule_name": "my_rule",
            "domain_type": "multicolumn",
            "domain_kwargs": {
                "column_list": [
                    "event_type",
                    "user_id",
                    "user_agent",
                ],
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "event_type": SemanticDomainTypes.NUMERIC.value,
                    "user_id": SemanticDomainTypes.NUMERIC.value,
                    "user_agent": SemanticDomainTypes.TEXT.value,
                },
            },
        }
    ]

    domain: Domain = domains[0]

    # Also test that the dot notation is supported properly throughout the dictionary fields of the Domain object.
    assert domain.domain_type.value == "multicolumn"
    assert domain.domain_kwargs.column_list == [
        "event_type",
        "user_id",
        "user_agent",
    ]
