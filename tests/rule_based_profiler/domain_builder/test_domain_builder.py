from typing import List

from ruamel.yaml import YAML

from great_expectations import DataContext
from great_expectations.rule_based_profiler.domain_builder import (
    ColumnDomainBuilder,
    DomainBuilder,
    SimpleSemanticTypeColumnDomainBuilder,
    TableDomainBuilder,
)
from great_expectations.rule_based_profiler.types import (
    Domain,
    ParameterContainer,
    build_parameter_container_for_variables,
)

yaml = YAML()


# noinspection PyPep8Naming
def test_table_domain_builder(
    alice_columnar_table_single_batch_context,
    table_Users_domain,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    domain_builder: DomainBuilder = TableDomainBuilder(
        data_context=data_context,
        batch_request=None,
    )
    domains: List[Domain] = domain_builder.get_domains()

    assert len(domains) == 1
    assert domains == [
        {
            "domain_type": "table",
        }
    ]

    domain: Domain = domains[0]
    # Assert Domain object equivalence.
    assert domain == table_Users_domain
    # Also test that the dot notation is supported properly throughout the dictionary fields of the Domain object.
    assert domain.domain_kwargs.batch_id is None


# noinspection PyPep8Naming
def test_column_domain_builder(
    alice_columnar_table_single_batch_context,
    alice_columnar_table_single_batch,
    column_Age_domain,
    column_Date_domain,
    column_Description_domain,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    profiler_config: str = alice_columnar_table_single_batch["profiler_config"]

    full_profiler_config_dict: dict = yaml.safe_load(profiler_config)

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
        data_context=data_context,
        batch_request=batch_request,
    )
    domains: List[Domain] = domain_builder.get_domains(variables=variables)

    assert len(domains) == 7
    assert domains == [
        {
            "domain_type": "column",
            "domain_kwargs": {
                "column": "id",
            },
            "details": {},
        },
        {
            "domain_type": "column",
            "domain_kwargs": {
                "column": "event_type",
            },
            "details": {},
        },
        {
            "domain_type": "column",
            "domain_kwargs": {
                "column": "user_id",
            },
            "details": {},
        },
        {
            "domain_type": "column",
            "domain_kwargs": {
                "column": "event_ts",
            },
            "details": {},
        },
        {
            "domain_type": "column",
            "domain_kwargs": {
                "column": "server_ts",
            },
            "details": {},
        },
        {
            "domain_type": "column",
            "domain_kwargs": {
                "column": "device_ts",
            },
            "details": {},
        },
        {
            "domain_type": "column",
            "domain_kwargs": {
                "column": "user_agent",
            },
            "details": {},
        },
    ]


# noinspection PyPep8Naming
def test_simple_semantic_type_column_domain_builder(
    alice_columnar_table_single_batch_context,
    alice_columnar_table_single_batch,
    column_Age_domain,
    column_Description_domain,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    profiler_config: str = alice_columnar_table_single_batch["profiler_config"]

    full_profiler_config_dict: dict = yaml.safe_load(profiler_config)

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
    domain_builder: DomainBuilder = SimpleSemanticTypeColumnDomainBuilder(
        data_context=data_context,
        batch_request=batch_request,
        semantic_types=[
            "numeric",
        ],
    )
    domains: List[Domain] = domain_builder.get_domains(variables=variables)

    assert len(domains) == 2
    assert domains == [
        {
            "domain_type": "column",
            "domain_kwargs": {
                "column": "event_type",
            },
            "details": {"inferred_semantic_domain_type": "numeric"},
        },
        {
            "domain_type": "column",
            "domain_kwargs": {
                "column": "user_id",
            },
            "details": {"inferred_semantic_domain_type": "numeric"},
        },
    ]
