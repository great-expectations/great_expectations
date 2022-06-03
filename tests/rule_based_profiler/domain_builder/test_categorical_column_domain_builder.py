from typing import List

import pytest

from great_expectations import DataContext
from great_expectations.core.batch import BatchRequest
from great_expectations.exceptions import ProfilerConfigurationError
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.domain_builder.categorical_column_domain_builder import (
    CategoricalColumnDomainBuilder,
)
from great_expectations.rule_based_profiler.helpers.cardinality_checker import (
    CardinalityLimitMode,
)
from great_expectations.rule_based_profiler.types import (
    INFERRED_SEMANTIC_TYPE_KEY,
    Domain,
    SemanticDomainTypes,
)


def test_instantiate_with_cardinality_limit_modes_from_class_variable(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: BatchRequest = BatchRequest(
        datasource_name="alice_columnar_table_single_batch_datasource",
        data_connector_name="alice_columnar_table_single_batch_data_connector",
        data_asset_name="alice_columnar_table_single_batch_data_asset",
    )

    domain_builder: DomainBuilder = CategoricalColumnDomainBuilder(
        exclude_column_name_suffixes="_id",
        cardinality_limit_mode=CategoricalColumnDomainBuilder.cardinality_limit_modes.VERY_FEW,
        data_context=data_context,
    )

    domain_builder.get_domains(rule_name="my_rule", batch_request=batch_request)


def test_instantiate_with_cardinality_limit_modes_from_enum(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: BatchRequest = BatchRequest(
        datasource_name="alice_columnar_table_single_batch_datasource",
        data_connector_name="alice_columnar_table_single_batch_data_connector",
        data_asset_name="alice_columnar_table_single_batch_data_asset",
    )

    domain_builder: DomainBuilder = CategoricalColumnDomainBuilder(
        exclude_column_name_suffixes="_id",
        cardinality_limit_mode=CardinalityLimitMode.VERY_FEW,
        data_context=data_context,
    )

    domain_builder.get_domains(rule_name="my_rule", batch_request=batch_request)


def test_instantiate_with_cardinality_limit_modes_from_string(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: BatchRequest = BatchRequest(
        datasource_name="alice_columnar_table_single_batch_datasource",
        data_connector_name="alice_columnar_table_single_batch_data_connector",
        data_asset_name="alice_columnar_table_single_batch_data_asset",
    )

    domain_builder: DomainBuilder = CategoricalColumnDomainBuilder(
        exclude_column_name_suffixes="_id",
        cardinality_limit_mode="very_few",
        data_context=data_context,
    )

    domain_builder.get_domains(rule_name="my_rule", batch_request=batch_request)


def test_instantiate_with_cardinality_limit_modes_from_dictionary(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: BatchRequest = BatchRequest(
        datasource_name="alice_columnar_table_single_batch_datasource",
        data_connector_name="alice_columnar_table_single_batch_data_connector",
        data_asset_name="alice_columnar_table_single_batch_data_asset",
    )

    domain_builder: DomainBuilder = CategoricalColumnDomainBuilder(
        exclude_column_name_suffixes="_id",
        cardinality_limit_mode={
            "name": "very_few",
            "max_proportion_unique": 10,
            "metric_name_defining_limit": "column.distinct_values.count",
        },
        data_context=data_context,
    )

    domain_builder.get_domains(rule_name="my_rule", batch_request=batch_request)


def test_single_batch_very_few_cardinality(alice_columnar_table_single_batch_context):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: BatchRequest = BatchRequest(
        datasource_name="alice_columnar_table_single_batch_datasource",
        data_connector_name="alice_columnar_table_single_batch_data_connector",
        data_asset_name="alice_columnar_table_single_batch_data_asset",
    )

    domain_builder: DomainBuilder = CategoricalColumnDomainBuilder(
        exclude_column_name_suffixes="_id",
        cardinality_limit_mode="very_few",
        data_context=data_context,
    )
    domains: List[Domain] = domain_builder.get_domains(
        rule_name="my_rule", batch_request=batch_request
    )

    alice_all_column_names: List[str] = [
        "event_type",
        "event_ts",
        "server_ts",
        "device_ts",
        "user_agent",
    ]

    column_name: str
    alice_all_column_domains: List[Domain] = [
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": column_name,
            },
            rule_name="my_rule",
        )
        for column_name in alice_all_column_names
    ]
    assert len(domains) == 5

    # Unit Tests for "inferred_semantic_domain_type" are provided separately.
    domain: Domain
    for domain in domains:
        domain.details = {}

    assert domains == alice_all_column_domains


def test_single_batch_one_cardinality(alice_columnar_table_single_batch_context):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: BatchRequest = BatchRequest(
        datasource_name="alice_columnar_table_single_batch_datasource",
        data_connector_name="alice_columnar_table_single_batch_data_connector",
        data_asset_name="alice_columnar_table_single_batch_data_asset",
    )

    domain_builder: DomainBuilder = CategoricalColumnDomainBuilder(
        cardinality_limit_mode="ONE",
        data_context=data_context,
    )
    domains: List[Domain] = domain_builder.get_domains(
        rule_name="my_rule", batch_request=batch_request
    )

    alice_all_column_names: List[str] = [
        "user_agent",
    ]

    column_name: str
    alice_all_column_domains: List[Domain] = [
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": column_name,
            },
            rule_name="my_rule",
        )
        for column_name in alice_all_column_names
    ]
    assert len(domains) == 1

    # Unit Tests for "inferred_semantic_domain_type" are provided separately.
    domain: Domain
    for domain in domains:
        domain.details = {}

    assert domains == alice_all_column_domains


def test_unsupported_cardinality_limit_from_string(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    with pytest.raises(ProfilerConfigurationError) as excinfo:
        # noinspection PyUnusedLocal,PyArgumentList
        domains: List[Domain] = CategoricalColumnDomainBuilder(
            cardinality_limit_mode="&*#$&INVALID&*#$*&",
            data_context=data_context,
        ).get_domains(rule_name="my_rule", batch_request=batch_request)

    assert "specify a supported cardinality mode" in str(excinfo.value)
    assert "REL_1" in str(excinfo.value)
    assert "MANY" in str(excinfo.value)


def test_unsupported_cardinality_limit_from_dictionary(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: BatchRequest = BatchRequest(
        datasource_name="alice_columnar_table_single_batch_datasource",
        data_connector_name="alice_columnar_table_single_batch_data_connector",
        data_asset_name="alice_columnar_table_single_batch_data_asset",
    )

    with pytest.raises(ProfilerConfigurationError) as excinfo:
        # noinspection PyUnusedLocal,PyArgumentList
        domains: List[Domain] = CategoricalColumnDomainBuilder(
            cardinality_limit_mode={
                "name": "&*#$&INVALID&*#$*&",
                "max_proportion_unique": 10,
                "metric_name_defining_limit": "column.distinct_values.count",
            },
            data_context=data_context,
        ).get_domains(rule_name="my_rule", batch_request=batch_request)

    assert "specify a supported cardinality mode" in str(excinfo.value)
    assert "REL_1" in str(excinfo.value)
    assert "MANY" in str(excinfo.value)


def test_unspecified_cardinality_limit(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    with pytest.raises(ProfilerConfigurationError) as excinfo:
        # noinspection PyUnusedLocal,PyArgumentList
        domains: List[Domain] = CategoricalColumnDomainBuilder(
            data_context=data_context
        ).get_domains(rule_name="my_rule", batch_request=batch_request)

    assert "Please pass ONE of the following parameters" in str(excinfo.value)
    assert "you passed 0 parameters" in str(excinfo.value)


def test_excluded_columns_single_batch(alice_columnar_table_single_batch_context):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: BatchRequest = BatchRequest(
        datasource_name="alice_columnar_table_single_batch_datasource",
        data_connector_name="alice_columnar_table_single_batch_data_connector",
        data_asset_name="alice_columnar_table_single_batch_data_asset",
    )

    domain_builder: DomainBuilder = CategoricalColumnDomainBuilder(
        cardinality_limit_mode="VERY_FEW",
        exclude_column_names=[
            "id",
            "event_type",
            "user_id",
            "event_ts",
            "server_ts",
        ],
        data_context=data_context,
    )
    domains: List[Domain] = domain_builder.get_domains(
        rule_name="my_rule", batch_request=batch_request
    )

    alice_all_column_names: List[str] = [
        "device_ts",
        "user_agent",
    ]

    column_name: str
    alice_all_column_domains: List[Domain] = [
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": column_name,
            },
            rule_name="my_rule",
        )
        for column_name in alice_all_column_names
    ]
    assert len(domains) == 2

    # Unit Tests for "inferred_semantic_domain_type" are provided separately.
    domain: Domain
    for domain in domains:
        domain.details = {}

    assert domains == alice_all_column_domains


def test_excluded_columns_empty_single_batch(alice_columnar_table_single_batch_context):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: BatchRequest = BatchRequest(
        datasource_name="alice_columnar_table_single_batch_datasource",
        data_connector_name="alice_columnar_table_single_batch_data_connector",
        data_asset_name="alice_columnar_table_single_batch_data_asset",
    )

    domain_builder: DomainBuilder = CategoricalColumnDomainBuilder(
        cardinality_limit_mode="VERY_FEW",
        exclude_column_names=[],
        data_context=data_context,
    )
    domains: List[Domain] = domain_builder.get_domains(
        rule_name="my_rule", batch_request=batch_request
    )

    alice_all_column_names: List[str] = [
        "id",
        "event_type",
        "event_ts",
        "server_ts",
        "device_ts",
        "user_agent",
    ]

    column_name: str
    alice_all_column_domains: List[Domain] = [
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": column_name,
            },
            rule_name="my_rule",
        )
        for column_name in alice_all_column_names
    ]
    assert len(domains) == 6

    # Unit Tests for "inferred_semantic_domain_type" are provided separately.
    domain: Domain
    for domain in domains:
        domain.details = {}

    assert domains == alice_all_column_domains


def test_multi_batch_very_few_cardinality(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_pandas",
        data_connector_name="monthly",
        data_asset_name="my_reports",
    )

    domain_builder: DomainBuilder = CategoricalColumnDomainBuilder(
        cardinality_limit_mode="very_few",
        data_context=data_context,
    )
    observed_domains: List[Domain] = domain_builder.get_domains(
        rule_name="my_rule", batch_request=batch_request
    )

    expected_domains: List[Domain] = [
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": "VendorID",
            },
            details={
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "VendorID": SemanticDomainTypes.NUMERIC,
                },
            },
            rule_name="my_rule",
        ),
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": "passenger_count",
            },
            details={
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "passenger_count": SemanticDomainTypes.NUMERIC,
                },
            },
            rule_name="my_rule",
        ),
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": "RatecodeID",
            },
            details={
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "RatecodeID": SemanticDomainTypes.NUMERIC,
                },
            },
            rule_name="my_rule",
        ),
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": "store_and_fwd_flag",
            },
            details={
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "store_and_fwd_flag": SemanticDomainTypes.TEXT,
                },
            },
            rule_name="my_rule",
        ),
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": "payment_type",
            },
            details={
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "payment_type": SemanticDomainTypes.NUMERIC,
                }
            },
            rule_name="my_rule",
        ),
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": "mta_tax",
            },
            details={
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "mta_tax": SemanticDomainTypes.NUMERIC,
                },
            },
            rule_name="my_rule",
        ),
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": "improvement_surcharge",
            },
            details={
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "improvement_surcharge": SemanticDomainTypes.NUMERIC,
                },
            },
            rule_name="my_rule",
        ),
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": "congestion_surcharge",
            },
            details={
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "congestion_surcharge": SemanticDomainTypes.NUMERIC,
                },
            },
            rule_name="my_rule",
        ),
    ]

    assert len(observed_domains) == 8
    assert observed_domains == expected_domains


def test_multi_batch_one_cardinality(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_pandas",
        data_connector_name="monthly",
        data_asset_name="my_reports",
    )

    domain_builder: DomainBuilder = CategoricalColumnDomainBuilder(
        cardinality_limit_mode="ONE",
        data_context=data_context,
    )
    observed_domains: List[Domain] = domain_builder.get_domains(
        rule_name="my_rule", batch_request=batch_request
    )

    expected_domains: List[Domain] = []

    assert len(observed_domains) == 0
    assert observed_domains == expected_domains
