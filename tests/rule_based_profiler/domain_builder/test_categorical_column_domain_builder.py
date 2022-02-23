from typing import List
from unittest import mock

import pytest

from great_expectations import DataContext
from great_expectations.core.batch import BatchRequest
from great_expectations.exceptions import ProfilerConfigurationError
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.domain_builder.categorical_column_domain_builder import (
    CategoricalColumnDomainBuilder,
)
from great_expectations.rule_based_profiler.types import Domain


def test_single_batch_very_few_cardinality(alice_columnar_table_single_batch_context):

    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: BatchRequest = BatchRequest(
        datasource_name="alice_columnar_table_single_batch_datasource",
        data_connector_name="alice_columnar_table_single_batch_data_connector",
        data_asset_name="alice_columnar_table_single_batch_data_asset",
    )

    domain_builder: DomainBuilder = CategoricalColumnDomainBuilder(
        data_context=data_context,
        batch_request=batch_request,
        cardinality_limit_mode="very_few",
    )
    domains: List[Domain] = domain_builder.get_domains()

    alice_all_column_names = [
        "id",
        "event_type",
        "user_id",
        "event_ts",
        "server_ts",
        "device_ts",
        "user_agent",
    ]
    alice_all_column_domains = [
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": column_name,
            },
        )
        for column_name in alice_all_column_names
    ]
    assert domains == alice_all_column_domains

    assert len(domains) == 7


def test_single_batch_one_cardinality(alice_columnar_table_single_batch_context):

    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: BatchRequest = BatchRequest(
        datasource_name="alice_columnar_table_single_batch_datasource",
        data_connector_name="alice_columnar_table_single_batch_data_connector",
        data_asset_name="alice_columnar_table_single_batch_data_asset",
    )

    domain_builder: DomainBuilder = CategoricalColumnDomainBuilder(
        data_context=data_context,
        batch_request=batch_request,
        cardinality_limit_mode="ONE",
    )
    domains: List[Domain] = domain_builder.get_domains()

    alice_all_column_names = [
        "user_agent",
    ]
    alice_all_column_domains = [
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": column_name,
            },
        )
        for column_name in alice_all_column_names
    ]
    assert domains == alice_all_column_domains

    assert len(domains) == 1


@mock.patch("great_expectations.data_context.data_context.DataContext")
@mock.patch("great_expectations.core.batch.BatchRequest")
def test_unsupported_cardinality_limit(
    mock_data_context: mock.MagicMock,
    mock_batch_request: mock.MagicMock,
):
    with pytest.raises(ProfilerConfigurationError) as excinfo:
        _: DomainBuilder = CategoricalColumnDomainBuilder(
            data_context=mock_data_context,
            batch_request=mock_batch_request,
            cardinality_limit_mode="&*#$&INVALID&*#$*&",
        )
    assert "specify a supported cardinality mode" in str(excinfo.value)
    assert "REL_1" in str(excinfo.value)
    assert "MANY" in str(excinfo.value)


@mock.patch("great_expectations.data_context.data_context.DataContext")
@mock.patch("great_expectations.core.batch.BatchRequest")
def test_unspecified_cardinality_limit(
    mock_data_context: mock.MagicMock,
    mock_batch_request: mock.MagicMock,
):
    with pytest.raises(ProfilerConfigurationError) as excinfo:
        _: DomainBuilder = CategoricalColumnDomainBuilder(
            data_context=mock_data_context,
            batch_request=mock_batch_request,
        )
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
        data_context=data_context,
        batch_request=batch_request,
        cardinality_limit_mode="VERY_FEW",
        exclude_columns=[
            "id",
            "event_type",
            "user_id",
            "event_ts",
            "server_ts",
        ],
    )
    domains: List[Domain] = domain_builder.get_domains()

    alice_all_column_names = [
        "device_ts",
        "user_agent",
    ]
    alice_all_column_domains = [
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": column_name,
            },
        )
        for column_name in alice_all_column_names
    ]
    assert domains == alice_all_column_domains

    assert len(domains) == 2


def test_excluded_columns_empty_single_batch(alice_columnar_table_single_batch_context):

    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: BatchRequest = BatchRequest(
        datasource_name="alice_columnar_table_single_batch_datasource",
        data_connector_name="alice_columnar_table_single_batch_data_connector",
        data_asset_name="alice_columnar_table_single_batch_data_asset",
    )

    domain_builder: DomainBuilder = CategoricalColumnDomainBuilder(
        data_context=data_context,
        batch_request=batch_request,
        cardinality_limit_mode="VERY_FEW",
        exclude_columns=[],
    )
    domains: List[Domain] = domain_builder.get_domains()

    alice_all_column_names = [
        "id",
        "event_type",
        "user_id",
        "event_ts",
        "server_ts",
        "device_ts",
        "user_agent",
    ]
    alice_all_column_domains = [
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": column_name,
            },
        )
        for column_name in alice_all_column_names
    ]
    assert domains == alice_all_column_domains

    assert len(domains) == 7


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
        data_context=data_context,
        batch_request=batch_request,
        cardinality_limit_mode="very_few",
    )
    observed_domains: List[Domain] = domain_builder.get_domains()

    expected_domains: List[Domain] = [
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": "VendorID",
            },
        ),
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": "passenger_count",
            },
        ),
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": "RatecodeID",
            },
        ),
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": "store_and_fwd_flag",
            },
        ),
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": "payment_type",
            },
        ),
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": "mta_tax",
            },
        ),
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": "improvement_surcharge",
            },
        ),
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": "congestion_surcharge",
            },
        ),
    ]

    assert observed_domains == expected_domains

    assert len(observed_domains) == 8


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
        data_context=data_context,
        batch_request=batch_request,
        cardinality_limit_mode="ONE",
    )
    observed_domains: List[Domain] = domain_builder.get_domains()

    expected_domains: List[Domain] = []

    assert observed_domains == expected_domains

    assert len(observed_domains) == 0
