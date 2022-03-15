from typing import List

import numpy as np

from great_expectations import DataContext
from great_expectations.core.batch import BatchRequest
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import (
    TableColumnTypesColumnDomainBuilder,
)
from great_expectations.rule_based_profiler.types import Domain


def test_table_column_types_int_pandas_single_batch(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: BatchRequest = BatchRequest(
        datasource_name="alice_columnar_table_single_batch_datasource",
        data_connector_name="alice_columnar_table_single_batch_data_connector",
        data_asset_name="alice_columnar_table_single_batch_data_asset",
    )

    domain_builder: TableColumnTypesColumnDomainBuilder = (
        TableColumnTypesColumnDomainBuilder(
            column_type=np.int64,
            batch_request=batch_request,
            data_context=data_context,
        )
    )
    domains: List[Domain] = domain_builder.get_domains()
    domains = sorted(domains, key=lambda x: x.domain_kwargs["column"])

    alice_compliant_column_names: List[str] = [
        "event_type",
        "user_id",
    ]

    column_name: str
    alice_expected_column_domains: List[Domain] = [
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": column_name,
            },
        )
        for column_name in alice_compliant_column_names
    ]
    alice_expected_column_domains = sorted(
        alice_expected_column_domains, key=lambda x: x.domain_kwargs["column"]
    )

    assert len(domains) == 2
    assert domains == alice_expected_column_domains


def test_table_column_types_object_pandas_single_batch(
    alice_columnar_table_single_batch_context,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: BatchRequest = BatchRequest(
        datasource_name="alice_columnar_table_single_batch_datasource",
        data_connector_name="alice_columnar_table_single_batch_data_connector",
        data_asset_name="alice_columnar_table_single_batch_data_asset",
    )

    domain_builder: TableColumnTypesColumnDomainBuilder = (
        TableColumnTypesColumnDomainBuilder(
            column_type=np.object,
            batch_request=batch_request,
            data_context=data_context,
        )
    )
    domains: List[Domain] = domain_builder.get_domains()
    domains = sorted(domains, key=lambda x: x.domain_kwargs["column"])

    alice_compliant_column_names: List[str] = [
        "device_ts",
        "event_ts",
        "id",
        "server_ts",
        "user_agent",
    ]

    column_name: str
    alice_expected_column_domains: List[Domain] = [
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": column_name,
            },
        )
        for column_name in alice_compliant_column_names
    ]
    alice_expected_column_domains = sorted(
        alice_expected_column_domains, key=lambda x: x.domain_kwargs["column"]
    )

    assert len(domains) == 5
    assert domains == alice_expected_column_domains


def test_table_column_types_int_pandas_multi_batch(
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

    domain_builder: TableColumnTypesColumnDomainBuilder = (
        TableColumnTypesColumnDomainBuilder(
            column_type=np.int64,
            batch_request=batch_request,
            data_context=data_context,
        )
    )
    domains: List[Domain] = domain_builder.get_domains()
    domains = sorted(domains, key=lambda x: x.domain_kwargs["column"])

    bobby_compliant_column_names: List[str] = [
        "DOLocationID",
        "PULocationID",
        "RatecodeID",
        "VendorID",
        "passenger_count",
        "payment_type",
    ]

    column_name: str
    bobby_expected_column_domains: List[Domain] = [
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": column_name,
            },
        )
        for column_name in bobby_compliant_column_names
    ]
    bobby_expected_column_domains = sorted(
        bobby_expected_column_domains, key=lambda x: x.domain_kwargs["column"]
    )

    assert len(domains) == 6
    assert domains == bobby_expected_column_domains


def test_table_column_types_object_pandas_multi_batch(
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

    domain_builder: TableColumnTypesColumnDomainBuilder = (
        TableColumnTypesColumnDomainBuilder(
            column_type=np.object,
            batch_request=batch_request,
            data_context=data_context,
        )
    )
    domains: List[Domain] = domain_builder.get_domains()
    domains = sorted(domains, key=lambda x: x.domain_kwargs["column"])

    bobby_compliant_column_names: List[str] = [
        "dropoff_datetime",
        "pickup_datetime",
        "store_and_fwd_flag",
    ]

    column_name: str
    bobby_expected_column_domains: List[Domain] = [
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": column_name,
            },
        )
        for column_name in bobby_compliant_column_names
    ]
    bobby_expected_column_domains = sorted(
        bobby_expected_column_domains, key=lambda x: x.domain_kwargs["column"]
    )

    assert len(domains) == 3
    assert domains == bobby_expected_column_domains
