from typing import List

import pytest

from great_expectations import DataContext
from great_expectations.core.batch import BatchRequest
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.rule_based_profiler.domain import Domain
from great_expectations.rule_based_profiler.domain_builder import (
    MapMetricColumnDomainBuilder,
)


@pytest.mark.integration
@pytest.mark.slow  # 1.20s
def test_column_values_unique_single_batch(alice_columnar_table_single_batch_context):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: BatchRequest = BatchRequest(
        datasource_name="alice_columnar_table_single_batch_datasource",
        data_connector_name="alice_columnar_table_single_batch_data_connector",
        data_asset_name="alice_columnar_table_single_batch_data_asset",
    )

    domain_builder: MapMetricColumnDomainBuilder = MapMetricColumnDomainBuilder(
        map_metric_name="column_values.unique",
        max_unexpected_values=0,
        max_unexpected_ratio=None,
        min_max_unexpected_values_proportion=9.75e-1,
        data_context=data_context,
    )
    domains: List[Domain] = domain_builder.get_domains(
        rule_name="my_rule", batch_request=batch_request
    )

    # Unit Tests for "inferred_semantic_domain_type" are provided separately.
    domain: Domain
    for domain in domains:
        domain.details = {}

    domains = sorted(domains, key=lambda x: x.domain_kwargs["column"])

    alice_compliant_column_names: List[str] = [
        "id",
        "event_type",
        "user_id",
        "event_ts",
        "server_ts",
        "device_ts",
    ]

    column_name: str
    alice_expected_column_domains: List[Domain] = [
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": column_name,
            },
            rule_name="my_rule",
        )
        for column_name in alice_compliant_column_names
    ]
    alice_expected_column_domains = sorted(
        alice_expected_column_domains, key=lambda x: x.domain_kwargs["column"]
    )

    assert len(domains) == 6
    assert domains == alice_expected_column_domains


@pytest.mark.integration
def test_column_values_nonnull_multi_batch_one_column_not_emitted(
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

    domain_builder: MapMetricColumnDomainBuilder = MapMetricColumnDomainBuilder(
        map_metric_name="column_values.nonnull",
        max_unexpected_values=0,
        max_unexpected_ratio=None,
        min_max_unexpected_values_proportion=9.75e-1,
        data_context=data_context,
    )
    domains: List[Domain] = domain_builder.get_domains(
        rule_name="my_rule", batch_request=batch_request
    )

    # Unit Tests for "inferred_semantic_domain_type" are provided separately.
    domain: Domain
    for domain in domains:
        domain.details = {}

    domains = sorted(domains, key=lambda x: x.domain_kwargs["column"])

    bobby_compliant_column_names: List[str] = [
        "VendorID",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
    ]

    column_name: str
    bobby_expected_column_domains: List[Domain] = [
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": column_name,
            },
            rule_name="my_rule",
        )
        for column_name in bobby_compliant_column_names
    ]
    bobby_expected_column_domains = sorted(
        bobby_expected_column_domains, key=lambda x: x.domain_kwargs["column"]
    )

    assert len(domains) == 17
    assert domains == bobby_expected_column_domains


@pytest.mark.integration
def test_column_values_nonnull_multi_batch_all_columns_emitted_loose_max_unexpected_values(
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

    domain_builder: MapMetricColumnDomainBuilder = MapMetricColumnDomainBuilder(
        map_metric_name="column_values.nonnull",
        max_unexpected_values=4736,
        max_unexpected_ratio=None,
        min_max_unexpected_values_proportion=1.0,
        data_context=data_context,
    )
    domains: List[Domain] = domain_builder.get_domains(
        rule_name="my_rule", batch_request=batch_request
    )

    # Unit Tests for "inferred_semantic_domain_type" are provided separately.
    domain: Domain
    for domain in domains:
        domain.details = {}

    domains = sorted(domains, key=lambda x: x.domain_kwargs["column"])

    bobby_compliant_column_names: List[str] = [
        "VendorID",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
    ]

    column_name: str
    bobby_expected_column_domains: List[Domain] = [
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": column_name,
            },
            rule_name="my_rule",
        )
        for column_name in bobby_compliant_column_names
    ]
    bobby_expected_column_domains = sorted(
        bobby_expected_column_domains, key=lambda x: x.domain_kwargs["column"]
    )

    assert len(domains) == 18
    assert domains == bobby_expected_column_domains


@pytest.mark.integration
@pytest.mark.slow  # 2.66s
def test_column_values_nonnull_multi_batch_all_columns_emitted_loose_min_max_unexpected_values_proportion(
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

    domain_builder: MapMetricColumnDomainBuilder = MapMetricColumnDomainBuilder(
        map_metric_name="column_values.nonnull",
        max_unexpected_values=0,
        max_unexpected_ratio=None,
        min_max_unexpected_values_proportion=6.66e-1,
        data_context=data_context,
    )
    domains: List[Domain] = domain_builder.get_domains(
        rule_name="my_rule", batch_request=batch_request
    )

    # Unit Tests for "inferred_semantic_domain_type" are provided separately.
    domain: Domain
    for domain in domains:
        domain.details = {}

    domains = sorted(domains, key=lambda x: x.domain_kwargs["column"])

    bobby_compliant_column_names: List[str] = [
        "VendorID",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
    ]

    column_name: str
    bobby_expected_column_domains: List[Domain] = [
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": column_name,
            },
            rule_name="my_rule",
        )
        for column_name in bobby_compliant_column_names
    ]
    bobby_expected_column_domains = sorted(
        bobby_expected_column_domains, key=lambda x: x.domain_kwargs["column"]
    )

    assert len(domains) == 18
    assert domains == bobby_expected_column_domains


@pytest.mark.integration
def test_column_values_nonnull_multi_batch_one_column_not_emitted_tight_max_unexpected_ratio(
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

    domain_builder: MapMetricColumnDomainBuilder = MapMetricColumnDomainBuilder(
        map_metric_name="column_values.nonnull",
        max_unexpected_values=0,
        max_unexpected_ratio=0.0,
        min_max_unexpected_values_proportion=1.0,
        data_context=data_context,
    )
    domains: List[Domain] = domain_builder.get_domains(
        rule_name="my_rule", batch_request=batch_request
    )

    # Unit Tests for "inferred_semantic_domain_type" are provided separately.
    domain: Domain
    for domain in domains:
        domain.details = {}

    domains = sorted(domains, key=lambda x: x.domain_kwargs["column"])

    bobby_compliant_column_names: List[str] = [
        "VendorID",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
    ]

    column_name: str
    bobby_expected_column_domains: List[Domain] = [
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": column_name,
            },
            rule_name="my_rule",
        )
        for column_name in bobby_compliant_column_names
    ]
    bobby_expected_column_domains = sorted(
        bobby_expected_column_domains, key=lambda x: x.domain_kwargs["column"]
    )

    assert len(domains) == 17
    assert domains == bobby_expected_column_domains


@pytest.mark.integration
def test_column_values_nonnull_multi_batch_all_columns_emitted_loose_max_unexpected_ratio(
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

    domain_builder: MapMetricColumnDomainBuilder = MapMetricColumnDomainBuilder(
        map_metric_name="column_values.nonnull",
        max_unexpected_values=0,
        max_unexpected_ratio=5.58 - 1,
        min_max_unexpected_values_proportion=1.0,
        data_context=data_context,
    )
    domains: List[Domain] = domain_builder.get_domains(
        rule_name="my_rule", batch_request=batch_request
    )

    # Unit Tests for "inferred_semantic_domain_type" are provided separately.
    domain: Domain
    for domain in domains:
        domain.details = {}

    domains = sorted(domains, key=lambda x: x.domain_kwargs["column"])

    bobby_compliant_column_names: List[str] = [
        "VendorID",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
    ]

    column_name: str
    bobby_expected_column_domains: List[Domain] = [
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": column_name,
            },
            rule_name="my_rule",
        )
        for column_name in bobby_compliant_column_names
    ]
    bobby_expected_column_domains = sorted(
        bobby_expected_column_domains, key=lambda x: x.domain_kwargs["column"]
    )

    assert len(domains) == 18
    assert domains == bobby_expected_column_domains
