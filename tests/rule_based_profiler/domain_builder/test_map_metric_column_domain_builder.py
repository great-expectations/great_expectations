from typing import List

from great_expectations import DataContext
from great_expectations.core.batch import BatchRequest
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import (
    MapMetricColumnDomainBuilder,
)
from great_expectations.rule_based_profiler.types import Domain


def test_column_values_unique_single_batch(alice_columnar_table_single_batch_context):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: BatchRequest = BatchRequest(
        datasource_name="alice_columnar_table_single_batch_datasource",
        data_connector_name="alice_columnar_table_single_batch_data_connector",
        data_asset_name="alice_columnar_table_single_batch_data_asset",
    )

    domain_builder: MapMetricColumnDomainBuilder = MapMetricColumnDomainBuilder(
        map_metric_name="column_values.unique",
        batch_request=batch_request,
        data_context=data_context,
        max_unexpected_values=0,
        min_max_unexpected_values_proportion=9.75e-1,
    )
    domains: List[Domain] = domain_builder.get_domains()
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
        )
        for column_name in alice_compliant_column_names
    ]
    alice_expected_column_domains = sorted(
        alice_expected_column_domains, key=lambda x: x.domain_kwargs["column"]
    )

    assert len(domains) == 6
    assert domains == alice_expected_column_domains


def test_column_values_nonnull_multi_batch(
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

    domain_builder: MapMetricColumnDomainBuilder
    domains: List[Domain]
    bobby_compliant_column_names: List[str]
    bobby_expected_column_domains: List[Domain]
    column_name: str

    domain_builder = MapMetricColumnDomainBuilder(
        map_metric_name="column_values.nonnull",
        batch_request=batch_request,
        data_context=data_context,
        max_unexpected_values=0,
        min_max_unexpected_values_proportion=9.75e-1,
    )
    domains = domain_builder.get_domains()
    domains = sorted(domains, key=lambda x: x.domain_kwargs["column"])

    bobby_compliant_column_names = [
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

    bobby_expected_column_domains = [
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

    assert len(domains) == 17
    assert domains == bobby_expected_column_domains

    domain_builder = MapMetricColumnDomainBuilder(
        map_metric_name="column_values.nonnull",
        batch_request=batch_request,
        data_context=data_context,
        max_unexpected_values=4736,
        min_max_unexpected_values_proportion=1.0,
    )
    domains = domain_builder.get_domains()
    domains = sorted(domains, key=lambda x: x.domain_kwargs["column"])

    bobby_compliant_column_names = [
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

    bobby_expected_column_domains = [
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

    assert len(domains) == 18
    assert domains == bobby_expected_column_domains

    domain_builder = MapMetricColumnDomainBuilder(
        map_metric_name="column_values.nonnull",
        batch_request=batch_request,
        data_context=data_context,
        max_unexpected_values=0,
        min_max_unexpected_values_proportion=6.66e-1,
    )
    domains = domain_builder.get_domains()
    domains = sorted(domains, key=lambda x: x.domain_kwargs["column"])

    bobby_compliant_column_names = [
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

    bobby_expected_column_domains = [
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

    assert len(domains) == 18
    assert domains == bobby_expected_column_domains
