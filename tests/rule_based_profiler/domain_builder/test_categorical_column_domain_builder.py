from typing import List

from great_expectations import DataContext
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.domain_builder.categorical_column_domain_builder import (
    CategoricalColumnDomainBuilder,
)
from great_expectations.rule_based_profiler.types import Domain


def test_single_batch_very_few_cardinality(alice_columnar_table_single_batch_context):
    data_context: DataContext = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    domain_builder: DomainBuilder = CategoricalColumnDomainBuilder(
        data_context=data_context,
        batch_request=batch_request,
        cardinality_limit="very_few",
    )
    domains: List[Domain] = domain_builder.get_domains()

    assert len(domains) == 7

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


def test_exhaustively_all_supported_cardinality_limits():
    raise NotImplementedError


def test_unsupported_cardinality_limit():
    raise NotImplementedError


def test_unspecified_cardinality_limit():
    raise NotImplementedError


def test_cardinality_limit_specified_as_str():
    raise NotImplementedError


def test_cardinality_limit_specified_as_object():
    raise NotImplementedError


def test_excluded_columns():
    raise NotImplementedError


def test_multi_batch_very_few_cardinality(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: DataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    # TODO AJB 20220222: Why is this failing when passing a BatchRequest obj
    #  to the DomainBuilder?
    # batch_request: BatchRequest = BatchRequest(
    #     datasource_name="taxi_pandas",
    #     data_connector_name="monthly",
    #     data_asset_name="my_reports",
    # )

    domain_builder: DomainBuilder = CategoricalColumnDomainBuilder(
        data_context=data_context,
        batch_request=batch_request,
        cardinality_limit="very_few",
    )
    observed_domains: List[Domain] = domain_builder.get_domains()

    assert len(observed_domains) == 10

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
                "column": "extra",
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
                "column": "tolls_amount",
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
