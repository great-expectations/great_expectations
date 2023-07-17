"""
This file contains demo code for the column_descriptive_metrics module.
Unit, integration and end-to-end tests should be written to replace this code.
"""
import uuid
from unittest import mock

import pytest
from great_expectations.experimental.column_descriptive_metrics.asset_inspector import (
    AssetInspector,
    MetricConverter,
)

import pandas as pd
import great_expectations as gx
from great_expectations.experimental.column_descriptive_metrics.metrics import (
    Metric,
    BatchPointer,
    Value,
    Metrics,
)


@pytest.fixture
def cloud_org_id() -> uuid.UUID:
    return uuid.UUID("6e9c4af6-7616-43c3-9b40-a1a9e100c4a0")


@pytest.fixture
def metric_id() -> uuid.UUID:
    return uuid.UUID("0fa72ac7-df72-4bf2-9fe5-e6b01eed6e95")


@pytest.fixture
def run_id() -> uuid.UUID:
    return uuid.UUID("2a24847b-dae4-43c7-8027-1bd1cd572690")


def test_demo_asset_inspector(
    cloud_org_id: uuid.UUID, metric_id: uuid.UUID, run_id: uuid.UUID
):
    """This is a demo of how to get column descriptive metrics,
    this should be replaced with proper tests."""

    # Working in notebook:

    context = gx.get_context()

    validator = context.sources.pandas_default.read_csv(
        "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
    )

    context.list_datasources()

    datasource = context.get_datasource("default_pandas_datasource")

    data_asset = datasource.assets[0]

    batch_request = data_asset.build_batch_request()

    # Not working with simple dataframe, probably user error on my part:
    # The code sample for this doc works: https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/fluent/in_memory/how_to_connect_to_in_memory_data_using_pandas
    # context = gx.get_context()
    # datasource = context.sources.add_pandas(name="my_pandas_datasource")
    #
    # d = {"col1": [1, 2], "col2": [3, 4]}
    # df = pd.DataFrame(data=d)
    #
    # name = "dataframe"
    # data_asset = datasource.add_dataframe_asset(name=name)
    # batch_request = data_asset.build_batch_request(dataframe=df)
    # # my_batch = data_asset.get_batch_list_from_batch_request(
    # #     batch_request=batch_request
    # # )[0]

    # From here down assume we just have the batch request from the agent action:
    datasource_from_action = context.get_datasource(batch_request.datasource_name)
    data_asset_from_action = datasource_from_action.get_asset(
        batch_request.data_asset_name
    )
    batch_from_action = data_asset_from_action.get_batch_list_from_batch_request(
        batch_request
    )[0]

    with mock.patch(
        f"{AssetInspector.__module__}.{AssetInspector.__name__}._generate_run_id",
        return_value=run_id,
    ), mock.patch(
        f"{MetricConverter.__module__}.{MetricConverter.__name__}._generate_metric_id",
        return_value=metric_id,
    ):
        asset_inspector = AssetInspector(organization_id=cloud_org_id)
        metrics = asset_inspector.get_column_descriptive_metrics(
            batch=batch_from_action
        )

    assert metrics == Metrics(
        metrics=[
            Metric(
                id=metric_id,
                organization_id=cloud_org_id,
                run_id=run_id,
                batch_pointer=BatchPointer(
                    datasource_name="default_pandas_datasource",
                    data_asset_name="#ephemeral_pandas_asset",
                    batch_id="default_pandas_datasource-#ephemeral_pandas_asset",
                ),
                metric_name="table.row_count",
                metric_domain_kwargs={},
                metric_value_kwargs={},
                column=None,
                value=Value(value=10000),
                details={},
            ),
            Metric(
                id=metric_id,
                organization_id=cloud_org_id,
                run_id=run_id,
                batch_pointer=BatchPointer(
                    datasource_name="default_pandas_datasource",
                    data_asset_name="#ephemeral_pandas_asset",
                    batch_id="default_pandas_datasource-#ephemeral_pandas_asset",
                ),
                metric_name="table.columns",
                metric_domain_kwargs={},
                metric_value_kwargs={},
                column=None,
                value=Value(
                    value=[
                        "vendor_id",
                        "pickup_datetime",
                        "dropoff_datetime",
                        "passenger_count",
                        "trip_distance",
                        "rate_code_id",
                        "store_and_fwd_flag",
                        "pickup_location_id",
                        "dropoff_location_id",
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
                ),
                details={},
            ),
        ]
    )
