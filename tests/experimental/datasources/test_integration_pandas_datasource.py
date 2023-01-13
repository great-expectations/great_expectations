import pathlib
from typing import Tuple

import pytest

from great_expectations import DataContext
from great_expectations.data_context.util import file_relative_path
from great_expectations.experimental.datasources.interfaces import DataAsset, Datasource
from tests.experimental.datasources.integration_test_util import (
    run_checkpoint_and_datadoc_on_taxi_data_2019_01,
    run_data_assistant_and_checkpoint_on_month_of_taxi_data,
    run_multibatch_data_assistant_and_checkpoint_on_year_of_taxi_data,
)


@pytest.fixture
def pandas_data(empty_data_context) -> Tuple[Datasource, DataAsset]:
    csv_path = pathlib.Path(
        file_relative_path(__file__, "../../test_sets/taxi_yellow_tripdata_samples")
    ).absolute()
    context: DataContext = empty_data_context
    panda_ds = context.sources.add_pandas(name="my_pandas")
    # Add and configure a data asset
    asset = panda_ds.add_csv_asset(
        name="csv_asset",
        data_path=csv_path,
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
        order_by=["year", "month"],
    )
    return context, panda_ds, asset


@pytest.mark.integration
@pytest.mark.parametrize("include_rendered_content", [False, True])
def test_run_checkpoint_and_data_doc(pandas_data, include_rendered_content):
    context, _, asset = pandas_data
    batch_request = asset.get_batch_request({"year": "2019", "month": "01"})
    run_checkpoint_and_datadoc_on_taxi_data_2019_01(
        context, include_rendered_content, batch_request
    )


@pytest.mark.integration
@pytest.mark.slow  # 4s
def test_run_data_assistant_and_checkpoint(pandas_data):
    """Test using data assistants to create expectation suite and run checkpoint"""
    context, _, asset = pandas_data
    batch_request = asset.get_batch_request({"year": "2019", "month": "01"})
    run_data_assistant_and_checkpoint_on_month_of_taxi_data(context, batch_request)


@pytest.mark.integration
@pytest.mark.slow  # 9s
def test_run_multibatch_data_assistant_and_checkpoint(pandas_data):
    """Test using data assistants to create expectation suite and run checkpoint"""
    context, _, asset = pandas_data
    batch_request = asset.get_batch_request({"year": "2020"})
    run_multibatch_data_assistant_and_checkpoint_on_year_of_taxi_data(
        context, batch_request
    )
