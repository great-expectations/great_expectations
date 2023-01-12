import pathlib

import pytest

from great_expectations import DataContext
from great_expectations.data_context.util import file_relative_path
from tests.experimental.datasources.integration_test_util import (
    run_checkpoint_and_datadoc_on_taxi_data_2019_01,
)


@pytest.fixture
def csv_path() -> pathlib.Path:
    return pathlib.Path(
        file_relative_path(__file__, "../../test_sets/taxi_yellow_tripdata_samples")
    ).absolute()


@pytest.mark.integration
@pytest.mark.slow  # 2s
@pytest.mark.parametrize("include_rendered_content", [False, True])
def test_run_checkpoint_and_data_doc(
    empty_data_context, include_rendered_content, csv_path
):
    context: DataContext = empty_data_context
    panda_ds = context.sources.add_pandas(name="my_pandas")
    # Add and configure a data asset
    asset = panda_ds.add_csv_asset(
        name="csv_asset",
        data_path=csv_path,
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
        order_by=["year", "month"],
    )
    batch_request = asset.get_batch_request({"year": "2019", "month": "01"})
    run_checkpoint_and_datadoc_on_taxi_data_2019_01(
        context, include_rendered_content, batch_request
    )
