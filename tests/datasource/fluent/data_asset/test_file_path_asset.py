import pathlib
from typing import List

import pytest

from great_expectations.core.partitioners import PartitionerYearAndMonth
from great_expectations.datasource.fluent.pandas_filesystem_datasource import (
    PandasFilesystemDatasource,
)


@pytest.fixture
def validated_pandas_filesystem_datasource(
    pandas_filesystem_datasource: PandasFilesystemDatasource,
):
    years = ["2018", "2019", "2020"]
    all_files: List[str] = [
        file_name.stem
        for file_name in list(pathlib.Path(pandas_filesystem_datasource.base_directory).iterdir())
    ]
    # assert there are 12 files for each year
    for year in years:
        files_for_year = [
            file_name
            for file_name in all_files
            if file_name.find(f"yellow_tripdata_sample_{year}") == 0
        ]
        assert len(files_for_year) == 12
    return pandas_filesystem_datasource


@pytest.mark.filesystem
def test_get_batch_list_from_batch_request__sort_ascending(
    validated_pandas_filesystem_datasource: PandasFilesystemDatasource,
):
    """Verify that get_batch_list_from_batch_request respects a partitioner's ascending sort order.

    NOTE: we just happen to be using pandas as the concrete class.
    """
    asset = validated_pandas_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
    )
    batch_definition = asset.add_batch_definition(
        "foo",
        partitioner=PartitionerYearAndMonth(
            column_name="TODO: delete column from this partitioner", sort_ascending=True
        ),
    )
    batch_request = batch_definition.build_batch_request()

    batches = asset.get_batch_list_from_batch_request(batch_request)

    expected_years = ["2018"] * 12 + ["2019"] * 12 + ["2020"] * 12
    expected_months = [format(m, "02d") for m in range(1, 13)] * 3

    assert (len(batches)) == 36
    for i, batch in enumerate(batches):
        assert batch.metadata["year"] == str(expected_years[i])
        assert batch.metadata["month"] == str(expected_months[i])


@pytest.mark.filesystem
def test_get_batch_list_from_batch_request__sort_descending(
    validated_pandas_filesystem_datasource: PandasFilesystemDatasource,
):
    """Verify that get_batch_list_from_batch_request respects a partitioner's descending sort order.

    NOTE: we just happen to be using pandas as the concrete class.
    """
    asset = validated_pandas_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
    )
    batch_definition = asset.add_batch_definition(
        "foo",
        partitioner=PartitionerYearAndMonth(
            column_name="TODO: delete column from this partitioner", sort_ascending=False
        ),
    )
    batch_request = batch_definition.build_batch_request()

    batches = asset.get_batch_list_from_batch_request(batch_request)

    expected_years = list(reversed(["2018"] * 12 + ["2019"] * 12 + ["2020"] * 12))
    expected_months = list(reversed([format(m, "02d") for m in range(1, 13)] * 3))

    assert (len(batches)) == 36
    for i, batch in enumerate(batches):
        assert batch.metadata["year"] == str(expected_years[i])
        assert batch.metadata["month"] == str(expected_months[i])
