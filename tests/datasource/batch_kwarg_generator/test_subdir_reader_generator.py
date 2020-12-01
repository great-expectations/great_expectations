import os

import pytest

from great_expectations.datasource.batch_kwargs_generator import (
    SubdirReaderBatchKwargsGenerator,
)
from great_expectations.exceptions import BatchKwargsError


def test_subdir_reader_path_partitioning(basic_pandas_datasource, tmp_path_factory):
    base_directory = str(
        tmp_path_factory.mktemp("test_subdir_reader_path_partitioning")
    )
    mock_files = [
        "asset_1/20190101__asset_1.csv",
        "asset_1/20190102__asset_1.csv",
        "asset_1/20190103__asset_1.csv",
        "asset_2/20190101__asset_2.csv",
        "asset_2/20190102__asset_2.csv",
    ]
    for file in mock_files:
        os.makedirs(os.path.join(base_directory, file.split("/")[0]), exist_ok=True)
        open(os.path.join(base_directory, file), "w").close()

    subdir_reader_generator = SubdirReaderBatchKwargsGenerator(
        "test_generator",
        datasource=basic_pandas_datasource,
        base_directory=base_directory,
    )

    # We should see two assets
    known_assets = subdir_reader_generator.get_available_data_asset_names()["names"]
    # Use set in test to avoid order issues
    assert set(known_assets) == {("asset_2", "directory"), ("asset_1", "directory")}

    # We should see three partitions for the first:
    known_partitions = subdir_reader_generator.get_available_partition_ids(
        data_asset_name="asset_1"
    )
    assert set(known_partitions) == {
        "20190101__asset_1",
        "20190102__asset_1",
        "20190103__asset_1",
    }

    asset_1_kwargs = [
        kwargs
        for kwargs in subdir_reader_generator.get_iterator(data_asset_name="asset_1")
    ]
    asset_2_kwargs = [
        kwargs
        for kwargs in subdir_reader_generator.get_iterator(data_asset_name="asset_2")
    ]
    with pytest.raises(BatchKwargsError):
        not_an_asset_kwargs = [
            kwargs
            for kwargs in subdir_reader_generator.get_iterator(
                data_asset_name="not_an_asset"
            )
        ]

    assert len(asset_1_kwargs) == 3
    paths = [kwargs["path"] for kwargs in asset_1_kwargs]
    assert set(paths) == {
        os.path.join(base_directory, "asset_1/20190101__asset_1.csv"),
        os.path.join(base_directory, "asset_1/20190102__asset_1.csv"),
        os.path.join(base_directory, "asset_1/20190103__asset_1.csv"),
    }
    partitions = subdir_reader_generator.get_available_partition_ids(
        data_asset_name="asset_1"
    )

    # SubdirReaderBatchKwargsGenerator uses filenames from subdirectories to generate partition names
    assert set(partitions) == {
        "20190101__asset_1",
        "20190102__asset_1",
        "20190103__asset_1",
    }
    assert len(asset_1_kwargs[0].keys()) == 2

    assert len(asset_2_kwargs) == 2
    paths = [kwargs["path"] for kwargs in asset_2_kwargs]
    assert set(paths) == {
        os.path.join(base_directory, "asset_2/20190101__asset_2.csv"),
        os.path.join(base_directory, "asset_2/20190102__asset_2.csv"),
    }
    partitions = subdir_reader_generator.get_available_partition_ids(
        data_asset_name="asset_2"
    )
    assert set(partitions) == {("20190101__asset_2"), ("20190102__asset_2")}
    assert len(asset_2_kwargs[0].keys()) == 2


def test_subdir_reader_file_partitioning(basic_pandas_datasource, tmp_path_factory):
    base_directory = str(
        tmp_path_factory.mktemp("test_subdir_reader_file_partitioning")
    )
    mock_files = [
        "20190101__asset_1.csv",
        "20190102__asset_1.csv",
        "20190103__asset_1.csv",
        "asset_2/20190101__asset_2.csv",
        "asset_2/20190102__asset_2.csv",
    ]
    for file in mock_files:
        if "/" in file:
            os.makedirs(os.path.join(base_directory, file.split("/")[0]), exist_ok=True)
        open(os.path.join(base_directory, file), "w").close()

    # If we have files, we should see them as individual assets
    subdir_reader_generator = SubdirReaderBatchKwargsGenerator(
        "test_generator",
        datasource=basic_pandas_datasource,
        base_directory=base_directory,
    )

    known_assets = subdir_reader_generator.get_available_data_asset_names()["names"]
    assert set(known_assets) == {
        ("20190101__asset_1", "file"),
        ("20190102__asset_1", "file"),
        ("20190103__asset_1", "file"),
        ("asset_2", "directory"),
    }

    # SubdirReaderBatchKwargsGenerator uses the filename as partition name for root files
    known_partitions = subdir_reader_generator.get_available_partition_ids(
        data_asset_name="20190101__asset_1"
    )
    assert set(known_partitions) == {"20190101__asset_1"}

    kwargs = subdir_reader_generator.build_batch_kwargs(
        data_asset_name="20190101__asset_1", partition_id="20190101__asset_1"
    )
    assert kwargs["path"] == os.path.join(base_directory, "20190101__asset_1.csv")

    # We should also be able to pass a limit
    kwargs = subdir_reader_generator.build_batch_kwargs(
        data_asset_name="20190101__asset_1", partition_id="20190101__asset_1", limit=10
    )
    assert kwargs["path"] == os.path.join(base_directory, "20190101__asset_1.csv")
    assert kwargs["reader_options"]["nrows"] == 10


def test_subdir_reader_configurable_reader_method(
    basic_pandas_datasource, tmp_path_factory
):
    base_directory = str(
        tmp_path_factory.mktemp("test_subdir_reader_configurable_reader_method")
    )
    mock_files = [
        "20190101__asset_1.dat",
        "20190102__asset_1.dat",
        "20190103__asset_1.dat",
        "asset_2/20190101__asset_2.dat",
        "asset_2/20190102__asset_2.dat",
    ]
    for file in mock_files:
        if "/" in file:
            os.makedirs(os.path.join(base_directory, file.split("/")[0]), exist_ok=True)
        open(os.path.join(base_directory, file), "w").close()

    # If we have files, we should see them as individual assets
    subdir_reader_generator = SubdirReaderBatchKwargsGenerator(
        "test_generator",
        datasource=basic_pandas_datasource,
        base_directory=base_directory,
        reader_method="csv",
        known_extensions=[".dat"],
    )
    batch_kwargs = next(subdir_reader_generator.get_iterator(data_asset_name="asset_2"))
    assert batch_kwargs["reader_method"] == "csv"
