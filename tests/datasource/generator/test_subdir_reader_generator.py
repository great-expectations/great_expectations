import pytest

import os

from great_expectations.data_context.util import safe_mmkdir

from great_expectations.datasource.generator import SubdirReaderGenerator


def test_subdir_reader_path_partitioning(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("test_folder_connection_path"))
    mock_files = [
        "asset_1/20190101__asset_1.csv",
        "asset_1/20190102__asset_1.csv",
        "asset_1/20190103__asset_1.csv",
        "asset_2/20190101__asset_2.csv",
        "asset_2/20190102__asset_2.csv"
    ]
    for file in mock_files:
        safe_mmkdir(os.path.join(base_directory, file.split("/")[0]))
        open(os.path.join(base_directory, file), "w").close()

    subdir_reader_generator = SubdirReaderGenerator("test_generator", base_directory=base_directory)

    # We should see two assets
    known_assets = subdir_reader_generator.get_available_data_asset_names()
    # Use set in test to avoid order issues
    assert set(known_assets) == {"asset_1", "asset_2"}

    # We should see three partitions for the first:
    known_partitions = subdir_reader_generator.get_available_partition_ids("asset_1")
    assert set(known_partitions) == {
        "20190101__asset_1",
        "20190102__asset_1",
        "20190103__asset_1"
    }

    asset_1_kwargs = [kwargs for kwargs in subdir_reader_generator.get_iterator("asset_1")]
    asset_2_kwargs = [kwargs for kwargs in subdir_reader_generator.get_iterator("asset_2")]
    with pytest.raises(IOError):
        not_an_asset_kwargs = [kwargs for kwargs in subdir_reader_generator.get_iterator("not_an_asset")]

    assert len(asset_1_kwargs) == 3
    paths = [kwargs["path"] for kwargs in asset_1_kwargs]
    assert set(paths) == {
        os.path.join(base_directory, "asset_1/20190101__asset_1.csv"),
        os.path.join(base_directory, "asset_1/20190102__asset_1.csv"),
        os.path.join(base_directory, "asset_1/20190103__asset_1.csv")
    }
    partitions = [kwargs["partition_id"] for kwargs in asset_1_kwargs]

    # SubdirReaderGenerator uses filenames from subdirectories to generate partition names
    assert set(partitions) == {
        "20190101__asset_1",
        "20190102__asset_1",
        "20190103__asset_1"
    }
    assert len(asset_1_kwargs[0].keys()) == 2

    assert len(asset_2_kwargs) == 2
    paths = [kwargs["path"] for kwargs in asset_2_kwargs]
    assert set(paths) == {
        os.path.join(base_directory, "asset_2/20190101__asset_2.csv"),
        os.path.join(base_directory, "asset_2/20190102__asset_2.csv")
    }
    partitions = [kwargs["partition_id"] for kwargs in asset_2_kwargs]
    assert set(partitions) == {
        "20190101__asset_2",
        "20190102__asset_2"
    }
    assert len(asset_2_kwargs[0].keys()) == 2


def test_subdir_reader_file_partitioning(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("test_folder_connection_path"))
    mock_files = [
        "20190101__asset_1.csv",
        "20190102__asset_1.csv",
        "20190103__asset_1.csv",
        "asset_2/20190101__asset_2.csv",
        "asset_2/20190102__asset_2.csv"
    ]
    for file in mock_files:
        if "/" in file:
            safe_mmkdir(os.path.join(base_directory, file.split("/")[0]))
        open(os.path.join(base_directory, file), "w").close()

    # If we have files, we should see them as individual assets
    subdir_reader_generator = SubdirReaderGenerator("test_generator", base_directory=base_directory)

    known_assets = subdir_reader_generator.get_available_data_asset_names()
    assert set(known_assets) == {
        "20190101__asset_1",
        "20190102__asset_1",
        "20190103__asset_1",
        "asset_2"
    }

    # SubdirReaderGenerator uses the filename as partition name for root files
    known_partitions = subdir_reader_generator.get_available_partition_ids("20190101__asset_1")
    assert set(known_partitions) == {"20190101__asset_1"}

    kwargs = subdir_reader_generator.build_batch_kwargs_from_partition_id("20190101__asset_1", "20190101__asset_1")
    assert kwargs["path"] == os.path.join(base_directory, "20190101__asset_1.csv")
