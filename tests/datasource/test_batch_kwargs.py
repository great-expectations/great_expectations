import pytest

import os

from freezegun import freeze_time

try:
    from unittest import mock
except ImportError:
    import mock

from great_expectations.data_context.util import safe_mmkdir
from great_expectations.datasource.types import *
from great_expectations.datasource.generator import SubdirReaderGenerator, GlobReaderGenerator


@freeze_time("1955-11-05")
def test_batch_kwargs_id():
    test_batch_kwargs = PathBatchKwargs(
        {
            "path": "/data/test.csv"
        }
    )

    # When there is only a single "important" key used in batch_kwargs, the ID can prominently include it
    assert test_batch_kwargs.batch_id == "19551105T000000.000000Z::path:/data/test.csv"

    test_batch_kwargs = PathBatchKwargs(
        {
            "path": "/data/test.csv",
            "partition_id": "1"
        }
    )

    # When partition_id is explicitly included, we can extract it and potentially still have a human readable id
    assert test_batch_kwargs.batch_id == "1::path:/data/test.csv"

    test_batch_kwargs = PathBatchKwargs(
        {
            "path": "/data/test.csv",
            "iterator": True,
            "partition_id": "3",
            "chunksize": 2e7,
            "parse_dates": [0, 3],
            "names": ["start", "type", "quantity", "end"]
        }
    )
    # When there are multiple relevant keys we use the hash of the batch_kwargs dictionary
    assert test_batch_kwargs.batch_id == "3::c2076ea127e6d98cb63b1a5da5024cee"


def test_batch_kwargs_from_dict():
    test_batch_kwargs = {
            "path": "/data/test.csv",
            "partition_id": "1"
        }

    # The build_batch_id convenience method makes it possible to build a batch_id from a dict.
    # HOWEVER, using it can be difficult since the default-ignored keys may depend on a specific batch_kwargs type
    assert BatchKwargs.build_batch_id(test_batch_kwargs) == "1::path:/data/test.csv"


def test_glob_reader_path_partitioning():
    test_asset_globs = {
        "test_asset": {
            "glob": "*",
            "partition_regex": r"^((19|20)\d\d[- /.]?(0[1-9]|1[012])[- /.]?(0[1-9]|[12][0-9]|3[01]))_(.*)\.csv",
            "match_group_id": 1
        }
    }
    glob_generator = GlobReaderGenerator("test_generator", asset_globs=test_asset_globs)

    with mock.patch("glob.glob") as mock_glob:
        mock_glob_match = [
            "20190101__my_data.csv",
            "20190102__my_data.csv",
            "20190103__my_data.csv",
            "20190104__my_data.csv",
            "20190105__my_data.csv"
        ]
        mock_glob.return_value = mock_glob_match
        kwargs = [kwargs for kwargs in glob_generator.get_iterator("test_asset")]

    # GlobReaderGenerator uses partition_regex to extract partitions from filenames
    assert len(kwargs) == len(mock_glob_match)
    assert kwargs[0]["path"] == "20190101__my_data.csv"
    assert kwargs[0]["partition_id"] == "20190101"
    assert "timestamp" in kwargs[0]
    assert len(kwargs[0].keys()) == 3


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

    asset_1_kwargs = [kwargs for kwargs in subdir_reader_generator.get_iterator("asset_1")]
    asset_2_kwargs = [kwargs for kwargs in subdir_reader_generator.get_iterator("asset_2")]
    with pytest.raises(IOError):
        not_an_asset_kwargs = [kwargs for kwargs in subdir_reader_generator.get_iterator("not_an_asset")]

    assert len(asset_1_kwargs) == 3
    paths = set([kwargs["path"] for kwargs in asset_1_kwargs])
    assert paths == {
        os.path.join(base_directory, "asset_1/20190101__asset_1.csv"),
        os.path.join(base_directory, "asset_1/20190102__asset_1.csv"),
        os.path.join(base_directory, "asset_1/20190103__asset_1.csv")
    }
    partitions = set([kwargs["partition_id"] for kwargs in asset_1_kwargs])

    # SubdirReaderGenerator uses filenames from subdirectories to generate partition names
    assert partitions == {
        "20190101__asset_1",
        "20190102__asset_1",
        "20190103__asset_1"
    }
    assert "timestamp" in asset_1_kwargs[0]
    assert len(asset_1_kwargs[0].keys()) == 3

    # FIXME: We need to resolve whether this test should pass (because timestamp is *not* part of id)
    # assert asset_1_kwargs[0].batch_id == \
    #    asset_1_kwargs[0]["partition_id"] + "::" + "path:" + asset_1_kwargs[0]["path"]
    # FIXME: Or whether this one should pass (because timestamp *is* part of id)
    assert asset_1_kwargs[0].batch_id == BatchKwargs.build_batch_id(asset_1_kwargs[0])

    assert len(asset_2_kwargs) == 2
    paths = set([kwargs["path"] for kwargs in asset_2_kwargs])
    assert paths == {
        os.path.join(base_directory, "asset_2/20190101__asset_2.csv"),
        os.path.join(base_directory, "asset_2/20190102__asset_2.csv")
    }
    partitions = set([kwargs["partition_id"] for kwargs in asset_2_kwargs])
    assert partitions == {
        "20190101__asset_2",
        "20190102__asset_2"
    }
    assert "timestamp" in asset_2_kwargs[0]
    assert len(asset_2_kwargs[0].keys()) == 3
