import pytest

import os

from great_expectations.data_context.util import safe_mmkdir

from great_expectations.datasource.generator import SubdirReaderGenerator
from great_expectations.datasource.types import BatchKwargs


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

    assert asset_1_kwargs[0].batch_kwargs_fingerprint == \
       asset_1_kwargs[0]["partition_id"] + "::" + "path:" + asset_1_kwargs[0]["path"]
    # FIXME: Or whether this one should pass (because timestamp *is* part of fingerprint)
    assert asset_1_kwargs[0].batch_id_fingerprint == BatchKwargs.build_batch_fingerprint(asset_1_kwargs[0])

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
