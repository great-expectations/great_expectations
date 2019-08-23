from freezegun import freeze_time

try:
    from unittest import mock
except ImportError:
    import mock

from great_expectations.datasource.types import *
from great_expectations.datasource.generator import GlobReaderGenerator


@freeze_time("1955-11-05")
def test_batch_kwargs_id():
    test_batch_kwargs = PandasDatasourcePathBatchKwargs(
        {
            "path": "/data/test.csv"
        }
    )

    # When there is only a single "important" key used in batch_kwargs, the ID can prominently include it
    assert test_batch_kwargs.batch_id == "19551105T000000.000000Z::path:/data/test.csv"

    test_batch_kwargs = PandasDatasourcePathBatchKwargs(
        {
            "path": "/data/test.csv",
            "partition_id": "1"
        }
    )

    # When there are multiple relevant keys -- even if one is the partition_id -- we use the
    # hash of the batch_kwargs dictionary
    assert test_batch_kwargs.batch_id == "1::ed101097759db5a5f5f3bfee08bc5e70"


def test_batch_kwargs_path_partitioning():
    test_asset_globs = {
        "test_asset": {
            "glob": "*",
            "partition_regex": r"^((19|20)\d\d[- /.]?(0[1-9]|1[012])[- /.]?(0[1-9]|[12][0-9]|3[01]))_(.*)\.csv"
        }
    }
    glob_generator = GlobReaderGenerator("test_generator", asset_globs=test_asset_globs)

    with mock.patch("glob.glob") as mock_glob:
        mock_glob_match = [
            "20190101__my_data.csv",
            "20190102__my_data.csv",
            "20190102__my_data.csv",
            "20190103__my_data.csv",
            "20190104__my_data.csv"
        ]
        mock_glob.return_value = mock_glob_match
        kwargs = [kwargs for kwargs in glob_generator.get_iterator("test_asset")]

    assert len(kwargs) == len(mock_glob_match)
    assert kwargs[0]["path"] == "20190101__my_data.csv"
    assert kwargs[0]["partition_id"] == "20190101"
    assert "timestamp" in kwargs[0]
    assert len(kwargs[0].keys()) == 3
