import pytest

try:
    from unittest import mock
except ImportError:
    import mock

from great_expectations.datasource.generator import GlobReaderGenerator
from great_expectations.datasource.types import (
    PathBatchKwargs,
    PandasDatasourceBatchKwargs,
    SparkDFDatasourceBatchKwargs
)
from great_expectations.exceptions import BatchKwargsError


@pytest.fixture(scope="module")
def mocked_glob_kwargs():
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
    return kwargs


def test_glob_reader_generator_returns_typed_kwargs(mocked_glob_kwargs):
    # Returned Kwargs should be PathKwargs.
    assert all(
        [isinstance(kwargs, PathBatchKwargs) for kwargs in mocked_glob_kwargs]
    )
    # Path Kwargs should be usable by PandasDatasource and SparkDFDatasource
    assert issubclass(PathBatchKwargs, PandasDatasourceBatchKwargs)
    assert issubclass(PathBatchKwargs, SparkDFDatasourceBatchKwargs)


def test_glob_reader_path_partitioning(mocked_glob_kwargs):
    kwargs = mocked_glob_kwargs
    # GlobReaderGenerator uses partition_regex to extract partitions from filenames
    assert len(kwargs) == 5
    assert kwargs[0]["path"] == "20190101__my_data.csv"
    assert kwargs[0]["partition_id"] == "20190101"
    assert "timestamp" in kwargs[0]
    assert len(kwargs[0].keys()) == 3


def test_glob_reader_generator_partitioning():
    glob_generator = GlobReaderGenerator(
        "test_generator",
        datasource=None,
        base_directory="/data/project/",
        reader_options={
            "sep": "|",
            "quoting": 3
        },
        asset_globs={
            "asset1": {
                "glob": "asset1/*__my_data.csv",
                "partition_regex": r"^.*(20\d\d\d\d\d\d)__my_data\.csv$",
                "match_group_id": 1  # This is optional
            },
            "asset2": {
                "glob": "asset2/*__my_data.csv",
                "partition_regex": r"^.*(20\d\d\d\d\d\d)__my_data\.csv$"
            },
            "no_partition_asset1": {
                "glob": "no_partition_asset1/*.csv"
            },
            "no_partition_asset2": {
                "glob": "my_data.csv"
            }
        }
                                         )

    with mock.patch("glob.glob") as mock_glob, mock.patch("os.path.isdir") as is_dir:
        mock_glob_match = [
            "/data/project/asset1/20190101__my_data.csv",
            "/data/project/asset1/20190102__my_data.csv",
            "/data/project/asset1/20190103__my_data.csv",
            "/data/project/asset1/20190104__my_data.csv",
            "/data/project/asset1/20190105__my_data.csv",
            "/data/project/asset2/20190101__my_data.csv",
            "/data/project/asset2/20190102__my_data.csv",
            "/data/project/asset2/20190103__my_data.csv",
            "/data/project/asset2/20190104__my_data.csv",
            "/data/project/asset2/20190105__my_data.csv",
            "/data/project/no_partition_asset1/this_is_a_batch_of_data.csv",
            "/data/project/no_partition_asset1/this_is_another_batch_of_data.csv",
            "/data/project/my_data.csv"
        ]
        mock_glob.return_value = mock_glob_match
        is_dir.return_value = True
        names = glob_generator.get_available_data_asset_names()
        # Use set in test to avoid order issues
        assert set(names) == {"asset1", "asset2", "no_partition_asset1", "no_partition_asset2"}

    with mock.patch("glob.glob") as mock_glob, mock.patch("os.path.isdir") as is_dir:
        mock_glob_match = [
                "/data/project/asset1/20190101__my_data.csv",
                "/data/project/asset1/20190102__my_data.csv",
                "/data/project/asset1/20190103__my_data.csv",
                "/data/project/asset1/20190104__my_data.csv",
                "/data/project/asset1/20190105__my_data.csv"
            ]
        mock_glob.return_value = mock_glob_match
        is_dir.return_value = True
        partitions = glob_generator.get_available_partition_ids("asset1")
        # Use set in test to avoid order issues
        assert set(partitions) == {
            "20190101",
            "20190102",
            "20190103",
            "20190104",
            "20190105",
        }
        batch_kwargs = glob_generator.build_batch_kwargs_from_partition_id("asset1", "20190101")
        assert isinstance(batch_kwargs, PathBatchKwargs)
        assert batch_kwargs["path"] == "/data/project/asset1/20190101__my_data.csv"
        assert "timestamp" in batch_kwargs
        assert batch_kwargs["partition_id"] == "20190101"
        assert batch_kwargs["sep"] == "|"
        assert batch_kwargs["quoting"] == 3
        assert len(batch_kwargs) == 5

    with mock.patch("glob.glob") as mock_glob, mock.patch("os.path.isdir") as is_dir:
        mock_glob_match = [
            "/data/project/no_partition_asset1/this_is_a_batch_of_data.csv",
            "/data/project/no_partition_asset1/this_is_another_batch_of_data.csv"
            ]
        mock_glob.return_value = mock_glob_match
        is_dir.return_value = True
        partitions = glob_generator.get_available_partition_ids("no_partition_asset1")
        # Use set in test to avoid order issues
        assert set(partitions) == {
            'no_partition_asset1/this_is_a_batch_of_data.csv',
            'no_partition_asset1/this_is_another_batch_of_data.csv'
        }
        with pytest.raises(BatchKwargsError):
            # There is no valid partition id defined
            batch_kwargs = glob_generator.build_batch_kwargs_from_partition_id("no_partition_asset1", "this_is_a_batch_of_data.csv")

        with pytest.raises(BatchKwargsError):
            # ...and partition_id of none is unsuccessful
            batch_kwargs = glob_generator.build_batch_kwargs_from_partition_id("no_partition_asset1", partition_id=None)

        # ... but we *can* fall back to a path as the partition_id, though it is not advised
        batch_kwargs = glob_generator.build_batch_kwargs_from_partition_id("no_partition_asset1",
                                                                        "no_partition_asset1/this_is_a_batch_of_data.csv")
        assert isinstance(batch_kwargs, PathBatchKwargs)
        assert batch_kwargs["path"] == "/data/project/no_partition_asset1/this_is_a_batch_of_data.csv"
        assert batch_kwargs["partition_id"] == "no_partition_asset1/this_is_a_batch_of_data.csv"
        assert "timestamp" in batch_kwargs
        assert batch_kwargs["sep"] == "|"
        assert batch_kwargs["quoting"] == 3
        assert len(batch_kwargs) == 5

        # When partition isn't really well defined, though, the preferred way is to use yield_batch_kwargs
        batch_kwargs = glob_generator.yield_batch_kwargs("no_partition_asset1")
        assert isinstance(batch_kwargs, PathBatchKwargs)
        assert batch_kwargs["path"] in mock_glob_match
        assert "timestamp" in batch_kwargs

        # We do define a partition_id in this case, using the default fallback logic of relative path
        assert batch_kwargs["partition_id"] == "no_partition_asset1/this_is_a_batch_of_data.csv"
        assert batch_kwargs["sep"] == "|"
        assert batch_kwargs["quoting"] == 3
        assert len(batch_kwargs) == 5


def test_glob_reader_generator_customize_partitioning():
    from dateutil.parser import parse as parse

    # We can subclass the generator to change the way that it builds partitions
    class DateutilPartitioningGlobReaderGenerator(GlobReaderGenerator):
        def _partitioner(self, path, glob_):
            return parse(path, fuzzy=True).strftime("%Y-%m-%d")

    glob_generator = DateutilPartitioningGlobReaderGenerator("test_generator")  # default asset blob is ok

    with mock.patch("glob.glob") as mock_glob:
        mock_glob_match = [
            "20190101__my_data.csv",
            "20190102__my_data.csv",
            "20190103__my_data.csv",
            "20190104__my_data.csv",
            "20190105__my_data.csv"
        ]
        mock_glob.return_value = mock_glob_match
        default_asset_kwargs = [kwargs for kwargs in glob_generator.get_iterator("default")]

    partitions = set([kwargs["partition_id"] for kwargs in default_asset_kwargs])

    # Our custom partitioner will have used dateutil to parse. Note that it can then use any date format we chose
    assert partitions == {
        "2019-01-01",
        "2019-01-02",
        "2019-01-03",
        "2019-01-04",
        "2019-01-05",
    }
