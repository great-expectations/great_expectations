import pytest

from great_expectations.datasource.batch_kwargs_generator import (
    GlobReaderBatchKwargsGenerator,
)
from great_expectations.datasource.types import (
    PandasDatasourceBatchKwargs,
    PathBatchKwargs,
    SparkDFDatasourceBatchKwargs,
)
from great_expectations.exceptions import BatchKwargsError

try:
    from unittest import mock
except ImportError:
    from unittest import mock


@pytest.fixture(scope="module")
def mocked_glob_kwargs(basic_pandas_datasource):
    test_asset_globs = {
        "test_asset": {
            "glob": "*",
            "partition_regex": r"^((19|20)\d\d[- /.]?(0[1-9]|1[012])[- /.]?(0[1-9]|[12][0-9]|3[01]))_(.*)\.csv",
            "match_group_id": 1,
        }
    }
    glob_generator = GlobReaderBatchKwargsGenerator(
        "test_generator",
        datasource=basic_pandas_datasource,
        asset_globs=test_asset_globs,
    )

    with mock.patch("glob.glob") as mock_glob:
        mock_glob_match = [
            "20190101__my_data.csv",
            "20190102__my_data.csv",
            "20190103__my_data.csv",
            "20190104__my_data.csv",
            "20190105__my_data.csv",
        ]
        mock_glob.return_value = mock_glob_match
        kwargs = [
            kwargs
            for kwargs in glob_generator.get_iterator(data_asset_name="test_asset")
        ]
    return kwargs


def test_glob_reader_generator_returns_typed_kwargs(mocked_glob_kwargs):
    # Returned Kwargs should be PathKwargs.
    assert all([isinstance(kwargs, PathBatchKwargs) for kwargs in mocked_glob_kwargs])
    # Path Kwargs should be usable by PandasDatasource and SparkDFDatasource
    assert issubclass(PathBatchKwargs, PandasDatasourceBatchKwargs)
    assert issubclass(PathBatchKwargs, SparkDFDatasourceBatchKwargs)


def test_glob_reader_path_partitioning(mocked_glob_kwargs):
    kwargs = mocked_glob_kwargs
    # GlobReaderBatchKwargsGenerator uses partition_regex to extract partitions from filenames
    assert len(kwargs) == 5
    assert kwargs[0]["path"] == "20190101__my_data.csv"
    assert len(kwargs[0].keys()) == 2


def test_glob_reader_generator_relative_path(basic_pandas_datasource):
    glob_generator = GlobReaderBatchKwargsGenerator(
        "test_generator",
        datasource=basic_pandas_datasource,
        base_directory="../data/project/",
        reader_options={"sep": "|", "quoting": 3},
        reader_method="read_csv",
        asset_globs={
            "asset1": {
                "glob": "asset1/*__my_data.csv",
                "partition_regex": r"^.*(20\d\d\d\d\d\d)__my_data\.csv$",
                "match_group_id": 1,  # This is optional
            },
            "asset2": {
                "glob": "asset2/*__my_data.csv",
                "partition_regex": r"^.*(20\d\d\d\d\d\d)__my_data\.csv$",
            },
            "asset3": {"glob": "asset3/my_data.parquet", "reader_method": "parquet"},
            "no_partition_asset1": {"glob": "no_partition_asset1/*.csv"},
            "no_partition_asset2": {"glob": "my_data.csv"},
        },
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
            "/data/project/my_data.csv",
        ]
        mock_glob.return_value = mock_glob_match
        is_dir.return_value = True
        names = glob_generator.get_available_data_asset_names()
        # Use set in test to avoid order issues
        assert set(names["names"]) == {
            ("asset1", "path"),
            ("asset2", "path"),
            ("asset3", "path"),
            ("no_partition_asset1", "path"),
            ("no_partition_asset2", "path"),
        }


def test_glob_reader_generator_partitioning(basic_pandas_datasource):
    glob_generator = GlobReaderBatchKwargsGenerator(
        "test_generator",
        datasource=basic_pandas_datasource,
        base_directory="/data/project/",
        reader_options={"sep": "|", "quoting": 3},
        reader_method="read_csv",
        asset_globs={
            "asset1": {
                "glob": "asset1/*__my_data.csv",
                "partition_regex": r"^.*(20\d\d\d\d\d\d)__my_data\.csv$",
                "match_group_id": 1,  # This is optional
            },
            "asset2": {
                "glob": "asset2/*__my_data.csv",
                "partition_regex": r"^.*(20\d\d\d\d\d\d)__my_data\.csv$",
            },
            "asset3": {"glob": "asset3/my_data.parquet", "reader_method": "parquet"},
            "no_partition_asset1": {"glob": "no_partition_asset1/*.csv"},
            "no_partition_asset2": {"glob": "my_data.csv"},
        },
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
            "/data/project/my_data.csv",
        ]
        mock_glob.return_value = mock_glob_match
        is_dir.return_value = True
        names = glob_generator.get_available_data_asset_names()
        # Use set in test to avoid order issues
        assert set(names["names"]) == {
            ("asset1", "path"),
            ("asset2", "path"),
            ("asset3", "path"),
            ("no_partition_asset1", "path"),
            ("no_partition_asset2", "path"),
        }

    with mock.patch("glob.glob") as mock_glob, mock.patch("os.path.isdir") as is_dir:
        mock_glob_match = [
            "/data/project/asset1/20190101__my_data.csv",
            "/data/project/asset1/20190102__my_data.csv",
            "/data/project/asset1/20190103__my_data.csv",
            "/data/project/asset1/20190104__my_data.csv",
            "/data/project/asset1/20190105__my_data.csv",
        ]
        mock_glob.return_value = mock_glob_match
        is_dir.return_value = True
        partitions = glob_generator.get_available_partition_ids(
            data_asset_name="asset1"
        )
        # Use set in test to avoid order issues
        assert set(partitions) == {
            "20190101",
            "20190102",
            "20190103",
            "20190104",
            "20190105",
        }
        batch_kwargs = glob_generator.build_batch_kwargs(
            data_asset_name="asset1", partition_id="20190101"
        )
        assert isinstance(batch_kwargs, PathBatchKwargs)
        assert batch_kwargs["path"] == "/data/project/asset1/20190101__my_data.csv"
        assert batch_kwargs["reader_options"]["sep"] == "|"
        assert batch_kwargs["reader_options"]["quoting"] == 3
        assert batch_kwargs["reader_method"] == "read_csv"
        assert batch_kwargs["datasource"] == "basic_pandas_datasource"
        assert batch_kwargs["data_asset_name"] == "asset1"
        assert len(batch_kwargs) == 5

    with mock.patch("glob.glob") as mock_glob, mock.patch("os.path.isdir") as is_dir:
        mock_glob_match = [
            "/data/project/no_partition_asset1/this_is_a_batch_of_data.csv",
            "/data/project/no_partition_asset1/this_is_another_batch_of_data.csv",
        ]
        mock_glob.return_value = mock_glob_match
        is_dir.return_value = True
        partitions = glob_generator.get_available_partition_ids(
            data_asset_name="no_partition_asset1"
        )
        # Use set in test to avoid order issues
        assert set(partitions) == {
            "no_partition_asset1/this_is_a_batch_of_data.csv",
            "no_partition_asset1/this_is_another_batch_of_data.csv",
        }
        with pytest.raises(BatchKwargsError):
            # There is no valid partition id defined
            batch_kwargs = glob_generator.build_batch_kwargs(
                data_asset_name="no_partition_asset1",
                partition_id="this_is_a_batch_of_data.csv",
            )

        # ... but we *can* fall back to a path as the partition_id, though it is not advised
        batch_kwargs = glob_generator.build_batch_kwargs(
            data_asset_name="no_partition_asset1",
            partition_id="no_partition_asset1/this_is_a_batch_of_data.csv",
        )
        assert isinstance(batch_kwargs, PathBatchKwargs)
        assert (
            batch_kwargs["path"]
            == "/data/project/no_partition_asset1/this_is_a_batch_of_data.csv"
        )
        assert batch_kwargs["reader_options"]["sep"] == "|"
        assert batch_kwargs["reader_options"]["quoting"] == 3
        assert batch_kwargs["datasource"] == "basic_pandas_datasource"
        assert batch_kwargs["data_asset_name"] == "no_partition_asset1"
        assert len(batch_kwargs) == 5

        # When partition isn't really well defined, though, the preferred way is to use yield_batch_kwargs
        batch_kwargs = glob_generator.yield_batch_kwargs("no_partition_asset1")
        assert isinstance(batch_kwargs, PathBatchKwargs)
        assert batch_kwargs["path"] in mock_glob_match

        # We do define a partition_id in this case, using the default fallback logic of relative path
        assert batch_kwargs["reader_options"]["sep"] == "|"
        assert batch_kwargs["reader_options"]["quoting"] == 3
        assert batch_kwargs["datasource"] == "basic_pandas_datasource"
        assert len(batch_kwargs) == 4

        # We should be able to pass limit as well
        batch_kwargs = glob_generator.yield_batch_kwargs(
            "no_partition_asset1", limit=10
        )
        assert isinstance(batch_kwargs, PathBatchKwargs)
        assert batch_kwargs["path"] in mock_glob_match

        assert batch_kwargs["reader_options"]["sep"] == "|"
        assert batch_kwargs["reader_options"]["quoting"] == 3
        assert batch_kwargs["reader_options"]["nrows"] == 10
        assert len(batch_kwargs) == 4

    with mock.patch("glob.glob") as mock_glob, mock.patch("os.path.isdir") as is_dir:
        mock_glob_match = ["/data/project/asset3/mydata.parquet"]
        mock_glob.return_value = mock_glob_match
        is_dir.return_value = True
        batch_kwargs = glob_generator.yield_batch_kwargs("asset3")
        assert batch_kwargs["reader_method"] == "parquet"


def test_glob_reader_generator_customize_partitioning(basic_pandas_datasource):
    from dateutil.parser import parse as parse

    # We can subclass the generator to change the way that it builds partitions
    class DateutilPartitioningGlobReaderGenerator(GlobReaderBatchKwargsGenerator):
        def _partitioner(self, path, glob_):
            return parse(path, fuzzy=True).strftime("%Y-%m-%d")

    # default asset blob is ok
    glob_generator = DateutilPartitioningGlobReaderGenerator(
        "test_generator", basic_pandas_datasource
    )

    with mock.patch("glob.glob") as mock_glob:
        mock_glob_match = [
            "20190101__my_data.csv",
            "20190102__my_data.csv",
            "20190103__my_data.csv",
            "20190104__my_data.csv",
            "20190105__my_data.csv",
        ]
        mock_glob.return_value = mock_glob_match
        partitions = set(
            glob_generator.get_available_partition_ids(data_asset_name="default")
        )

    # Our custom partitioner will have used dateutil to parse. Note that it can then use any date format we chose
    assert partitions == {
        "2019-01-01",
        "2019-01-02",
        "2019-01-03",
        "2019-01-04",
        "2019-01-05",
    }
