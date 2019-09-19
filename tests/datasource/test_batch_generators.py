import pytest

import os

try:
    from unittest import mock
except ImportError:
    import mock

from great_expectations.exceptions import DataContextError
from great_expectations.datasource.generator import SubdirReaderGenerator, GlobReaderGenerator, DatabricksTableGenerator
from great_expectations.datasource.types import BatchKwargs, PathBatchKwargs
from great_expectations.exceptions import BatchKwargsError


def test_file_kwargs_generator(data_context, filesystem_csv):
    base_dir = filesystem_csv

    datasource = data_context.add_datasource("default",
                                        module_name="great_expectations.datasource",
                                        class_name="PandasDatasource",
                                        base_directory=str(base_dir))
    generator = datasource.get_generator("default")
    known_data_asset_names = datasource.get_available_data_asset_names()

    assert known_data_asset_names["default"] == {"f1", "f2", "f3"}

    f1_batches = [batch_kwargs for batch_kwargs in generator.get_iterator("f1")]
    assert len(f1_batches) == 1
    assert "timestamp" in f1_batches[0]
    del f1_batches[0]["timestamp"]
    assert f1_batches[0] == {
            "path": os.path.join(base_dir, "f1.csv"),
            "partition_id": "f1",
            "sep": None,
            "engine": "python"
        }

    f3_batches = [batch_kwargs["path"] for batch_kwargs in generator.get_iterator("f3")]
    expected_batches = [
        {
            "path": os.path.join(base_dir, "f3", "f3_20190101.csv")
        },
        {
            "path": os.path.join(base_dir, "f3", "f3_20190102.csv")
        }
    ]
    for batch in expected_batches:
        assert batch["path"] in f3_batches
    assert len(f3_batches) == 2


def test_file_kwargs_generator_error(data_context, filesystem_csv):
    base_dir = filesystem_csv
    data_context.add_datasource("default",
                                module_name="great_expectations.datasource",
                                class_name="PandasDatasource",
                                base_directory=str(base_dir))

    with pytest.raises(DataContextError) as exc:
        data_context.get_batch("f4")
        assert "f4" in exc.message


def test_glob_reader_generator(tmp_path_factory):
    """Provides an example of how glob generator works: we specify our own
    names for data_assets, and an associated glob; the generator
    will take care of providing batches consisting of one file per
    batch corresponding to the glob."""
    
    basedir = str(tmp_path_factory.mktemp("test_glob_reader_generator"))

    with open(os.path.join(basedir, "f1.blarg"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f2.csv"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f3.blarg"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f4.blarg"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f5.blarg"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f6.blarg"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f7.xls"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f8.parquet"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f9.xls"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f0.json"), "w") as outfile:
        outfile.write("\n\n\n")

    g2 = GlobReaderGenerator(base_directory=basedir, asset_globs={
        "blargs": {
            "glob": "*.blarg"
        },
        "fs": {
            "glob": "f*"
        }
    })

    g2_assets = g2.get_available_data_asset_names()
    assert g2_assets == {"blargs", "fs"}

    with pytest.warns(DeprecationWarning):
        # This is an old style of asset_globs configuration that should raise a deprecationwarning
        g2 = GlobReaderGenerator(base_directory=basedir, asset_globs={
            "blargs": "*.blarg",
            "fs": "f*"
        })
        g2_assets = g2.get_available_data_asset_names()
        assert g2_assets == {"blargs", "fs"}

    blargs_kwargs = [x["path"] for x in g2.get_iterator("blargs")]
    real_blargs = [
        os.path.join(basedir, "f1.blarg"),
        os.path.join(basedir, "f3.blarg"),
        os.path.join(basedir, "f4.blarg"),
        os.path.join(basedir, "f5.blarg"),
        os.path.join(basedir, "f6.blarg")
    ]
    for kwargs in real_blargs:
        assert kwargs in blargs_kwargs

    assert len(blargs_kwargs) == len(real_blargs)


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
        assert names == {"asset1", "asset2", "no_partition_asset1", "no_partition_asset2"}

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
        assert partitions == {
            "20190101",
            "20190102",
            "20190103",
            "20190104",
            "20190105",
        }
        batch_kwargs = glob_generator.build_batch_kwargs_from_partition("asset1", "20190101")
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
        assert partitions == {
            'no_partition_asset1/this_is_a_batch_of_data.csv',
            'no_partition_asset1/this_is_another_batch_of_data.csv'
        }
        with pytest.raises(BatchKwargsError):
            # There is no valid partition id defined
            batch_kwargs = glob_generator.build_batch_kwargs_from_partition("no_partition_asset1", "this_is_a_batch_of_data.csv")

        with pytest.raises(BatchKwargsError):
            # ...and partition_id of none is unsuccessful
            batch_kwargs = glob_generator.build_batch_kwargs_from_partition("no_partition_asset1", partition_id=None)

        # ... but we *can* fall back to a path as the partition_id, though it is not advised
        batch_kwargs = glob_generator.build_batch_kwargs_from_partition("no_partition_asset1",
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

    with mock.patch("glob.glob") as mock_glob:
        mock_glob_match = [
            "/data/project/asset/20190101__my_data.csv",
            "/data/project/asset/20190102__my_data.csv",
            "/data/project/asset/20190103__my_data.csv",
            "/data/project/asset/20190104__my_data.csv",
            "/data/project/asset/20190105__my_data.csv"
        ]
        mock_glob.return_value = mock_glob_match
        default_asset_kwargs = [kwargs for kwargs in glob_generator.get_iterator("default")]


def test_file_kwargs_generator_extensions(tmp_path_factory):
    """csv, xls, parquet, json should be recognized file extensions"""
    basedir = str(tmp_path_factory.mktemp("test_file_kwargs_generator_extensions"))

    # Do not include: invalid extension
    with open(os.path.join(basedir, "f1.blarg"), "w") as outfile:
        outfile.write("\n\n\n")
    # Include
    with open(os.path.join(basedir, "f2.csv"), "w") as outfile:
        outfile.write("\n\n\n")
    # Do not include: valid subdir, but no valid files in it
    os.mkdir(os.path.join(basedir, "f3"))
    with open(os.path.join(basedir, "f3", "f3_1.blarg"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f3", "f3_2.blarg"), "w") as outfile:
        outfile.write("\n\n\n")
    # Include: valid subdir with valid files
    os.mkdir(os.path.join(basedir, "f4"))
    with open(os.path.join(basedir, "f4", "f4_1.csv"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f4", "f4_2.csv"), "w") as outfile:
        outfile.write("\n\n\n")
    # Do not include: valid extension, but dot prefix
    with open(os.path.join(basedir, ".f5.csv"), "w") as outfile:
        outfile.write("\n\n\n")
    
    # Include: valid extensions
    with open(os.path.join(basedir, "f6.tsv"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f7.xls"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f8.parquet"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f9.xls"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(basedir, "f0.json"), "w") as outfile:
        outfile.write("\n\n\n")

    g1 = SubdirReaderGenerator(base_directory=basedir)

    g1_assets = g1.get_available_data_asset_names()
    assert g1_assets == {"f2", "f4", "f6", "f7", "f8", "f9", "f0"}


def test_databricks_generator():
    generator = DatabricksTableGenerator()
    available_assets = generator.get_available_data_asset_names()

    # We have no tables available
    assert available_assets == set()

    databricks_kwargs_iterator = generator.get_iterator("foo")
    kwargs = [batch_kwargs for batch_kwargs in databricks_kwargs_iterator]
    assert "timestamp" in kwargs[0]
    assert "select * from" in kwargs[0]["query"].lower()
