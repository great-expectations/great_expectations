import os
import re

import pandas as pd
import pytest

from great_expectations import DataContext
from great_expectations.core.batch import Batch
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.datasource import SparkDFDatasource
from great_expectations.exceptions import BatchKwargsError
from great_expectations.util import is_library_loadable
from great_expectations.validator.validator import BridgeValidator

yaml = YAMLHandler()


@pytest.fixture(scope="module")
def test_parquet_folder_connection_path(tmp_path_factory):
    pandas_version = re.match(r"(\d+)\.(\d+)\..+", pd.__version__)
    if pandas_version is None:
        raise ValueError("Unrecognized pandas version!")
    else:
        pandas_major_version = int(pandas_version.group(1))
        pandas_minor_version = int(pandas_version.group(2))
        if pandas_major_version == 0 and pandas_minor_version < 23:
            pytest.skip("Pandas version < 23 is no longer compatible with pyarrow")
    df1 = pd.DataFrame({"col_1": [1, 2, 3, 4, 5], "col_2": ["a", "b", "c", "d", "e"]})
    basepath = str(tmp_path_factory.mktemp("parquet_context"))
    df1.to_parquet(os.path.join(basepath, "test.parquet"))  # noqa: PTH118

    return basepath


def test_sparkdf_datasource_custom_data_asset(
    data_context_parameterized_expectation_suite,
    test_folder_connection_path_csv,
    spark_session,
):
    assert spark_session  # Ensure a spark session exists
    name = "test_sparkdf_datasource"
    # type_ = "spark"
    class_name = "SparkDFDatasource"

    data_asset_type_config = {
        "module_name": "custom_sparkdf_dataset",
        "class_name": "CustomSparkDFDataset",
    }
    data_context_parameterized_expectation_suite.add_datasource(
        name,
        class_name=class_name,
        data_asset_type=data_asset_type_config,
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": test_folder_connection_path_csv,
            }
        },
    )

    # We should now see updated configs
    with open(
        os.path.join(  # noqa: PTH118
            data_context_parameterized_expectation_suite.root_directory,
            "great_expectations.yml",
        ),
    ) as data_context_config_file:
        data_context_file_config = yaml.load(data_context_config_file)

    assert (
        data_context_file_config["datasources"][name]["data_asset_type"]["module_name"]
        == "custom_sparkdf_dataset"
    )
    assert (
        data_context_file_config["datasources"][name]["data_asset_type"]["class_name"]
        == "CustomSparkDFDataset"
    )

    # We should be able to get a dataset of the correct type from the datasource.
    data_context_parameterized_expectation_suite.add_expectation_suite(
        "test_sparkdf_datasource.default"
    )
    batch_kwargs = data_context_parameterized_expectation_suite.build_batch_kwargs(
        name, "subdir_reader", "test"
    )
    batch_kwargs["reader_options"] = {"header": True, "inferSchema": True}
    batch = data_context_parameterized_expectation_suite.get_batch(
        batch_kwargs=batch_kwargs,
        expectation_suite_name="test_sparkdf_datasource.default",
    )
    assert type(batch).__name__ == "CustomSparkDFDataset"
    res = batch.expect_column_approx_quantile_values_to_be_between(
        "col_1",
        quantile_ranges={"quantiles": [0.0, 1.0], "value_ranges": [[1, 1], [5, 5]]},
    )
    assert res.success is True


def test_force_reuse_spark_context(
    data_context_parameterized_expectation_suite, tmp_path_factory, test_backends
):
    """
    Ensure that an external sparkSession can be reused by specifying the
    force_reuse_spark_context argument.
    """
    if "SparkDFDataset" not in test_backends:
        pytest.skip("No spark backend selected.")
    from pyspark.sql import SparkSession  # isort:skip

    dataset_name = "test_spark_dataset"

    spark = SparkSession.builder.appName("local").master("local[1]").getOrCreate()
    data = {"col1": [0, 1, 2], "col2": ["a", "b", "c"]}

    spark_df = spark.createDataFrame(pd.DataFrame(data))
    tmp_parquet_filename = os.path.join(  # noqa: PTH118
        tmp_path_factory.mktemp(dataset_name).as_posix(), dataset_name
    )
    spark_df.write.format("parquet").save(tmp_parquet_filename)

    data_context_parameterized_expectation_suite.add_datasource(
        dataset_name,
        class_name="SparkDFDatasource",
        force_reuse_spark_context=True,
        module_name="great_expectations.datasource",
        batch_kwargs_generators={},
    )

    df = spark.read.format("parquet").load(tmp_parquet_filename)
    batch_kwargs = {"dataset": df, "datasource": dataset_name}
    _ = data_context_parameterized_expectation_suite.add_expectation_suite(dataset_name)
    batch = data_context_parameterized_expectation_suite.get_batch(
        batch_kwargs=batch_kwargs, expectation_suite_name=dataset_name
    )
    results = batch.expect_column_max_to_be_between("col1", min_value=1, max_value=100)
    assert results.success, "Failed to use external SparkSession"
    spark.stop()


def test_spark_kwargs_are_passed_through(
    data_context_parameterized_expectation_suite,
    tmp_path_factory,
    test_backends,
    spark_session,
):
    """
    Ensure that an external SparkSession is not stopped when the spark_config matches
    the one specified in the GX Context.
    """
    if "SparkDFDataset" not in test_backends:
        pytest.skip("No spark backend selected.")
    dataset_name = "test_spark_dataset"

    spark_config = dict(spark_session.sparkContext.getConf().getAll())
    data_context_parameterized_expectation_suite.add_datasource(
        dataset_name,
        class_name="SparkDFDatasource",
        spark_config=spark_config,
        force_reuse_spark_context=False,
        module_name="great_expectations.datasource",
        batch_kwargs_generators={},
    )
    datasource_config = data_context_parameterized_expectation_suite.get_datasource(
        dataset_name
    ).config

    actual_spark_config = datasource_config["spark_config"]
    expected_spark_config = dict(spark_session.sparkContext.getConf().getAll())

    # 20220714 - Chetan `spark.sql.warehouse.dir` intermittently shows up in Spark config
    # As the rest of the config adheres to expectations, we conditionally pop and assert
    # against known values in the payload.
    for config in (actual_spark_config, expected_spark_config):
        config.pop("spark.sql.warehouse.dir", None)

    assert datasource_config["spark_config"] == expected_spark_config
    assert datasource_config["force_reuse_spark_context"] is False

    dataset_name = "test_spark_dataset_2"
    data_context_parameterized_expectation_suite.add_datasource(
        dataset_name,
        class_name="SparkDFDatasource",
        spark_config={},
        force_reuse_spark_context=True,
        module_name="great_expectations.datasource",
        batch_kwargs_generators={},
    )
    datasource_config = data_context_parameterized_expectation_suite.get_datasource(
        dataset_name
    ).config
    assert datasource_config["spark_config"] == {}
    assert datasource_config["force_reuse_spark_context"] == True  # noqa: E712


def test_create_sparkdf_datasource(
    data_context_parameterized_expectation_suite, tmp_path_factory, test_backends
):
    if "SparkDFDataset" not in test_backends:
        pytest.skip("Spark has not been enabled, so this test must be skipped.")
    base_dir = tmp_path_factory.mktemp("test_create_sparkdf_datasource")
    name = "test_sparkdf_datasource"
    # type_ = "spark"
    class_name = "SparkDFDatasource"

    data_context_parameterized_expectation_suite.add_datasource(
        name,
        class_name=class_name,
        batch_kwargs_generators={
            "default": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": str(base_dir),
            }
        },
    )
    data_context_config = data_context_parameterized_expectation_suite.get_config()

    assert name in data_context_config["datasources"]
    assert data_context_config["datasources"][name]["class_name"] == class_name
    assert data_context_config["datasources"][name]["batch_kwargs_generators"][
        "default"
    ]["base_directory"] == str(base_dir)

    base_dir = tmp_path_factory.mktemp("test_create_sparkdf_datasource-2")
    name = "test_sparkdf_datasource"

    data_context_parameterized_expectation_suite.add_datasource(
        name,
        class_name=class_name,
        batch_kwargs_generators={
            "default": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "reader_options": {"sep": "|", "header": False},
            }
        },
    )
    data_context_config = data_context_parameterized_expectation_suite.get_config()

    assert name in data_context_config["datasources"]
    assert data_context_config["datasources"][name]["class_name"] == class_name
    assert (
        data_context_config["datasources"][name]["batch_kwargs_generators"]["default"][
            "reader_options"
        ]["sep"]
        == "|"
    )

    # Note that pipe is special in yml, so let's also check to see that it was properly serialized
    with open(
        os.path.join(  # noqa: PTH118
            data_context_parameterized_expectation_suite.root_directory,
            "great_expectations.yml",
        ),
    ) as configfile:
        lines = configfile.readlines()
        assert "          sep: '|'\n" in lines
        assert "          header: false\n" in lines


@pytest.mark.skipif(
    not is_library_loadable(library_name="pyarrow")
    and not is_library_loadable(library_name="fastparquet"),
    reason="pyarrow and fastparquet are not installed",
)
def test_standalone_spark_parquet_datasource(
    test_parquet_folder_connection_path, spark_session
):
    assert spark_session  # Ensure a sparksession exists
    datasource = SparkDFDatasource(
        "SparkParquet",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": test_parquet_folder_connection_path,
            }
        },
    )

    assert datasource.get_available_data_asset_names()["subdir_reader"]["names"] == [
        ("test", "file")
    ]
    batch = datasource.get_batch(
        batch_kwargs={
            "path": os.path.join(  # noqa: PTH118
                test_parquet_folder_connection_path, "test.parquet"
            )
        }
    )
    assert isinstance(batch, Batch)
    # NOTE: below is a great example of CSV vs. Parquet typing: pandas reads content as string, spark as int
    assert batch.data.head()["col_1"] == 1
    assert batch.data.count() == 5

    # Limit should also work
    batch = datasource.get_batch(
        batch_kwargs={
            "path": os.path.join(  # noqa: PTH118
                test_parquet_folder_connection_path, "test.parquet"
            ),
            "limit": 2,
        }
    )
    assert isinstance(batch, Batch)
    # NOTE: below is a great example of CSV vs. Parquet typing: pandas reads content as string, spark as int
    assert batch.data.head()["col_1"] == 1
    assert batch.data.count() == 2


def test_standalone_spark_csv_datasource(
    test_folder_connection_path_csv, test_backends
):
    if "SparkDFDataset" not in test_backends:
        pytest.skip("Spark has not been enabled, so this test must be skipped.")
    datasource = SparkDFDatasource(
        "SparkParquet",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": test_folder_connection_path_csv,
            }
        },
    )

    assert datasource.get_available_data_asset_names()["subdir_reader"]["names"] == [
        ("test", "file")
    ]
    batch = datasource.get_batch(
        batch_kwargs={
            "path": os.path.join(  # noqa: PTH118
                test_folder_connection_path_csv, "test.csv"
            ),
            "reader_options": {"header": True},
        }
    )
    assert isinstance(batch, Batch)
    # NOTE: below is a great example of CSV vs. Parquet typing: pandas reads content as string, spark as int
    assert batch.data.head()["col_1"] == "1"


def test_invalid_reader_sparkdf_datasource(tmp_path_factory, test_backends):
    if "SparkDFDataset" not in test_backends:
        pytest.skip("Spark has not been enabled, so this test must be skipped.")
    basepath = str(tmp_path_factory.mktemp("test_invalid_reader_sparkdf_datasource"))
    datasource = SparkDFDatasource(
        "mysparksource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": basepath,
            }
        },
    )

    with open(
        os.path.join(  # noqa: PTH118
            basepath, "idonotlooklikeacsvbutiam.notrecognized"
        ),
        "w",
    ) as newfile:
        newfile.write("a,b\n1,2\n3,4\n")

    with pytest.raises(BatchKwargsError) as exc:
        datasource.get_batch(
            batch_kwargs={
                "path": os.path.join(  # noqa: PTH118
                    basepath, "idonotlooklikeacsvbutiam.notrecognized"
                )
            }
        )
        assert "Unable to determine reader for path" in exc.value.message

    with pytest.raises(BatchKwargsError) as exc:
        datasource.get_batch(
            batch_kwargs={
                "path": os.path.join(  # noqa: PTH118
                    basepath, "idonotlooklikeacsvbutiam.notrecognized"
                ),
                "reader_method": "blarg",
            }
        )
        assert "Unknown reader method: blarg" in exc.value.message

    with pytest.raises(BatchKwargsError) as exc:
        datasource.get_batch(
            batch_kwargs={
                "path": os.path.join(  # noqa: PTH118
                    basepath, "idonotlooklikeacsvbutiam.notrecognized"
                ),
                "reader_method": "excel",
            }
        )
        assert "Unknown reader: excel" in exc.value.message

    batch = datasource.get_batch(
        batch_kwargs={
            "path": os.path.join(  # noqa: PTH118
                basepath, "idonotlooklikeacsvbutiam.notrecognized"
            ),
            "reader_method": "csv",
            "reader_options": {"header": True},
        }
    )
    assert batch.data.head()["a"] == "1"


def test_spark_datasource_processes_dataset_options(
    test_folder_connection_path_csv, test_backends, empty_data_context
):
    context: DataContext = empty_data_context
    if "SparkDFDataset" not in test_backends:
        pytest.skip("Spark has not been enabled, so this test must be skipped.")
    datasource = SparkDFDatasource(
        "PandasCSV",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": test_folder_connection_path_csv,
            }
        },
    )
    batch_kwargs = datasource.build_batch_kwargs(
        "subdir_reader", data_asset_name="test"
    )
    batch_kwargs["dataset_options"] = {"caching": False, "persist": False}
    batch = datasource.get_batch(batch_kwargs)
    validator = BridgeValidator(
        batch, ExpectationSuite(expectation_suite_name="foo", data_context=context)
    )
    dataset = validator.get_dataset()
    assert dataset.caching is False
    assert dataset._persist is False
