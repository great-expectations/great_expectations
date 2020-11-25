import os
import re

import pandas as pd
import pytest
from ruamel.yaml import YAML

from great_expectations.core.batch import Batch
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.dataset import SparkDFDataset
from great_expectations.datasource import SparkDFDatasource
from great_expectations.datasource.types import InMemoryBatchKwargs
from great_expectations.exceptions import BatchKwargsError
from great_expectations.util import is_library_loadable
from great_expectations.validator.validator import BridgeValidator, Validator

yaml = YAML()


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
    df1.to_parquet(os.path.join(basepath, "test.parquet"))

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
        os.path.join(
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
    data_context_parameterized_expectation_suite.create_expectation_suite(
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
        os.path.join(
            data_context_parameterized_expectation_suite.root_directory,
            "great_expectations.yml",
        ),
    ) as configfile:
        lines = configfile.readlines()
        assert "          sep: '|'\n" in lines
        assert "          header: false\n" in lines


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
            "path": os.path.join(test_parquet_folder_connection_path, "test.parquet")
        }
    )
    assert isinstance(batch, Batch)
    # NOTE: below is a great example of CSV vs. Parquet typing: pandas reads content as string, spark as int
    assert batch.data.head()["col_1"] == 1
    assert batch.data.count() == 5

    # Limit should also work
    batch = datasource.get_batch(
        batch_kwargs={
            "path": os.path.join(test_parquet_folder_connection_path, "test.parquet"),
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
            "path": os.path.join(test_folder_connection_path_csv, "test.csv"),
            "reader_options": {"header": True},
        }
    )
    assert isinstance(batch, Batch)
    # NOTE: below is a great example of CSV vs. Parquet typing: pandas reads content as string, spark as int
    assert batch.data.head()["col_1"] == "1"


def test_standalone_spark_passthrough_datasource(
    data_context_parameterized_expectation_suite, dataset, test_backends
):
    if "SparkDFDataset" not in test_backends:
        pytest.skip("Spark has not been enabled, so this test must be skipped.")
    datasource = data_context_parameterized_expectation_suite.add_datasource(
        "spark_source",
        module_name="great_expectations.datasource",
        class_name="SparkDFDatasource",
    )

    # We want to ensure that an externally-created spark DataFrame can be successfully instantiated using the
    # datasource built in a data context
    # Our dataset fixture is parameterized by all backends. The spark source should only accept a spark dataset
    data_context_parameterized_expectation_suite.create_expectation_suite("new_suite")
    batch_kwargs = InMemoryBatchKwargs(datasource="spark_source", dataset=dataset)

    if isinstance(dataset, SparkDFDataset):
        # We should be smart enough to figure out this is a batch:
        batch = data_context_parameterized_expectation_suite.get_batch(
            batch_kwargs=batch_kwargs, expectation_suite_name="new_suite"
        )
        res = batch.expect_column_to_exist("infinities")
        assert res.success is True
        res = batch.expect_column_to_exist("not_a_column")
        assert res.success is False
        batch.save_expectation_suite()
        assert os.path.isfile(
            os.path.join(
                data_context_parameterized_expectation_suite.root_directory,
                "expectations/new_suite.json",
            )
        )

    else:
        with pytest.raises(BatchKwargsError) as exc:
            # noinspection PyUnusedLocal
            batch = data_context_parameterized_expectation_suite.get_batch(
                batch_kwargs=batch_kwargs, expectation_suite_name="new_suite"
            )
            assert "Unrecognized batch_kwargs for spark_source" in exc.value.message


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
        os.path.join(basepath, "idonotlooklikeacsvbutiam.notrecognized"), "w"
    ) as newfile:
        newfile.write("a,b\n1,2\n3,4\n")

    with pytest.raises(BatchKwargsError) as exc:
        datasource.get_batch(
            batch_kwargs={
                "path": os.path.join(basepath, "idonotlooklikeacsvbutiam.notrecognized")
            }
        )
        assert "Unable to determine reader for path" in exc.value.message

    with pytest.raises(BatchKwargsError) as exc:
        datasource.get_batch(
            batch_kwargs={
                "path": os.path.join(
                    basepath, "idonotlooklikeacsvbutiam.notrecognized"
                ),
                "reader_method": "blarg",
            }
        )
        assert "Unknown reader method: blarg" in exc.value.message

    with pytest.raises(BatchKwargsError) as exc:
        datasource.get_batch(
            batch_kwargs={
                "path": os.path.join(
                    basepath, "idonotlooklikeacsvbutiam.notrecognized"
                ),
                "reader_method": "excel",
            }
        )
        assert "Unknown reader: excel" in exc.value.message

    batch = datasource.get_batch(
        batch_kwargs={
            "path": os.path.join(basepath, "idonotlooklikeacsvbutiam.notrecognized"),
            "reader_method": "csv",
            "reader_options": {"header": True},
        }
    )
    assert batch.data.head()["a"] == "1"


@pytest.mark.skipif(
    is_library_loadable(library_name="pyspark"),
    reason="Spark 3.0.0 creates one JVM per session, makikng configuration immutable.  A future PR handles this better.",
)
def test_spark_config(test_backends):
    if "SparkDFDataset" not in test_backends:
        pytest.skip("Spark has not been enabled, so this test must be skipped.")
    source = SparkDFDatasource()
    conf = source.spark.sparkContext.getConf().getAll()
    # Without specifying any spark_config values we get defaults
    assert ("spark.app.name", "pyspark-shell") in conf

    source = SparkDFDatasource(
        spark_config={
            "spark.app.name": "great_expectations",
            "spark.sql.catalogImplementation": "hive",
            "spark.executor.memory": "128m",
        }
    )

    # Test that our values were set
    conf = source.spark.sparkContext.getConf().getAll()
    assert ("spark.app.name", "great_expectations") in conf
    assert ("spark.sql.catalogImplementation", "hive") in conf
    assert ("spark.executor.memory", "128m") in conf


def test_spark_datasource_processes_dataset_options(
    test_folder_connection_path_csv, test_backends
):
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
    validator = BridgeValidator(batch, ExpectationSuite(expectation_suite_name="foo"))
    dataset = validator.get_dataset()
    assert dataset.caching is False
    assert dataset._persist is False
