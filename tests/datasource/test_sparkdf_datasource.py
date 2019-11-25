# -*- coding: utf-8 -*-

import pytest

from ruamel.yaml import YAML
import os

import pandas as pd

from great_expectations.exceptions import BatchKwargsError
from great_expectations.datasource import SparkDFDatasource
from great_expectations.dataset import SparkDFDataset
from great_expectations.datasource.types import InMemoryBatchKwargs

yaml = YAML()


@pytest.fixture(scope="module")
def test_folder_connection_path(tmp_path_factory):
    df1 = pd.DataFrame(
        {'col_1': [1, 2, 3, 4, 5], 'col_2': ['a', 'b', 'c', 'd', 'e']})
    path = str(tmp_path_factory.mktemp("test_folder_connection_path"))
    df1.to_csv(os.path.join(path, "test.csv"))

    return str(path)


@pytest.fixture(scope="module")
def test_parquet_folder_connection_path(tmp_path_factory):
    df1 = pd.DataFrame(
        {'col_1': [1, 2, 3, 4, 5], 'col_2': ['a', 'b', 'c', 'd', 'e']})
    basepath = str(tmp_path_factory.mktemp("parquet_context"))
    df1.to_parquet(os.path.join(basepath, "test.parquet"))

    return basepath


def test_sparkdf_datasource_custom_data_asset(data_context, test_folder_connection_path, spark_session):
    assert spark_session  # Ensure a spark session exists
    name = "test_sparkdf_datasource"
    # type_ = "spark"
    class_name = "SparkDFDatasource"

    data_asset_type_config = {
        "module_name": "custom_sparkdf_dataset",
        "class_name": "CustomSparkDFDataset"
    }
    data_context.add_datasource(name,
                                class_name=class_name,
                                base_directory=test_folder_connection_path,
                                data_asset_type=data_asset_type_config)

    # We should now see updated configs
    with open(os.path.join(data_context.root_directory, "great_expectations.yml"), "r") as data_context_config_file:
        data_context_file_config = yaml.load(data_context_config_file)

    assert data_context_file_config["datasources"][name]["data_asset_type"]["module_name"] == "custom_sparkdf_dataset"
    assert data_context_file_config["datasources"][name]["data_asset_type"]["class_name"] == "CustomSparkDFDataset"

    # We should be able to get a dataset of the correct type from the datasource.
    data_context.create_expectation_suite("test_sparkdf_datasource/default/test", "default")
    batch_kwargs = data_context.yield_batch_kwargs("test_sparkdf_datasource/default/test")
    batch = data_context.get_batch(
        "test_sparkdf_datasource/default/test",
        expectation_suite_name="default",
        batch_kwargs=batch_kwargs,
        reader_options={
            'header': True,
            'inferSchema': True
        }
    )
    assert type(batch).__name__ == "CustomSparkDFDataset"
    res = batch.expect_column_approx_quantile_values_to_be_between("col_1", quantile_ranges={
        "quantiles": [0., 1.],
        "value_ranges": [[1, 1], [5, 5]]
    })
    assert res.success is True


def test_create_sparkdf_datasource(data_context, tmp_path_factory):
    pyspark_skip = pytest.importorskip("pyspark")
    base_dir = tmp_path_factory.mktemp('test_create_sparkdf_datasource')
    name = "test_sparkdf_datasource"
    # type_ = "spark"
    class_name = "SparkDFDatasource"

    data_context.add_datasource(name, class_name=class_name,
                                generators={
                                    "default": {
                                        "class_name": "SubdirReaderGenerator",
                                        "base_directory": str(base_dir)
                                    }
                                }
                                )
    data_context_config = data_context.get_config()

    assert name in data_context_config["datasources"] 
    assert data_context_config["datasources"][name]["class_name"] == class_name
    assert data_context_config["datasources"][name]["generators"]["default"]["base_directory"] == str(base_dir)

    base_dir = tmp_path_factory.mktemp('test_create_sparkdf_datasource-2')
    name = "test_sparkdf_datasource"

    data_context.add_datasource(name,
                                class_name=class_name,
                                generators={
                                    "default": {
                                        "class_name": "SubdirReaderGenerator",
                                        "reader_options":
                                            {
                                                "sep": "|",
                                                "header": False
                                            }
                                    }
                                    }
                                )
    data_context_config = data_context.get_config()

    assert name in data_context_config["datasources"] 
    assert data_context_config["datasources"][name]["class_name"] == class_name
    assert data_context_config["datasources"][name]["generators"]["default"]["reader_options"]["sep"] == "|"

    # Note that pipe is special in yml, so let's also check to see that it was properly serialized
    with open(os.path.join(data_context.root_directory, "great_expectations.yml"), "r") as configfile:
        lines = configfile.readlines()
        assert "          sep: '|'\n" in lines
        assert "          header: false\n" in lines


def test_standalone_spark_parquet_datasource(test_parquet_folder_connection_path, spark_session):
    assert spark_session  # Ensure a sparksession exists
    datasource = SparkDFDatasource('SparkParquet', base_directory=test_parquet_folder_connection_path)

    assert datasource.get_available_data_asset_names() == {
        "default": ['test']
    }
    dataset = datasource.get_batch('test',
                                   expectation_suite_name="default",
                                   batch_kwargs={
                                       "path": os.path.join(test_parquet_folder_connection_path,
                                                            'test.parquet')
                                   })
    assert isinstance(dataset, SparkDFDataset)
    # NOTE: below is a great example of CSV vs. Parquet typing: pandas reads content as string, spark as int
    assert dataset.spark_df.head()['col_1'] == 1
    assert dataset.spark_df.count() == 5

    # Limit should also work
    dataset = datasource.get_batch('test',
                                   expectation_suite_name="default",
                                   batch_kwargs={
                                       "path": os.path.join(test_parquet_folder_connection_path,
                                                            'test.parquet'),
                                       "limit": 2
                                   })
    assert isinstance(dataset, SparkDFDataset)
    # NOTE: below is a great example of CSV vs. Parquet typing: pandas reads content as string, spark as int
    assert dataset.spark_df.head()['col_1'] == 1
    assert dataset.spark_df.count() == 2


def test_standalone_spark_csv_datasource(test_folder_connection_path):
    pyspark_skip = pytest.importorskip("pyspark")
    datasource = SparkDFDatasource('SparkParquet', base_directory=test_folder_connection_path)
    assert datasource.get_available_data_asset_names() == {
        "default": ['test']
    }
    dataset = datasource.get_batch('test',
                                   expectation_suite_name="default",
                                   batch_kwargs={
                                       "path": os.path.join(test_folder_connection_path,
                                                            'test.csv')
                                   },
                                   reader_options={"header": True})
    assert isinstance(dataset, SparkDFDataset)
    # NOTE: below is a great example of CSV vs. Parquet typing: pandas reads content as string, spark as int
    assert dataset.spark_df.head()['col_1'] == '1'


def test_standalone_spark_passthrough_generator_datasource(data_context, dataset):
    pyspark_skip = pytest.importorskip("pyspark")
    datasource = data_context.add_datasource("spark_source",
                                             module_name="great_expectations.datasource",
                                             class_name="SparkDFDatasource",
                                             generators={"passthrough": {"class_name": "InMemoryGenerator"}})

    # We want to ensure that an externally-created spark DataFrame can be successfully instantiated using the
    # datasource built in a data context
    # Our dataset fixture is parameterized by all backends. The spark source should only accept a spark dataset
    data_context.create_expectation_suite("spark_source/passthrough/new_asset", "new_suite")
    batch_kwargs = InMemoryBatchKwargs(dataset=dataset)

    if isinstance(dataset, SparkDFDataset):
        # We should be smart enough to figure out this is a batch:
        batch = data_context.get_batch("spark_source/passthrough/new_asset", "new_suite", batch_kwargs)
        res = batch.expect_column_to_exist("infinities")
        assert res.success is True
        res = batch.expect_column_to_exist("not_a_column")
        assert res.success is False
        batch.save_expectation_suite()
        assert os.path.isfile(os.path.join(
            data_context.root_directory,
            "expectations/spark_source/passthrough/new_asset/new_suite.json")
        )

    else:
        with pytest.raises(BatchKwargsError) as exc:
            # noinspection PyUnusedLocal
            batch = data_context.get_batch("spark_source/passthrough/new_asset", "new_suite", batch_kwargs)
            assert "Unrecognized batch_kwargs for spark_source" in exc.value.message


def test_invalid_reader_sparkdf_datasource(tmp_path_factory):
    pyspark_skip = pytest.importorskip("pyspark")
    basepath = str(tmp_path_factory.mktemp("test_invalid_reader_sparkdf_datasource"))
    datasource = SparkDFDatasource('mysparksource', base_directory=basepath)

    with open(os.path.join(basepath, "idonotlooklikeacsvbutiam.notrecognized"), "w") as newfile:
        newfile.write("a,b\n1,2\n3,4\n")

    with pytest.raises(BatchKwargsError) as exc:
        datasource.get_data_asset("idonotlooklikeacsvbutiam.notrecognized", batch_kwargs={
            "path": os.path.join(basepath, "idonotlooklikeacsvbutiam.notrecognized")
        })
        assert "Unable to determine reader for path" in exc.value.message

    with pytest.raises(BatchKwargsError) as exc:
        datasource.get_data_asset("idonotlooklikeacsvbutiam.notrecognized", batch_kwargs={
            "path": os.path.join(basepath, "idonotlooklikeacsvbutiam.notrecognized")
        }, reader_method="blarg")
        assert "Unknown reader method: blarg" in exc.value.message

    with pytest.raises(BatchKwargsError) as exc:
        datasource.get_data_asset("idonotlooklikeacsvbutiam.notrecognized", batch_kwargs={
            "path": os.path.join(basepath, "idonotlooklikeacsvbutiam.notrecognized")
        }, reader_method="excel")
        assert "Unsupported reader: excel" in exc.value.message

    dataset = datasource.get_data_asset("idonotlooklikeacsvbutiam.notrecognized", batch_kwargs={
            "path": os.path.join(basepath, "idonotlooklikeacsvbutiam.notrecognized"),
        },
        reader_method="csv", reader_options={'header': True})
    assert dataset.spark_df.head()["a"] == "1"
