# -*- coding: utf-8 -*-

import pytest
from six import PY3

from ruamel.yaml import YAML
import os
import shutil

import pandas as pd
import sqlalchemy as sa

from great_expectations.exceptions import BatchKwargsError
from great_expectations.datasource import PandasDatasource, SqlAlchemyDatasource, SparkDFDatasource
from great_expectations.dataset import PandasDataset, SqlAlchemyDataset, SparkDFDataset

yaml = YAML(typ='safe')


@pytest.fixture(scope="module")
def test_folder_connection_path(tmp_path_factory):
    df1 = pd.DataFrame(
        {'col_1': [1, 2, 3, 4, 5], 'col_2': ['a', 'b', 'c', 'd', 'e']})
    path = str(tmp_path_factory.mktemp("test_folder_connection_path"))
    df1.to_csv(os.path.join(path, "test.csv"))

    return str(path)


@pytest.fixture(scope="module")
def test_db_connection_string(tmp_path_factory):
    df1 = pd.DataFrame(
        {'col_1': [1, 2, 3, 4, 5], 'col_2': ['a', 'b', 'c', 'd', 'e']})
    df2 = pd.DataFrame(
        {'col_1': [0, 1, 2, 3, 4], 'col_2': ['b', 'c', 'd', 'e', 'f']})

    basepath = str(tmp_path_factory.mktemp("db_context"))
    path = os.path.join(basepath, "test.db")
    engine = sa.create_engine('sqlite:///' + str(path))
    df1.to_sql('table_1', con=engine, index=True)
    df2.to_sql('table_2', con=engine, index=True, schema='main')

    # Return a connection string to this newly-created db
    return 'sqlite:///' + str(path)


@pytest.fixture(scope="module")
def test_parquet_folder_connection_path(tmp_path_factory):
    df1 = pd.DataFrame(
        {'col_1': [1, 2, 3, 4, 5], 'col_2': ['a', 'b', 'c', 'd', 'e']})
    basepath = str(tmp_path_factory.mktemp("parquet_context"))
    df1.to_parquet(os.path.join(basepath, "test.parquet"))

    return basepath


def test_create_pandas_datasource(data_context, tmp_path_factory):
    basedir = tmp_path_factory.mktemp('test_create_pandas_datasource')
    name = "test_pandas_datasource"
    type_ = "pandas"

    data_context.add_datasource(name, type_, base_directory=str(basedir))
    data_context_config = data_context.get_config()

    assert name in data_context_config["datasources"] 
    assert data_context_config["datasources"][name]["type"] == type_

    # We should now see updated configs
    # Finally, we should be able to confirm that the folder structure is as expected
    with open(os.path.join(data_context.root_directory, "great_expectations.yml"), "r") as data_context_config_file:
        data_context_file_config = yaml.load(data_context_config_file)

    assert data_context_file_config["datasources"][name] == data_context_config["datasources"][name]


def test_standalone_pandas_datasource(test_folder_connection_path):
    datasource = PandasDatasource('PandasCSV', base_directory=test_folder_connection_path)

    assert datasource.get_available_data_asset_names() == {"default": {"test"}}
    manual_batch_kwargs = datasource.build_batch_kwargs(os.path.join(str(test_folder_connection_path), "test.csv"))

    # Get the default (subdir_path) generator
    generator = datasource.get_generator()
    auto_batch_kwargs = generator.yield_batch_kwargs("test")

    assert manual_batch_kwargs["path"] == auto_batch_kwargs["path"]

    # Include some extra kwargs...
    dataset = datasource.get_batch("test", batch_kwargs=auto_batch_kwargs, sep=",", header=0, index_col=0)
    assert isinstance(dataset, PandasDataset)
    assert (dataset["col_1"] == [1, 2, 3, 4, 5]).all()


def test_standalone_sqlalchemy_datasource(test_db_connection_string):
    datasource = SqlAlchemyDatasource(
        'SqlAlchemy', connection_string=test_db_connection_string, echo=False)

    assert datasource.get_available_data_asset_names() == {"default": {"main.table_1", "main.table_2"}}
    dataset1 = datasource.get_batch("main.table_1")
    dataset2 = datasource.get_batch("main.table_2")
    assert isinstance(dataset1, SqlAlchemyDataset)
    assert isinstance(dataset2, SqlAlchemyDataset)


def test_create_sqlalchemy_datasource(data_context):
    name = "test_sqlalchemy_datasource"
    type_ = "sqlalchemy"

    # Use sqlite so we don't require postgres for this test.
    connection_kwargs = {
        "drivername": "sqlite"
    }
    # connection_kwargs = {
    #     "drivername": "postgresql",
    #     "username": "postgres",
    #     "password": "",
    #     "host": "localhost",
    #     "port": 5432,
    #     "database": "test_ci",
    # }

    # It should be possible to create a sqlalchemy source using these params without
    # saving a profile
    data_context.add_datasource(name, type_, **connection_kwargs)
    data_context_config = data_context.get_config()
    assert name in data_context_config["datasources"]
    assert data_context_config["datasources"][name]["type"] == type_

    # We should be able to get it in this session even without saving the config
    source = data_context.get_datasource(name)
    assert isinstance(source, SqlAlchemyDatasource)

    profile_name = "test_sqlalchemy_datasource"
    data_context.add_profile_credentials(profile_name, **connection_kwargs)

    # But we should be able to add a source using a profile
    name = "second_source"
    data_context.add_datasource(name, type_, profile="test_sqlalchemy_datasource")

    data_context_config = data_context.get_config()
    assert name in data_context_config["datasources"]
    assert data_context_config["datasources"][name]["type"] == type_
    assert data_context_config["datasources"][name]["profile"] == profile_name

    source = data_context.get_datasource(name)
    assert isinstance(source, SqlAlchemyDatasource)

    # Finally, we should be able to confirm that the folder structure is as expected
    with open(os.path.join(data_context.root_directory, "uncommitted/credentials/profiles.yml"), "r") as profiles_file:
        profiles = yaml.load(profiles_file)

    assert profiles == {
        profile_name: dict(**connection_kwargs)
    }


def test_create_sparkdf_datasource(data_context, tmp_path_factory):
    pyspark_skip = pytest.importorskip("pyspark")
    base_dir = tmp_path_factory.mktemp('test_create_sparkdf_datasource')
    name = "test_sparkdf_datasource"
    type_ = "spark"

    data_context.add_datasource(name, type_, base_directory=str(base_dir))
    data_context_config = data_context.get_config()

    assert name in data_context_config["datasources"] 
    assert data_context_config["datasources"][name]["type"] == type_
    assert data_context_config["datasources"][name]["generators"]["default"]["base_directory"] == str(base_dir)

    base_dir = tmp_path_factory.mktemp('test_create_sparkdf_datasource-2')
    name = "test_sparkdf_datasource"
    type_ = "spark"

    data_context.add_datasource(name, type_, reader_options={"sep": "|", "header": False})
    data_context_config = data_context.get_config()

    assert name in data_context_config["datasources"] 
    assert data_context_config["datasources"][name]["type"] == type_ 
    assert data_context_config["datasources"][name]["generators"]["default"]["reader_options"]["sep"] == "|"

    # Note that pipe is special in yml, so let's also check to see that it was properly serialized
    with open(os.path.join(data_context.root_directory, "great_expectations.yml"), "r") as configfile:
        lines = configfile.readlines()
        assert "          sep: '|'\n" in lines
        assert "          header: false\n" in lines


def test_sqlalchemy_source_templating(sqlitedb_engine):
    datasource = SqlAlchemyDatasource(engine=sqlitedb_engine)
    generator = datasource.get_generator()
    generator.add_query("test", "select 'cat' as ${col_name};")
    df = datasource.get_batch("test", col_name="animal_name")
    res = df.expect_column_to_exist("animal_name")
    assert res["success"] == True


def test_pandas_source_readcsv(data_context, tmp_path_factory):
    if not PY3:
        # We don't specifically test py2 unicode reading since this test is about our handling of kwargs *to* read_csv
        pytest.skip()
    basedir = tmp_path_factory.mktemp('test_create_pandas_datasource')
    shutil.copy("./tests/test_sets/unicode.csv", basedir)
    data_context.add_datasource(name="mysource", type_="pandas", reader_options={"encoding": "utf-8"}, base_directory=str(basedir))

    batch = data_context.get_batch("mysource/unicode")
    assert len(batch["Μ"] == 1)
    assert "😁" in list(batch["Μ"])

    data_context.add_datasource(name="mysource2", type_="pandas", base_directory=str(basedir))
    batch = data_context.get_batch("mysource2/unicode")
    assert "😁" in list(batch["Μ"])

    data_context.add_datasource(name="mysource3", type_="pandas", reader_options={"encoding": "utf-16"}, base_directory=str(basedir))
    with pytest.raises(UnicodeError, match="UTF-16 stream does not start with BOM"):
        batch = data_context.get_batch("mysource3/unicode")

    with pytest.raises(LookupError, match="unknown encoding: blarg"):
        batch = data_context.get_batch("mysource/unicode", encoding='blarg')

    batch = data_context.get_batch("mysource2/unicode", encoding='utf-8')
    assert "😁" in list(batch["Μ"])


def test_standalone_spark_parquet_datasource(test_parquet_folder_connection_path):
    pyspark_skip = pytest.importorskip("pyspark")
    datasource = SparkDFDatasource('SparkParquet', base_directory=test_parquet_folder_connection_path)

    assert datasource.get_available_data_asset_names() == {
        "default": set(['test'])
    }
    dataset = datasource.get_batch('test')
    assert isinstance(dataset, SparkDFDataset)
    # NOTE: below is a great example of CSV vs. Parquet typing: pandas reads content as string, spark as int
    assert dataset.spark_df.head()['col_1'] == 1


def test_standalone_spark_csv_datasource(test_folder_connection_path):
    pyspark_skip = pytest.importorskip("pyspark")
    datasource = SparkDFDatasource('SparkParquet', base_directory=test_folder_connection_path)
    assert datasource.get_available_data_asset_names() == {
        "default": set(['test'])
    }
    dataset = datasource.get_batch('test', header=True)
    assert isinstance(dataset, SparkDFDataset)
    # NOTE: below is a great example of CSV vs. Parquet typing: pandas reads content as string, spark as int
    assert dataset.spark_df.head()['col_1'] == '1'


def test_standalone_spark_passthrough_generator_datasource(data_context, dataset):
    pyspark_skip = pytest.importorskip("pyspark")
    # noinspection PyUnusedLocal
    datasource = data_context.add_datasource("spark_source", "spark", generators={"passthrough": {"type": "memory"}})

    # We want to ensure that an externally-created spark DataFrame can be successfully instantiated using the
    # datasource built in a data context
    # Our dataset fixture is parameterized by all backends. The spark source should only accept a spark dataset

    if isinstance(dataset, SparkDFDataset):
        # We should be smart enough to figure out this is a batch:
        batch = data_context.get_batch("spark_source/passthrough/new_asset", "new_suite", dataset)
        res = batch.expect_column_to_exist("infinities")
        assert res["success"] is True
        res = batch.expect_column_to_exist("not_a_column")
        assert res["success"] is False
        batch.save_expectation_suite()
        assert os.path.isfile(os.path.join(
            data_context.root_directory,
            "expectations/spark_source/passthrough/new_asset/new_suite.json")
        )

    else:
        with pytest.raises(BatchKwargsError) as exc:
            # noinspection PyUnusedLocal
            batch = data_context.get_batch("spark_source/passthrough/new_asset", "new_suite", dataset)
            assert "Unrecognized batch_kwargs for spark_source" in exc.message


def test_invalid_reader_sparkdf_datasource(tmp_path_factory):
    pyspark_skip = pytest.importorskip("pyspark")
    basepath = str(tmp_path_factory.mktemp("test_invalid_reader_sparkdf_datasource"))
    datasource = SparkDFDatasource('mysparksource', base_directory=basepath)

    with open(os.path.join(basepath, "idonotlooklikeacsvbutiam.notrecognized"), "w") as newfile:
        newfile.write("a,b\n1,2\n3,4\n")

    with pytest.raises(BatchKwargsError) as exc:
        datasource.get_batch("idonotlooklikeacsvbutiam.notrecognized", expectation_suite_name="default", batch_kwargs={
            "path": os.path.join(basepath, "idonotlooklikeacsvbutiam.notrecognized")
        })
        assert "Unable to determine reader for path" in exc.message

    with pytest.raises(BatchKwargsError) as exc:
        datasource.get_batch("idonotlooklikeacsvbutiam.notrecognized", expectation_suite_name="default", batch_kwargs={
            "path": os.path.join(basepath, "idonotlooklikeacsvbutiam.notrecognized")
        }, reader_method="blarg")
        assert "Unknown reader method: blarg" in exc.message

    with pytest.raises(BatchKwargsError) as exc:
        datasource.get_batch("idonotlooklikeacsvbutiam.notrecognized", expectation_suite_name="default", batch_kwargs={
            "path": os.path.join(basepath, "idonotlooklikeacsvbutiam.notrecognized")
        }, reader_method="excel")
        assert "Unsupported reader: excel" in exc.message

    dataset = datasource.get_batch("idonotlooklikeacsvbutiam.notrecognized", expectation_suite_name="default", batch_kwargs={
            "path": os.path.join(basepath, "idonotlooklikeacsvbutiam.notrecognized")
        },
        reader_method="csv", header=True)
    assert dataset.spark_df.head()["a"] == "1"


def test_invalid_reader_pandas_datasource(tmp_path_factory):
    basepath = str(tmp_path_factory.mktemp("test_invalid_reader_pandas_datasource"))
    datasource = PandasDatasource('mypandassource', base_directory=basepath)

    with open(os.path.join(basepath, "idonotlooklikeacsvbutiam.notrecognized"), "w") as newfile:
        newfile.write("a,b\n1,2\n3,4\n")

    with pytest.raises(BatchKwargsError) as exc:
        datasource.get_batch("idonotlooklikeacsvbutiam.notrecognized", expectation_suite_name="default", batch_kwargs={
            "path": os.path.join(basepath, "idonotlooklikeacsvbutiam.notrecognized")
        })
        assert "Unable to determine reader for path" in exc.message

    with pytest.raises(BatchKwargsError) as exc:
        datasource.get_batch("idonotlooklikeacsvbutiam.notrecognized", expectation_suite_name="default", batch_kwargs={
            "path": os.path.join(basepath, "idonotlooklikeacsvbutiam.notrecognized")
        }, reader_method="blarg")
        assert "Unknown reader method: blarg" in exc.message

    dataset = datasource.get_batch("idonotlooklikeacsvbutiam.notrecognized", expectation_suite_name="default", batch_kwargs={
            "path": os.path.join(basepath, "idonotlooklikeacsvbutiam.notrecognized")
        }, reader_method="csv", header=0)
    assert dataset["a"][0] == 1