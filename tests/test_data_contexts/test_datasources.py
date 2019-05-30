import pytest

import os
import shutil
import yaml

from great_expectations.data_context import DataContext
from great_expectations.data_context.datasource.sqlalchemy_source import SqlAlchemyDatasource


@pytest.fixture()
def data_context(tmpdir_factory):
    # TODO: harmonize with Eugene's approach to testing data context so we have a single fixture
    context_path = tmpdir_factory.mktemp("empty_context_dir")
    asset_config_path = os.path.join(context_path, "great_expectations/expectations")
    os.makedirs(asset_config_path, exist_ok=True)
    shutil.copy("./tests/test_fixtures/great_expectations.yml", str(context_path))
    shutil.copy("./tests/test_fixtures/expectations/parameterized_expectations_config_fixture.json",str(asset_config_path))
    return DataContext(context_path)


def test_create_pandas_datasource(data_context, tmpdir):
    name = "test_pandas_datasource"
    type_ = "pandas"

    data_context.add_datasource(name, type_, base_directory=str(tmpdir))
    data_context_config = data_context.get_config()

    assert name in data_context_config["datasources"] 
    assert data_context_config["datasources"][name]["type"] == type_

    # We should now see updated configs
    # Finally, we should be able to confirm that the folder structure is as expected
    with open(os.path.join(data_context.context_root_directory, "great_expectations.yml"), "r") as data_context_config_file:
        data_context_file_config = yaml.safe_load(data_context_config_file)

    assert data_context_file_config["datasources"][name] == data_context_config["datasources"][name]


def test_create_sqlalchemy_datasource(data_context):
    name = "test_sqlalchemy_datasource"
    type_ = "sqlalchemy"
    connection_kwargs = {
        "drivername": "postgresql",
        "username": "user",
        "password": "pass",
        "host": "host",
        "port": 1234,
        "database": "db",
    }

    # It should be possible to create a sqlalchemy source using these params without
    # saving a profile
    data_context.add_datasource(name, type_, **connection_kwargs)
    data_context_config = data_context.get_config()
    assert name in data_context_config["datasources"] 
    assert data_context_config["datasources"][name]["type"] == type_
    # That should be *all* we store absent a profile
    assert len(data_context_config["datasources"][name]) == 1

    # We should be able to get it in this session even without saving the config
    source = data_context.get_datasource(name)
    assert isinstance(source, SqlAlchemyDatasource)

    profile_name = "test_sqlalchemy_datasource"
    data_context.add_profile_credentials(profile_name, **connection_kwargs)

    # But we should be able to add a source using a profile
    name = "second_source"
    data_context.add_datasource(name, type_, profile_name="test_sqlalchemy_datasource")
    
    data_context_config = data_context.get_config()
    assert name in data_context_config["datasources"] 
    assert data_context_config["datasources"][name]["type"] == type_
    assert data_context_config["datasources"][name]["profile"] == profile_name

    source = data_context.get_datasource(name)
    assert isinstance(source, SqlAlchemyDatasource)

    # Finally, we should be able to confirm that the folder structure is as expected
    with open(os.path.join(data_context.context_root_directory, "uncommitted/credentials/profiles.yml"), "r") as profiles_file:
        profiles = yaml.safe_load(profiles_file)
    
    assert profiles == {
        profile_name: {**connection_kwargs}
    }

def test_create_sparkdf_datasource(data_context, tmpdir):
    name = "test_pandas_datasource"
    type_ = "pandas"

    data_context.add_datasource(name, type_, base_directory=str(tmpdir))
    data_context_config = data_context.get_config()

    assert name in data_context_config["datasources"] 
    assert data_context_config["datasources"][name]["type"] == type_ 


def test_file_kwargs_genenrator(data_context, tmpdir):
    # Put a few files in the directory
    with open(os.path.join(tmpdir, "f1.csv"), "w") as outfile:
        outfile.writelines(["a\tb\tc\n"])
    with open(os.path.join(tmpdir, "f2.csv"), "w") as outfile:
        outfile.writelines(["a\tb\tc\n"])

    os.makedirs(os.path.join(tmpdir, "f3"))
    with open(os.path.join(tmpdir, "f3", "f3_20190101.csv"), "w") as outfile:
        outfile.writelines(["a\tb\tc\n"])
    with open(os.path.join(tmpdir, "f3", "f3_20190102.csv"), "w") as outfile:
        outfile.writelines(["a\tb\tc\n"])

    datasource = data_context.add_datasource("default", "pandas", base_directory=str(tmpdir))
    generator = datasource.add_generator("defaut", "filesystem")

    known_data_asset_names = set(generator.list_data_asset_names())

    assert known_data_asset_names == set([
        "f1", "f2", "f3"
    ])

    f1_batches = [batch_kwargs for batch_kwargs in generator.yield_batch_kwargs("f1")]
    assert f1_batches[0] == {
            "path": os.path.join(tmpdir, "f1.csv")
        }
    assert len(f1_batches) == 1

    f3_batches = [batch_kwargs for batch_kwargs in generator.yield_batch_kwargs("f3")]
    expected_batches = [
        {
            "path": os.path.join(tmpdir, "f3", "f3_20190101.csv")
        },
        {
            "path": os.path.join(tmpdir, "f3", "f3_20190102.csv")
        }
    ]
    for batch in expected_batches:
        assert batch in f3_batches
    assert len(f3_batches) == 2

def test_sql_query_generator():
    assert True