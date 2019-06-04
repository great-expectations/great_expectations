import pytest

from ruamel.yaml import YAML
yaml = YAML(typ='safe')
import os
import shutil

from great_expectations.data_context import DataContext
from great_expectations.data_context.datasource.sqlalchemy_source import SqlAlchemyDatasource


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
    with open(os.path.join(data_context.context_root_directory, "great_expectations/great_expectations.yml"), "r") as data_context_config_file:
        data_context_file_config = yaml.load(data_context_config_file)

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
    with open(os.path.join(data_context.context_root_directory, "great_expectations/uncommitted/credentials/profiles.yml"), "r") as profiles_file:
        profiles = yaml.load(profiles_file)
    
    assert profiles == {
        profile_name: dict(**connection_kwargs)
    }

def test_create_sparkdf_datasource(data_context, tmp_path_factory):
    base_dir = tmp_path_factory.mktemp('test_create_sparkdf_datasource')
    name = "test_pandas_datasource"
    type_ = "pandas"

    data_context.add_datasource(name, type_, base_directory=str(base_dir))
    data_context_config = data_context.get_config()

    assert name in data_context_config["datasources"] 
    assert data_context_config["datasources"][name]["type"] == type_ 

def test_sqlalchemysource_templating(sqlitedb_engine):
    datasource = SqlAlchemyDatasource(engine=sqlitedb_engine)
    generator = datasource.get_generator()
    generator.add_query("test", "select 'cat' as animal_name;")
    df = datasource.get_data_asset("test")
    assert True