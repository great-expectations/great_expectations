import pytest

from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource


def test_sanitize_yaml_and_save_datasource_raises_error_on_empty_yaml(
    empty_data_context,
):
    with pytest.raises(ValueError):
        sanitize_yaml_and_save_datasource(empty_data_context, "")


def test_sanitize_yaml_and_save_datasource_raises_error_on_non_string(
    empty_data_context,
):
    with pytest.raises(TypeError):
        for bad_input in [3, {"a", "b"}]:
            sanitize_yaml_and_save_datasource(empty_data_context, bad_input)


def test_sanitize_yaml_and_save_datasource_raises_error_on_missing_datasource_name(
    empty_data_context,
):
    yaml_snippet = """
class_name: SimpleSqlalchemyDatasource
introspection:
  whole_table:
    data_asset_name_suffix: __whole_table
connection_string: sqlite://"""

    with pytest.raises(ValueError):
        sanitize_yaml_and_save_datasource(empty_data_context, yaml_snippet)


def test_sanitize_yaml_and_save_datasource_works_without_credentials(
    sa,
    empty_data_context,
):
    context = empty_data_context
    yaml_snippet = """
name: my_datasource
class_name: SimpleSqlalchemyDatasource
introspection:
  whole_table:
    data_asset_name_suffix: __whole_table
connection_string: sqlite://"""

    assert len(context.list_datasources()) == 0
    sanitize_yaml_and_save_datasource(context, yaml_snippet)
    assert len(context.list_datasources()) == 1
    assert context.list_datasources() == [
        {
            "class_name": "SimpleSqlalchemyDatasource",
            "connection_string": "sqlite://",
            "introspection": {
                "whole_table": {"data_asset_name_suffix": "__whole_table"}
            },
            "module_name": "great_expectations.datasource",
            "name": "my_datasource",
        }
    ]
    obs = context.config_variables
    # remove the instance guid
    obs.pop("instance_id")
    assert obs == {}


def test_sanitize_yaml_and_save_datasource_works_with_credentials(
    sa,
    empty_data_context,
):
    context = empty_data_context
    yaml_snippet = """
name: foo_datasource
class_name: SimpleSqlalchemyDatasource
credentials:
  host: localhost
  port: '5432'
  username: user
  password: pass
  database: postgres
  drivername: postgresql"""

    assert len(context.list_datasources()) == 0
    sanitize_yaml_and_save_datasource(context, yaml_snippet)
    assert len(context.list_datasources()) == 1
    assert context.list_datasources() == [
        {
            "class_name": "SimpleSqlalchemyDatasource",
            "credentials": {
                "database": "postgres",
                "drivername": "postgresql",
                "host": "localhost",
                "password": "***",
                "port": "5432",
                "username": "user",
            },
            "module_name": "great_expectations.datasource",
            "name": "foo_datasource",
        }
    ]
    obs = context.config_variables
    # remove the instance guid
    obs.pop("instance_id")
    assert obs == {
        "foo_datasource": {
            "database": "postgres",
            "drivername": "postgresql",
            "host": "localhost",
            "password": "pass",
            "port": "5432",
            "username": "user",
        }
    }
