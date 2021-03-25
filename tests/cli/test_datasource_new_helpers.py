from unittest import mock

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations.cli.datasource import (
    BigqueryCredentialYamlHelper,
    ConnectionStringCredentialYamlHelper,
    MySQLCredentialYamlHelper,
    PandasYamlHelper,
    PostgresCredentialYamlHelper,
    RedshiftCredentialYamlHelper,
    SnowflakeAuthMethod,
    SnowflakeCredentialYamlHelper,
    SparkYamlHelper,
    SQLCredentialYamlHelper,
    check_if_datasource_name_exists,
)
from great_expectations.datasource.types import DatasourceTypes


def test_SQLCredentialYamlHelper_defaults(empty_data_context):
    helper = SQLCredentialYamlHelper(usage_stats_payload={"foo": "bar"})
    expected_credentials_snippet = '''\
host = "YOUR_HOST"
port = "YOUR_PORT"
username = "YOUR_USERNAME"
password = "YOUR_PASSWORD"
database = "YOUR_DATABASE"'''
    assert helper.credentials_snippet() == expected_credentials_snippet

    assert (
        helper.yaml_snippet()
        == '''f"""
name: {datasource_name}
class_name: SimpleSqlalchemyDatasource
introspection:
  whole_table:
    data_asset_name_suffix: __whole_table
credentials:
  host: {host}
  port: '{port}'
  username: {username}
  password: {password}
  database: {database}"""'''
    )

    renderer = helper.get_notebook_renderer(empty_data_context)
    assert renderer.sql_credentials_code_snippet == expected_credentials_snippet


def test_SQLCredentialYamlHelper_driver(empty_data_context):
    helper = SQLCredentialYamlHelper(usage_stats_payload={"foo": "bar"}, driver="stuff")
    expected_credentials_snippet = '''\
host = "YOUR_HOST"
port = "YOUR_PORT"
username = "YOUR_USERNAME"
password = "YOUR_PASSWORD"
database = "YOUR_DATABASE"'''
    assert helper.credentials_snippet() == expected_credentials_snippet

    assert (
        helper.yaml_snippet()
        == '''f"""
name: {datasource_name}
class_name: SimpleSqlalchemyDatasource
introspection:
  whole_table:
    data_asset_name_suffix: __whole_table
credentials:
  host: {host}
  port: '{port}'
  username: {username}
  password: {password}
  database: {database}
  drivername: stuff"""'''
    )

    renderer = helper.get_notebook_renderer(empty_data_context)
    assert renderer.sql_credentials_code_snippet == expected_credentials_snippet


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_MySQLCredentialYamlHelper(mock_emit, empty_data_context_stats_enabled):
    helper = MySQLCredentialYamlHelper("my_datasource")
    expected_credentials_snippet = '''\
host = "YOUR_HOST"
port = "3306"
username = "YOUR_USERNAME"
password = "YOUR_PASSWORD"
database = "YOUR_DATABASE"'''
    assert helper.credentials_snippet() == expected_credentials_snippet

    assert (
        helper.yaml_snippet()
        == '''f"""
name: {datasource_name}
class_name: SimpleSqlalchemyDatasource
introspection:
  whole_table:
    data_asset_name_suffix: __whole_table
credentials:
  host: {host}
  port: '{port}'
  username: {username}
  password: {password}
  database: {database}
  drivername: mysql+pymysql"""'''
    )

    helper.send_backend_choice_usage_message(empty_data_context_stats_enabled)
    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "cli.new_ds_choice",
                "event_payload": {
                    "type": "sqlalchemy",
                    "db": "MySQL",
                    "api_version": "v3",
                },
                "success": True,
            }
        ),
    ]

    renderer = helper.get_notebook_renderer(empty_data_context_stats_enabled)
    assert renderer.sql_credentials_code_snippet == expected_credentials_snippet


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_PostgresCredentialYamlHelper(mock_emit, empty_data_context_stats_enabled):
    helper = PostgresCredentialYamlHelper("my_datasource")
    expected_credentials_snippet = '''\
host = "YOUR_HOST"
port = "5432"
username = "YOUR_USERNAME"
password = "YOUR_PASSWORD"
database = "postgres"'''
    assert helper.credentials_snippet() == expected_credentials_snippet

    assert (
        helper.yaml_snippet()
        == '''f"""
name: {datasource_name}
class_name: SimpleSqlalchemyDatasource
introspection:
  whole_table:
    data_asset_name_suffix: __whole_table
credentials:
  host: {host}
  port: '{port}'
  username: {username}
  password: {password}
  database: {database}
  drivername: postgresql"""'''
    )
    helper.send_backend_choice_usage_message(empty_data_context_stats_enabled)
    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "cli.new_ds_choice",
                "event_payload": {
                    "type": "sqlalchemy",
                    "db": "Postgres",
                    "api_version": "v3",
                },
                "success": True,
            }
        ),
    ]
    renderer = helper.get_notebook_renderer(empty_data_context_stats_enabled)
    assert renderer.sql_credentials_code_snippet == expected_credentials_snippet


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_RedshiftCredentialYamlHelper(mock_emit, empty_data_context_stats_enabled):
    helper = RedshiftCredentialYamlHelper("my_datasource")
    expected_credentials_snippet = '''\
host = "YOUR_HOST"
port = "5439"
username = "YOUR_USERNAME"
password = "YOUR_PASSWORD"
database = "YOUR_DATABASE"'''
    assert helper.credentials_snippet() == expected_credentials_snippet

    assert (
        helper.yaml_snippet()
        == '''f"""
name: {datasource_name}
class_name: SimpleSqlalchemyDatasource
introspection:
  whole_table:
    data_asset_name_suffix: __whole_table
credentials:
  host: {host}
  port: '{port}'
  username: {username}
  password: {password}
  database: {database}
  query:
    sslmode: prefer
  drivername: postgresql+psycopg2"""'''
    )
    helper.send_backend_choice_usage_message(empty_data_context_stats_enabled)
    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "cli.new_ds_choice",
                "event_payload": {
                    "type": "sqlalchemy",
                    "db": "Redshift",
                    "api_version": "v3",
                },
                "success": True,
            }
        ),
    ]

    renderer = helper.get_notebook_renderer(empty_data_context_stats_enabled)
    assert renderer.sql_credentials_code_snippet == expected_credentials_snippet


@mock.patch("click.prompt")
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_SnowflakeCredentialYamlHelper_password_auth(
    mock_emit, mock_prompt, empty_data_context_stats_enabled
):
    helper = SnowflakeCredentialYamlHelper("my_datasource")
    mock_prompt.side_effect = ["1"]
    helper.prompt()
    assert helper.auth_method == SnowflakeAuthMethod.USER_AND_PASSWORD

    expected_credentials_snippet = '''\
host = "YOUR_HOST"  # The account name (include region -- ex 'ABCD.us-east-1')
username = "YOUR_USERNAME"
database = ""  # The database name (optional -- leave blank for none)
schema = ""  # The schema name (optional -- leave blank for none)
warehouse = ""  # The warehouse name (optional -- leave blank for none)
role = ""  # The role name (optional -- leave blank for none)
password = "YOUR_PASSWORD"'''
    assert helper.credentials_snippet() == expected_credentials_snippet

    assert (
        helper.yaml_snippet()
        == '''f"""
name: {datasource_name}
class_name: SimpleSqlalchemyDatasource
introspection:
  whole_table:
    data_asset_name_suffix: __whole_table
credentials:
  host: {host}
  username: {username}
  database: {database}
  query:
    schema: {schema}
    warehouse: {warehouse}
    role: {role}
  password: {password}
  drivername: snowflake"""'''
    )
    helper.send_backend_choice_usage_message(empty_data_context_stats_enabled)
    _snowflake_usage_stats_assertions(mock_emit)

    renderer = helper.get_notebook_renderer(empty_data_context_stats_enabled)
    assert renderer.sql_credentials_code_snippet == expected_credentials_snippet


@mock.patch("click.prompt")
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_SnowflakeCredentialYamlHelper_sso_auth(
    mock_emit, mock_prompt, empty_data_context_stats_enabled
):
    helper = SnowflakeCredentialYamlHelper("my_datasource")
    mock_prompt.side_effect = ["2"]
    helper.prompt()
    assert helper.auth_method == SnowflakeAuthMethod.SSO

    expected_credentials_snippet = """\
host = "YOUR_HOST"  # The account name (include region -- ex 'ABCD.us-east-1')
username = "YOUR_USERNAME"
database = ""  # The database name (optional -- leave blank for none)
schema = ""  # The schema name (optional -- leave blank for none)
warehouse = ""  # The warehouse name (optional -- leave blank for none)
role = ""  # The role name (optional -- leave blank for none)
authenticator_url = "externalbrowser"  # A valid okta URL or 'externalbrowser' used to connect through SSO"""
    assert helper.credentials_snippet() == expected_credentials_snippet

    assert (
        helper.yaml_snippet()
        == '''f"""
name: {datasource_name}
class_name: SimpleSqlalchemyDatasource
introspection:
  whole_table:
    data_asset_name_suffix: __whole_table
credentials:
  host: {host}
  username: {username}
  database: {database}
  query:
    schema: {schema}
    warehouse: {warehouse}
    role: {role}
  connect_args:
    authenticator: {authenticator_url}
  drivername: snowflake"""'''
    )
    helper.send_backend_choice_usage_message(empty_data_context_stats_enabled)
    _snowflake_usage_stats_assertions(mock_emit)
    renderer = helper.get_notebook_renderer(empty_data_context_stats_enabled)
    assert renderer.sql_credentials_code_snippet == expected_credentials_snippet


@mock.patch("click.prompt")
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_SnowflakeCredentialYamlHelper_key_pair_auth(
    mock_emit, mock_prompt, empty_data_context_stats_enabled
):
    helper = SnowflakeCredentialYamlHelper("my_datasource")
    mock_prompt.side_effect = ["3"]
    helper.prompt()
    assert helper.auth_method == SnowflakeAuthMethod.KEY_PAIR

    expected_credentials_snippet = """host = "YOUR_HOST"  # The account name (include region -- ex 'ABCD.us-east-1')
username = "YOUR_USERNAME"
database = ""  # The database name (optional -- leave blank for none)
schema = ""  # The schema name (optional -- leave blank for none)
warehouse = ""  # The warehouse name (optional -- leave blank for none)
role = ""  # The role name (optional -- leave blank for none)
private_key_path = "YOUR_KEY_PATH"  # Path to the private key used for authentication
private_key_passphrase = ""   # Passphrase for the private key used for authentication (optional -- leave blank for none)"""
    assert helper.credentials_snippet() == expected_credentials_snippet

    assert (
        helper.yaml_snippet()
        == '''f"""
name: {datasource_name}
class_name: SimpleSqlalchemyDatasource
introspection:
  whole_table:
    data_asset_name_suffix: __whole_table
credentials:
  host: {host}
  username: {username}
  database: {database}
  query:
    schema: {schema}
    warehouse: {warehouse}
    role: {role}
  private_key_path: {private_key_path}
  private_key_passphrase: {private_key_passphrase}
  drivername: snowflake"""'''
    )
    helper.send_backend_choice_usage_message(empty_data_context_stats_enabled)
    _snowflake_usage_stats_assertions(mock_emit)
    renderer = helper.get_notebook_renderer(empty_data_context_stats_enabled)
    assert renderer.sql_credentials_code_snippet == expected_credentials_snippet


def _snowflake_usage_stats_assertions(mock_emit):
    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "cli.new_ds_choice",
                "event_payload": {
                    "type": "sqlalchemy",
                    "db": "Snowflake",
                    "api_version": "v3",
                },
                "success": True,
            }
        ),
    ]


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_BigqueryCredentialYamlHelper(mock_emit, empty_data_context_stats_enabled):
    helper = BigqueryCredentialYamlHelper("my_datasource")
    assert (
        helper.credentials_snippet()
        == '''\
# The SQLAlchemy url/connection string for the BigQuery connection
# (reference: https://github.com/mxmzdlv/pybigquery#connection-string-parameters)"""
connection_string = "YOUR_BIGQUERY_CONNECTION_STRING"'''
    )

    assert (
        helper.yaml_snippet()
        == '''f"""
name: {datasource_name}
class_name: SimpleSqlalchemyDatasource
introspection:
  whole_table:
    data_asset_name_suffix: __whole_table
connection_string: {connection_string}"""'''
    )
    helper.send_backend_choice_usage_message(empty_data_context_stats_enabled)
    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "cli.new_ds_choice",
                "event_payload": {
                    "type": "sqlalchemy",
                    "db": "BigQuery",
                    "api_version": "v3",
                },
                "success": True,
            }
        ),
    ]


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_ConnectionStringCredentialYamlHelper(
    mock_emit, empty_data_context_stats_enabled
):
    helper = ConnectionStringCredentialYamlHelper("my_datasource")
    assert (
        helper.credentials_snippet()
        == '''\
# The url/connection string for the sqlalchemy connection
# (reference: https://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls)
connection_string = "YOUR_CONNECTION_STRING"'''
    )

    assert (
        helper.yaml_snippet()
        == '''f"""
name: {datasource_name}
class_name: SimpleSqlalchemyDatasource
introspection:
  whole_table:
    data_asset_name_suffix: __whole_table
connection_string: {connection_string}"""'''
    )

    assert helper.verify_libraries_installed() is True
    helper.send_backend_choice_usage_message(empty_data_context_stats_enabled)
    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "cli.new_ds_choice",
                "event_payload": {
                    "type": "sqlalchemy",
                    "db": "other",
                    "api_version": "v3",
                },
                "success": True,
            }
        ),
    ]


@mock.patch("click.prompt")
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_PandasYamlHelper(mock_emit, mock_prompt, empty_data_context_stats_enabled):
    helper = PandasYamlHelper(context_root_dir="foo", datasource_name="bar")
    assert helper.context_root_dir == "foo"
    assert helper.datasource_name == "bar"
    assert helper.datasource_type == DatasourceTypes.PANDAS
    assert helper.class_name == "PandasExecutionEngine"

    assert helper.verify_libraries_installed() is True

    helper.send_backend_choice_usage_message(empty_data_context_stats_enabled)
    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "cli.new_ds_choice",
                "event_payload": {"type": "pandas", "api_version": "v3"},
                "success": True,
            }
        ),
    ]
    assert helper.base_path == ""
    mock_prompt.side_effect = ["path/to/data"]
    helper.prompt()
    assert helper.base_path == "../path/to/data"

    assert (
        helper.yaml_snippet()
        == '''f"""
name: {datasource_name}
class_name: Datasource
execution_engine:
  class_name: PandasExecutionEngine
data_connectors:
  {datasource_name}_example_data_connector:
    class_name: InferredAssetFilesystemDataConnector
    datasource_name: {datasource_name}
    base_directory: ../path/to/data
    default_regex:
      group_names: data_asset_name
      pattern: (.*)
"""'''
    )


@mock.patch("click.prompt")
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_SparkYamlHelper(mock_emit, mock_prompt, empty_data_context_stats_enabled):
    helper = SparkYamlHelper(context_root_dir="foo", datasource_name="bar")
    assert helper.context_root_dir == "foo"
    assert helper.datasource_name == "bar"
    assert helper.datasource_type == DatasourceTypes.SPARK
    assert helper.class_name == "SparkDFExecutionEngine"

    helper.send_backend_choice_usage_message(empty_data_context_stats_enabled)
    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "cli.new_ds_choice",
                "event_payload": {"type": "spark", "api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert helper.base_path == ""
    mock_prompt.side_effect = ["path/to/data"]
    helper.prompt()
    assert helper.base_path == "../path/to/data"

    assert (
        helper.yaml_snippet()
        == '''f"""
name: {datasource_name}
class_name: Datasource
execution_engine:
  class_name: SparkDFExecutionEngine
data_connectors:
  {datasource_name}_example_data_connector:
    class_name: InferredAssetFilesystemDataConnector
    datasource_name: {datasource_name}
    base_directory: ../path/to/data
    default_regex:
      group_names: data_asset_name
      pattern: (.*)
"""'''
    )


def test_check_if_datasource_name_exists(
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):

    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    assert [d["name"] for d in context.list_datasources()] == ["my_datasource"]
    assert len(context.list_datasources()) == 1

    # Exists
    assert check_if_datasource_name_exists(
        context=context, datasource_name="my_datasource"
    )

    # Doesn't exist
    assert (
        check_if_datasource_name_exists(
            context=context, datasource_name="nonexistent_datasource"
        )
        is False
    )
