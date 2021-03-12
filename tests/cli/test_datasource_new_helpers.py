import mock

from great_expectations.cli.datasource import (
    BigqueryCredentialYamlHelper,
    GenericConnectionStringCredentialYamlHelper,
    MySQLCredentialYamlHelper,
    PandasYamlHelper,
    PostgresCredentialYamlHelper,
    RedshiftCredentialYamlHelper,
    SnowflakeAuthMethod,
    SnowflakeCredentialYamlHelper,
    SparkYamlHelper,
    SQLCredentialYamlHelper,
)
from great_expectations.datasource.types import DatasourceTypes


def test_SQLCredentialYamlHelper_defaults():
    helper = SQLCredentialYamlHelper(usage_stats_payload={"foo": "bar"})
    assert (
        helper.credentials_snippet()
        == '''\
host = "YOUR_HOST"
port = 
username = "YOUR_USERNAME"
password = "YOUR_PASSWORD"
database = "YOUR_DATABASE"'''
    )

    assert (
        helper.yaml_snippet()
        == '''f"""
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


def test_SQLCredentialYamlHelper_driver():
    helper = SQLCredentialYamlHelper(usage_stats_payload={"foo": "bar"}, driver="stuff")
    assert (
        helper.credentials_snippet()
        == '''\
host = "YOUR_HOST"
port = 
username = "YOUR_USERNAME"
password = "YOUR_PASSWORD"
database = "YOUR_DATABASE"'''
    )

    assert (
        helper.yaml_snippet()
        == '''f"""
class_name: SimpleSqlalchemyDatasource
introspection:
  whole_table:
    data_asset_name_suffix: __whole_table
credentials:
  drivername: stuff
  host: {host}
  port: '{port}'
  username: {username}
  password: {password}
  database: {database}"""'''
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_MySQLCredentialYamlHelper(mock_emit, empty_data_context_stats_enabled):
    helper = MySQLCredentialYamlHelper("my_datasource")
    assert (
        helper.credentials_snippet()
        == '''\
host = "YOUR_HOST"
port = 3306
username = "YOUR_USERNAME"
password = "YOUR_PASSWORD"
database = "YOUR_DATABASE"'''
    )

    assert (
        helper.yaml_snippet()
        == '''f"""
class_name: SimpleSqlalchemyDatasource
introspection:
  whole_table:
    data_asset_name_suffix: __whole_table
credentials:
  drivername: mysql+pymysql
  host: {host}
  port: '{port}'
  username: {username}
  password: {password}
  database: {database}"""'''
    )

    helper.send_backend_choice_usage_message(empty_data_context_stats_enabled)
    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "cli.new_ds_choice",
                # TODO taylor not sure what the expected message is since I could not find a test that checked for this
                "event_payload": {
                    "type": "sqlalchemy",
                    "db": "MySQL",
                    "api_version": "v3",
                },
                "success": True,
            }
        ),
    ]


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_PostgresCredentialYamlHelper(mock_emit, empty_data_context_stats_enabled):
    helper = PostgresCredentialYamlHelper("my_datasource")
    assert (
        helper.credentials_snippet()
        == '''\
host = "YOUR_HOST"
port = 5432
username = "YOUR_USERNAME"
password = "YOUR_PASSWORD"
database = "postgres"'''
    )

    assert (
        helper.yaml_snippet()
        == '''f"""
class_name: SimpleSqlalchemyDatasource
introspection:
  whole_table:
    data_asset_name_suffix: __whole_table
credentials:
  drivername: postgresql
  host: {host}
  port: '{port}'
  username: {username}
  password: {password}
  database: {database}"""'''
    )
    helper.send_backend_choice_usage_message(empty_data_context_stats_enabled)
    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "cli.new_ds_choice",
                # TODO taylor not sure what the expected message is since I could not find a test that checked for this
                "event_payload": {
                    "type": "sqlalchemy",
                    "db": "Postgres",
                    "api_version": "v3",
                },
                "success": True,
            }
        ),
    ]


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_RedshiftCredentialYamlHelper(mock_emit, empty_data_context_stats_enabled):
    helper = RedshiftCredentialYamlHelper("my_datasource")
    assert (
        helper.credentials_snippet()
        == '''\
host = "YOUR_HOST"
port = 5439
username = "YOUR_USERNAME"
password = "YOUR_PASSWORD"
database = "YOUR_DATABASE"'''
    )

    assert (
        helper.yaml_snippet()
        == '''f"""
class_name: SimpleSqlalchemyDatasource
introspection:
  whole_table:
    data_asset_name_suffix: __whole_table
credentials:
  drivername: postgresql+psycopg2
  host: {host}
  port: '{port}'
  username: {username}
  password: {password}
  database: {database}
  query:
    sslmode: prefer"""'''
    )
    helper.send_backend_choice_usage_message(empty_data_context_stats_enabled)
    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "cli.new_ds_choice",
                # TODO taylor not sure what the expected message is since I could not find a test that checked for this
                "event_payload": {
                    "type": "sqlalchemy",
                    "db": "Redshift",
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
def test_SnowflakeCredentialYamlHelper_password_auth(
    mock_emit, mock_prompt, empty_data_context_stats_enabled
):
    helper = SnowflakeCredentialYamlHelper("my_datasource")
    mock_prompt.side_effect = ["1"]
    helper.prompt()
    assert helper.auth_method == SnowflakeAuthMethod.USER_AND_PASSWORD

    assert (
        helper.credentials_snippet()
        == '''\
host = "YOUR_HOST"  # The account name (include region -- ex 'ABCD.us-east-1')
username = "YOUR_USERNAME"
database = ""  # The database name (optional -- leave blank for none)
schema = ""  # The schema name (optional -- leave blank for none)
warehouse = ""  # The warehouse name (optional -- leave blank for none)
role = ""  # The role name (optional -- leave blank for none)
password = "{self.password}"'''
    )

    assert (
        helper.yaml_snippet()
        == '''f"""
class_name: SimpleSqlalchemyDatasource
introspection:
  whole_table:
    data_asset_name_suffix: __whole_table
credentials:
  drivername: snowflake
  host: {host}
  username: {username}
  database: {database}
  query:
    schema: {schema}
    warehouse: {warehouse}
    role: {role}
  password: {password}
"""'''
    )
    helper.send_backend_choice_usage_message(empty_data_context_stats_enabled)
    _snowflake_usage_stats_assertions(mock_emit)


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

    assert (
        helper.credentials_snippet()
        == """\
host = "YOUR_HOST"  # The account name (include region -- ex 'ABCD.us-east-1')
username = "YOUR_USERNAME"
database = ""  # The database name (optional -- leave blank for none)
schema = ""  # The schema name (optional -- leave blank for none)
warehouse = ""  # The warehouse name (optional -- leave blank for none)
role = ""  # The role name (optional -- leave blank for none)
authenticator_url = "externalbrowser"  # A valid okta URL or 'externalbrowser' used to connect through SSO"""
    )

    assert (
        helper.yaml_snippet()
        == '''f"""
class_name: SimpleSqlalchemyDatasource
introspection:
  whole_table:
    data_asset_name_suffix: __whole_table
credentials:
  drivername: snowflake
  host: {host}
  username: {username}
  database: {database}
  query:
    schema: {schema}
    warehouse: {warehouse}
    role: {role}
  connect_args:
    authenticator: {authenticator_url}
"""'''
    )
    helper.send_backend_choice_usage_message(empty_data_context_stats_enabled)
    _snowflake_usage_stats_assertions(mock_emit)


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

    assert (
        helper.credentials_snippet()
        == """host = "YOUR_HOST"  # The account name (include region -- ex 'ABCD.us-east-1')
username = "YOUR_USERNAME"
database = ""  # The database name (optional -- leave blank for none)
schema = ""  # The schema name (optional -- leave blank for none)
warehouse = ""  # The warehouse name (optional -- leave blank for none)
role = ""  # The role name (optional -- leave blank for none)
private_key_path = "YOUR_KEY_PATH"  # Path to the private key used for authentication
private_key_passphrase = ""   # Passphrase for the private key used for authentication (optional -- leave blank for none)"""
    )

    assert (
        helper.yaml_snippet()
        == '''f"""
class_name: SimpleSqlalchemyDatasource
introspection:
  whole_table:
    data_asset_name_suffix: __whole_table
credentials:
  drivername: snowflake
  host: {host}
  username: {username}
  database: {database}
  query:
    schema: {schema}
    warehouse: {warehouse}
    role: {role}
  private_key_path: {private_key_path}
  private_key_passphrase: {private_key_passphrase}
"""'''
    )
    helper.send_backend_choice_usage_message(empty_data_context_stats_enabled)
    _snowflake_usage_stats_assertions(mock_emit)


def _snowflake_usage_stats_assertions(mock_emit):
    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "cli.new_ds_choice",
                # TODO taylor not sure what the expected message is since I could not find a test that checked for this
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
class_name: SimpleSqlalchemyDatasource
introspection:
  whole_table:
    data_asset_name_suffix: __whole_table
credentials:
  connection_string: {connection_string}"""'''
    )
    helper.send_backend_choice_usage_message(empty_data_context_stats_enabled)
    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "cli.new_ds_choice",
                # TODO taylor not sure what the expected message is since I could not find a test that checked for this
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
def test_GenericConnectionStringCredentialYamlHelper(
    mock_emit, empty_data_context_stats_enabled
):
    helper = GenericConnectionStringCredentialYamlHelper("my_datasource")
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
class_name: SimpleSqlalchemyDatasource
introspection:
  whole_table:
    data_asset_name_suffix: __whole_table
credentials:
  connection_string: {connection_string}"""'''
    )

    assert helper.verify_libraries_installed() is True
    helper.send_backend_choice_usage_message(empty_data_context_stats_enabled)
    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "cli.new_ds_choice",
                # TODO taylor not sure what the expected message is since I could not find a test that checked for this
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
    assert helper.base_path == "path/to/data"

    assert (
        helper.yaml_snippet()
        == '''f"""
class_name: Datasource
execution_engine:
  class_name: PandasExecutionEngine
data_connectors:
  {datasource_name}_example_data_connector:
    class_name: InferredAssetFilesystemDataConnector
    datasource_name: {datasource_name}
    base_directory: path/to/data
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
    assert helper.base_path == "path/to/data"

    assert (
        helper.yaml_snippet()
        == '''f"""
class_name: Datasource
execution_engine:
  class_name: SparkDFExecutionEngine
data_connectors:
  {datasource_name}_example_data_connector:
    class_name: InferredAssetFilesystemDataConnector
    datasource_name: {datasource_name}
    base_directory: path/to/data
    default_regex:
      group_names: data_asset_name
      pattern: (.*)
"""'''
    )
