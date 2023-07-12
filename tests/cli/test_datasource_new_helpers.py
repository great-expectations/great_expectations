from unittest import mock

import pytest

from great_expectations import DataContext
from great_expectations.cli.datasource import (
    AthenaCredentialYamlHelper,
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


pytestmark = [pytest.mark.cli]


@pytest.mark.unit
def test_SQLCredentialYamlHelper_defaults(empty_data_context):
    helper = SQLCredentialYamlHelper(usage_stats_payload={"foo": "bar"})
    expected_credentials_snippet = '''\
host = "YOUR_HOST"
port = "YOUR_PORT"
username = "YOUR_USERNAME"
password = "YOUR_PASSWORD"
database = "YOUR_DATABASE"
schema_name = "YOUR_SCHEMA"

# A table that you would like to add initially as a Data Asset
table_name = "YOUR_TABLE_NAME"'''
    assert helper.credentials_snippet() == expected_credentials_snippet
    assert (
        helper.yaml_snippet()
        == '''f"""
name: {datasource_name}
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  credentials:
    host: {host}
    port: '{port}'
    username: {username}
    password: {password}
    database: {database}
data_connectors:
  default_runtime_data_connector_name:
    class_name: RuntimeDataConnector
    batch_identifiers:
      - default_identifier_name
  default_inferred_data_connector_name:
    class_name: InferredAssetSqlDataConnector
    include_schema_name: True
    introspection_directives:
      schema_name: {schema_name}
  default_configured_data_connector_name:
    class_name: ConfiguredAssetSqlDataConnector
    assets:
      {table_name}:
        class_name: Asset
        schema_name: {schema_name}
"""'''
    )

    renderer = helper.get_notebook_renderer(empty_data_context)
    assert renderer.sql_credentials_code_snippet == expected_credentials_snippet


@pytest.mark.unit
def test_SQLCredentialYamlHelper_driver(empty_data_context):
    helper = SQLCredentialYamlHelper(usage_stats_payload={"foo": "bar"}, driver="stuff")
    expected_credentials_snippet = '''\
host = "YOUR_HOST"
port = "YOUR_PORT"
username = "YOUR_USERNAME"
password = "YOUR_PASSWORD"
database = "YOUR_DATABASE"
schema_name = "YOUR_SCHEMA"

# A table that you would like to add initially as a Data Asset
table_name = "YOUR_TABLE_NAME"'''
    assert helper.credentials_snippet() == expected_credentials_snippet

    print(helper.yaml_snippet())

    assert (
        helper.yaml_snippet()
        == '''f"""
name: {datasource_name}
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  credentials:
    host: {host}
    port: '{port}'
    username: {username}
    password: {password}
    database: {database}
    drivername: stuff
data_connectors:
  default_runtime_data_connector_name:
    class_name: RuntimeDataConnector
    batch_identifiers:
      - default_identifier_name
  default_inferred_data_connector_name:
    class_name: InferredAssetSqlDataConnector
    include_schema_name: True
    introspection_directives:
      schema_name: {schema_name}
  default_configured_data_connector_name:
    class_name: ConfiguredAssetSqlDataConnector
    assets:
      {table_name}:
        class_name: Asset
        schema_name: {schema_name}
"""'''
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
database = "YOUR_DATABASE"
schema_name = "YOUR_SCHEMA"

# A table that you would like to add initially as a Data Asset
table_name = "YOUR_TABLE_NAME"'''
    assert helper.credentials_snippet() == expected_credentials_snippet

    assert (
        helper.yaml_snippet()
        == '''f"""
name: {datasource_name}
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  credentials:
    host: {host}
    port: '{port}'
    username: {username}
    password: {password}
    database: {database}
    drivername: mysql+pymysql
data_connectors:
  default_runtime_data_connector_name:
    class_name: RuntimeDataConnector
    batch_identifiers:
      - default_identifier_name
  default_inferred_data_connector_name:
    class_name: InferredAssetSqlDataConnector
    include_schema_name: True
    introspection_directives:
      schema_name: {schema_name}
  default_configured_data_connector_name:
    class_name: ConfiguredAssetSqlDataConnector
    assets:
      {table_name}:
        class_name: Asset
        schema_name: {schema_name}
"""'''
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
database = "YOUR_DATABASE"
schema_name = "YOUR_SCHEMA"

# A table that you would like to add initially as a Data Asset
table_name = "YOUR_TABLE_NAME"'''
    assert helper.credentials_snippet() == expected_credentials_snippet

    assert (
        helper.yaml_snippet()
        == '''f"""
name: {datasource_name}
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  credentials:
    host: {host}
    port: '{port}'
    username: {username}
    password: {password}
    database: {database}
    drivername: postgresql
data_connectors:
  default_runtime_data_connector_name:
    class_name: RuntimeDataConnector
    batch_identifiers:
      - default_identifier_name
  default_inferred_data_connector_name:
    class_name: InferredAssetSqlDataConnector
    include_schema_name: True
    introspection_directives:
      schema_name: {schema_name}
  default_configured_data_connector_name:
    class_name: ConfiguredAssetSqlDataConnector
    assets:
      {table_name}:
        class_name: Asset
        schema_name: {schema_name}
"""'''
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
database = "YOUR_DATABASE"
schema_name = "YOUR_SCHEMA"

# A table that you would like to add initially as a Data Asset
table_name = "YOUR_TABLE_NAME"'''
    assert helper.credentials_snippet() == expected_credentials_snippet
    assert (
        helper.yaml_snippet()
        == '''f"""
name: {datasource_name}
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  credentials:
    host: {host}
    port: '{port}'
    username: {username}
    password: {password}
    database: {database}
    query:
      sslmode: prefer
    drivername: postgresql+psycopg2
data_connectors:
  default_runtime_data_connector_name:
    class_name: RuntimeDataConnector
    batch_identifiers:
      - default_identifier_name
  default_inferred_data_connector_name:
    class_name: InferredAssetSqlDataConnector
    include_schema_name: True
    introspection_directives:
      schema_name: {schema_name}
  default_configured_data_connector_name:
    class_name: ConfiguredAssetSqlDataConnector
    assets:
      {table_name}:
        class_name: Asset
        schema_name: {schema_name}
"""'''
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
database = ""  # The database name
schema_name = ""  # The schema name
warehouse = ""  # The warehouse name
role = ""  # The role name
table_name = ""  # A table that you would like to add initially as a Data Asset
password = "YOUR_PASSWORD"'''

    assert helper.credentials_snippet() == expected_credentials_snippet

    assert (
        helper.yaml_snippet()
        == '''f"""
name: {datasource_name}
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  credentials:
    host: {host}
    username: {username}
    database: {database}
    query:
      schema: {schema_name}
      warehouse: {warehouse}
      role: {role}
    password: {password}
    drivername: snowflake
data_connectors:
  default_runtime_data_connector_name:
    class_name: RuntimeDataConnector
    batch_identifiers:
      - default_identifier_name
  default_inferred_data_connector_name:
    class_name: InferredAssetSqlDataConnector
    include_schema_name: True
    introspection_directives:
      schema_name: {schema_name}
  default_configured_data_connector_name:
    class_name: ConfiguredAssetSqlDataConnector
    assets:
      {table_name}:
        class_name: Asset
        schema_name: {schema_name}
"""'''
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
database = ""  # The database name
schema_name = ""  # The schema name
warehouse = ""  # The warehouse name
role = ""  # The role name
table_name = ""  # A table that you would like to add initially as a Data Asset
authenticator_url = "externalbrowser"  # A valid okta URL or 'externalbrowser' used to connect through SSO"""
    assert helper.credentials_snippet() == expected_credentials_snippet
    assert (
        helper.yaml_snippet()
        == '''f"""
name: {datasource_name}
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  credentials:
    host: {host}
    username: {username}
    database: {database}
    query:
      schema: {schema_name}
      warehouse: {warehouse}
      role: {role}
    connect_args:
      authenticator: {authenticator_url}
    drivername: snowflake
data_connectors:
  default_runtime_data_connector_name:
    class_name: RuntimeDataConnector
    batch_identifiers:
      - default_identifier_name
  default_inferred_data_connector_name:
    class_name: InferredAssetSqlDataConnector
    include_schema_name: True
    introspection_directives:
      schema_name: {schema_name}
  default_configured_data_connector_name:
    class_name: ConfiguredAssetSqlDataConnector
    assets:
      {table_name}:
        class_name: Asset
        schema_name: {schema_name}
"""'''
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

    expected_credentials_snippet = """\
host = "YOUR_HOST"  # The account name (include region -- ex 'ABCD.us-east-1')
username = "YOUR_USERNAME"
database = ""  # The database name
schema_name = ""  # The schema name
warehouse = ""  # The warehouse name
role = ""  # The role name
table_name = ""  # A table that you would like to add initially as a Data Asset
private_key_path = "YOUR_KEY_PATH"  # Path to the private key used for authentication
private_key_passphrase = ""   # Passphrase for the private key used for authentication (optional -- leave blank for none)"""
    assert helper.credentials_snippet() == expected_credentials_snippet

    assert (
        helper.yaml_snippet()
        == '''f"""
name: {datasource_name}
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  credentials:
    host: {host}
    username: {username}
    database: {database}
    query:
      schema: {schema_name}
      warehouse: {warehouse}
      role: {role}
    private_key_path: {private_key_path}
    private_key_passphrase: {private_key_passphrase}
    drivername: snowflake
data_connectors:
  default_runtime_data_connector_name:
    class_name: RuntimeDataConnector
    batch_identifiers:
      - default_identifier_name
  default_inferred_data_connector_name:
    class_name: InferredAssetSqlDataConnector
    include_schema_name: True
    introspection_directives:
      schema_name: {schema_name}
  default_configured_data_connector_name:
    class_name: ConfiguredAssetSqlDataConnector
    assets:
      {table_name}:
        class_name: Asset
        schema_name: {schema_name}
"""'''
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
# (reference: https://github.com/googleapis/python-bigquery-sqlalchemy#connection-string-parameters)"""
connection_string = "YOUR_BIGQUERY_CONNECTION_STRING"

schema_name = ""  # or dataset name
table_name = ""'''
    )

    assert (
        helper.yaml_snippet()
        == '''f"""
name: {datasource_name}
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  connection_string: {connection_string}
data_connectors:
  default_runtime_data_connector_name:
    class_name: RuntimeDataConnector
    batch_identifiers:
      - default_identifier_name
  default_inferred_data_connector_name:
    class_name: InferredAssetSqlDataConnector
    include_schema_name: True
    introspection_directives:
      schema_name: {schema_name}
  default_configured_data_connector_name:
    class_name: ConfiguredAssetSqlDataConnector
    assets:
      {table_name}:
        class_name: Asset
        schema_name: {schema_name}
"""'''
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
        == '# The url/connection string for the sqlalchemy connection\n# (reference: https://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls)\nconnection_string = "YOUR_CONNECTION_STRING"\n\n# If schema_name is not relevant to your SQL backend (i.e. SQLite),\n# please remove from the following line and the configuration below\nschema_name = "YOUR_SCHEMA"\n\n# A table that you would like to add initially as a Data Asset\ntable_name = "YOUR_TABLE_NAME"'
    )

    assert (
        helper.yaml_snippet()
        == '''f"""
name: {datasource_name}
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  connection_string: {connection_string}
data_connectors:
  default_runtime_data_connector_name:
    class_name: RuntimeDataConnector
    batch_identifiers:
      - default_identifier_name
  default_inferred_data_connector_name:
    class_name: InferredAssetSqlDataConnector
    include_schema_name: True
    introspection_directives:
      schema_name: {schema_name}
  default_configured_data_connector_name:
    class_name: ConfiguredAssetSqlDataConnector
    assets:
      {table_name}:
        class_name: Asset
        schema_name: {schema_name}
"""'''
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
  default_inferred_data_connector_name:
    class_name: InferredAssetFilesystemDataConnector
    base_directory: ../path/to/data
    default_regex:
      group_names:
        - data_asset_name
      pattern: (.*)
  default_runtime_data_connector_name:
    class_name: RuntimeDataConnector
    assets:
      my_runtime_asset_name:
        batch_identifiers:
          - runtime_batch_identifier_name
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
  default_inferred_data_connector_name:
    class_name: InferredAssetFilesystemDataConnector
    base_directory: ../path/to/data
    default_regex:
      group_names:
        - data_asset_name
      pattern: (.*)
  default_runtime_data_connector_name:
    class_name: RuntimeDataConnector
    assets:
      my_runtime_asset_name:
        batch_identifiers:
          - runtime_batch_identifier_name
"""'''
    )


@pytest.mark.slow  # 1.45s
def test_check_if_datasource_name_exists(
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    assert [d["name"] for d in context.list_datasources()] == [
        "my_datasource",
    ]
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


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_AthenaCredentialYamlHelper(mock_emit, empty_data_context_stats_enabled):
    helper = AthenaCredentialYamlHelper("my_datasource")
    assert (
        helper.credentials_snippet()
        == '''\
# The SQLAlchemy url/connection string for the Athena connection
# (reference: https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/database/athena or https://github.com/laughingman7743/PyAthena/#sqlalchemy)"""

schema_name = "YOUR_SCHEMA"  # or database name. It is optional
table_name = "YOUR_TABLE_NAME"
region = "YOUR_REGION"
s3_path = "s3://YOUR_S3_BUCKET/path/to/"  # ignore partitioning

connection_string = f"awsathena+rest://@athena.{region}.amazonaws.com/{schema_name}?s3_staging_dir={s3_path}"
            '''
    )

    assert (
        helper.yaml_snippet()
        == '''f"""
name: {datasource_name}
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  connection_string: {connection_string}
data_connectors:
  default_runtime_data_connector_name:
    class_name: RuntimeDataConnector
    batch_identifiers:
      - default_identifier_name
  default_inferred_data_connector_name:
    class_name: InferredAssetSqlDataConnector
    include_schema_name: True
    introspection_directives:
      schema_name: {schema_name}
  default_configured_data_connector_name:
    class_name: ConfiguredAssetSqlDataConnector
    assets:
      {table_name}:
        class_name: Asset
        schema_name: {schema_name}
"""'''
    )

    helper.send_backend_choice_usage_message(empty_data_context_stats_enabled)
    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "cli.new_ds_choice",
                "event_payload": {
                    "type": "sqlalchemy",
                    "db": "Athena",
                    "api_version": "v3",
                },
                "success": True,
            }
        ),
    ]
