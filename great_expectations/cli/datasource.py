import enum
import logging
import os
import platform
import sys
from typing import Optional

import click

from great_expectations import DataContext
from great_expectations.cli import toolkit
from great_expectations.cli.pretty_printing import cli_message, cli_message_dict
from great_expectations.cli.util import verify_library_dependent_modules
from great_expectations.data_context.templates import YAMLToString
from great_expectations.datasource.types import DatasourceTypes
from great_expectations.render.renderer.datasource_new_notebook_renderer import (
    DatasourceNewNotebookRenderer,
)

logger = logging.getLogger(__name__)

try:
    import sqlalchemy
except ImportError:
    logger.debug(
        "Unable to load SqlAlchemy context; install optional sqlalchemy dependency for support"
    )
    sqlalchemy = None

yaml = YAMLToString()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False


class SupportedDatabases(enum.Enum):
    MYSQL = "MySQL"
    POSTGRES = "Postgres"
    REDSHIFT = "Redshift"
    SNOWFLAKE = "Snowflake"
    BIGQUERY = "BigQuery"
    OTHER = "other - Do you have a working SQLAlchemy connection string?"
    # TODO MSSQL


@click.group()
@click.pass_context
def datasource(ctx):
    """Datasource operations"""
    directory: str = toolkit.parse_cli_config_file_location(
        config_file_location=ctx.obj.config_file_location
    ).get("directory")
    context: DataContext = toolkit.load_data_context_with_error_handling(
        directory=directory,
        from_cli_upgrade_command=False,
    )
    # TODO consider moving this all the way up in to the CLIState constructor
    ctx.obj.data_context = context


@datasource.command(name="new")
@click.pass_context
def datasource_new(ctx):
    """Add a new Datasource to the data context."""
    context = ctx.obj.data_context
    toolkit.send_usage_message(
        data_context=context, event="cli.datasource.new", success=True
    )
    _interactive_datasource_new_flow(context)


@datasource.command(name="delete")
@click.argument("datasource")
@click.pass_context
def delete_datasource(ctx, datasource):
    """Delete the datasource specified as an argument"""
    context = ctx.obj.data_context
    if not ctx.obj.assume_yes:
        if not toolkit.confirm_proceed_or_exit(exit_on_no=False):
            cli_message(f"Datasource `{datasource}` was not deleted.")
            sys.exit(0)

    try:
        context.delete_datasource(datasource)
    except ValueError:
        cli_message(f"<red>Datasource {datasource} could not be found.</red>")
        sys.exit(1)
    try:
        context.get_datasource(datasource)
    except ValueError:
        cli_message("<green>{}</green>".format("Datasource deleted successfully."))
        sys.exit(0)


@datasource.command(name="list")
@click.pass_context
def datasource_list(ctx):
    """List known Datasources."""
    context = ctx.obj.data_context
    datasources = context.list_datasources()
    cli_message(_build_datasource_intro_string(datasources))
    for datasource in datasources:
        cli_message("")
        cli_message_dict(
            {
                "name": datasource["name"],
                "class_name": datasource["class_name"],
            }
        )

    toolkit.send_usage_message(
        data_context=context, event="cli.datasource.list", success=True
    )


def _build_datasource_intro_string(datasources):
    datasource_count = len(datasources)
    if datasource_count == 0:
        return "No Datasources found"
    elif datasource_count == 1:
        return "1 Datasource found:"
    return f"{datasource_count} Datasources found:"


def _interactive_datasource_new_flow(context: DataContext) -> None:
    """
    Interactive flow for adding a Datasource to an existing context.
    """
    files_or_sql_selection = click.prompt(
        """
What data would you like Great Expectations to connect to?
    1. Files on a filesystem (for processing with Pandas or Spark)
    2. Relational database (SQL)
""",
        type=click.Choice(["1", "2"]),
        show_choices=False,
    )
    notebook_path: Optional[str] = None
    if files_or_sql_selection == "1":
        execution_engine_selection = click.prompt(
            """
What are you processing your files with?
    1. Pandas
    2. PySpark
""",
            type=click.Choice(["1", "2"]),
            show_choices=False,
        )
        base_path = click.prompt(PROMPT_FILES_BASE_PATH)
        datasource_name = click.prompt(PROMPT_DATASOURCE_NAME)
        if execution_engine_selection == "1":  # pandas
            notebook_path = _create_pandas_datasource_notebook(
                context, datasource_name, base_path
            )
        elif execution_engine_selection == "2":  # Spark
            notebook_path = _create_spark_datasource_notebook(
                context, datasource_name, base_path
            )
    elif files_or_sql_selection == "2":
        notebook_path = _create_sqlalchemy_datasource_notebook(context)

    if notebook_path:
        cli_message(
            """<green>Because you requested to create a new Datasource, we'll open a notebook for you now to complete it!</green>\n\n"""
        )
        toolkit.launch_jupyter_notebook(notebook_path)


def _build_file_like_datasource_config(
    class_name: str, datasource_name: str, base_path: str
) -> dict:
    return {
        "class_name": "Datasource",
        "execution_engine": {"class_name": class_name},
        "data_connectors": {
            f"{datasource_name}_example_data_connector": {
                "class_name": "InferredAssetFilesystemDataConnector",
                "datasource_name": datasource_name,
                "base_directory": base_path,
                "default_regex": {
                    "group_names": "data_asset_name",
                    "pattern": "(.*)",
                },
            }
        },
    }


def _create_pandas_datasource_notebook(
    context, datasource_name: str, base_path: str
) -> str:
    toolkit.send_usage_message(
        data_context=context,
        event="cli.new_ds_choice",
        event_payload={"type": "pandas"},
        success=True,
    )

    pandas_config = _build_file_like_datasource_config(
        "PandasExecutionEngine", datasource_name, base_path
    )
    notebook_path = _build_notebook_from_datasource_config_dict_and_write_to_disk(
        context, DatasourceTypes.PANDAS, datasource_name, pandas_config
    )
    return notebook_path


def _build_notebook_from_datasource_config_dict_and_write_to_disk(
    context: DataContext,
    datasource_type: DatasourceTypes,
    datasource_name: str,
    json_dict: dict,
) -> str:
    renderer = DatasourceNewNotebookRenderer(
        context,
        datasource_name,
        datasource_type=datasource_type,
        datasource_yaml=yaml.dump(json_dict),
    )
    notebook_path = os.path.join(
        context.root_directory,
        context.GE_UNCOMMITTED_DIR,
        f"datasource_new_{datasource_name}.ipynb",
    )
    renderer.render_to_disk(notebook_path)
    return notebook_path


def _create_sqlalchemy_datasource_notebook(context: DataContext) -> Optional[str]:
    if not _verify_sqlalchemy_dependent_modules():
        return None

    selected_database = _prompt_user_for_database_backend()
    toolkit.send_usage_message(
        data_context=context,
        event="cli.new_ds_choice",
        event_payload={"type": "sqlalchemy", "db": selected_database.name},
        success=True,
    )
    datasource_name = _prompt_user_for_db_datasource_name(selected_database)

    datasource_config: dict = {
        "class_name": "SimpleSqlalchemyDatasource",
        "introspection": {"whole_table": {"data_asset_name_suffix": "__whole_table"}},
    }

    if selected_database == SupportedDatabases.MYSQL:
        if not _verify_mysql_dependent_modules():
            return None
        credentials = _collect_mysql_credentials()
        datasource_config["credentials"] = credentials
    elif selected_database == SupportedDatabases.POSTGRES:
        if not _verify_postgresql_dependent_modules():
            return None
        credentials = _collect_postgres_credentials()
        datasource_config["credentials"] = credentials
    elif selected_database == SupportedDatabases.REDSHIFT:
        if not _verify_redshift_dependent_modules():
            return None
        credentials = _collect_redshift_credentials()
        datasource_config["credentials"] = credentials
    elif selected_database == SupportedDatabases.SNOWFLAKE:
        if not _verify_snowflake_dependent_modules():
            return None
        credentials = _collect_snowflake_credentials()
        datasource_config["credentials"] = credentials
    elif selected_database == SupportedDatabases.BIGQUERY:
        if not _verify_bigquery_dependent_modules():
            return None
        datasource_config["connection_string"] = _collect_bigquery_connection_string()
    elif selected_database == SupportedDatabases.OTHER:
        datasource_config["connection_string"] = _collect_sqlalchemy_connection_string()

    notebook_path = _build_notebook_from_datasource_config_dict_and_write_to_disk(
        context, DatasourceTypes.SQL, datasource_name, datasource_config
    )
    return notebook_path


def _prompt_user_for_db_datasource_name(selected_database: SupportedDatabases) -> str:
    default_datasource_name = f"my_{selected_database.value.lower()}_db"
    if selected_database == SupportedDatabases.OTHER:
        default_datasource_name = "my_database"
    datasource_name = click.prompt(
        PROMPT_DATASOURCE_NAME, default=default_datasource_name
    )
    return datasource_name


def _prompt_user_for_database_backend() -> SupportedDatabases:
    enumerated_list = "\n".join(
        [f"    {i}. {db.value}" for i, db in enumerate(SupportedDatabases, 1)]
    )
    msg_prompt_choose_database = f"""
Which database backend are you using?
{enumerated_list}
"""
    db_choices = [str(x) for x in list(range(1, 1 + len(SupportedDatabases)))]
    selected_database_index = (
        int(
            click.prompt(
                msg_prompt_choose_database,
                type=click.Choice(db_choices),
                show_choices=False,
            )
        )
        - 1
    )  # don't show user a zero index list :)
    selected_database = list(SupportedDatabases)[selected_database_index]
    return selected_database


def _should_hide_input():
    """
    This is a workaround to help identify Windows and adjust the prompts accordingly
    since hidden prompts may freeze in certain Windows terminals
    """
    if "windows" in platform.platform().lower():
        return False
    return True


def _collect_postgres_credentials() -> dict:
    credentials = {"drivername": "postgresql"}

    db_hostname = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
    credentials["host"] = click.prompt(
        "What is the host for the postgres connection?",
        default=db_hostname,
    ).strip()
    credentials["port"] = click.prompt(
        "What is the port for the postgres connection?",
        default="5432",
    ).strip()
    credentials["username"] = click.prompt(
        "What is the username for the postgres connection?",
        default="postgres",
    ).strip()
    # This is a minimal workaround we're doing to deal with hidden input problems using Git Bash on Windows
    # TODO: Revisit this if we decide to fully support Windows and identify if there is a better solution
    credentials["password"] = click.prompt(
        "What is the password for the postgres connection?",
        default="",
        show_default=False,
        hide_input=_should_hide_input(),
    )
    credentials["database"] = click.prompt(
        "What is the database name for the postgres connection?",
        default="postgres",
        show_default=True,
    ).strip()

    return credentials


def _collect_snowflake_credentials() -> dict:
    credentials: dict = {"drivername": "snowflake", "query": {}}

    auth_method = click.prompt(
        """What authentication method would you like to use?
    1. User and Password
    2. Single sign-on (SSO)
    3. Key pair authentication
""",
        type=click.Choice(["1", "2", "3"]),
        show_choices=False,
    )

    credentials["username"] = click.prompt(
        "What is the user login name for the snowflake connection?",
    ).strip()

    credentials["host"] = click.prompt(
        "What is the account name for the snowflake connection (include region -- ex "
        "'ABCD.us-east-1')?",
    ).strip()

    database = click.prompt(
        "What is database name for the snowflake connection? (optional -- leave blank for none)",
    ).strip()
    if len(database) > 0:
        credentials["database"] = database

    schema = click.prompt(
        "What is schema name for the snowflake connection? (optional -- leave "
        "blank for none)",
    ).strip()

    if len(schema) > 0:
        credentials["query"]["schema"] = schema
    warehouse = click.prompt(
        "What is warehouse name for the snowflake connection? (optional "
        "-- leave blank for none)",
    ).strip()

    if len(warehouse) > 0:
        credentials["query"]["warehouse"] = warehouse

    role = click.prompt(
        "What is role name for the snowflake connection? (optional -- leave blank for none)",
    ).strip()
    if len(role) > 0:
        credentials["query"]["role"] = role

    if auth_method == "1":
        credentials = {**credentials, **_collect_snowflake_credentials_user_password()}
    elif auth_method == "2":
        credentials = {**credentials, **_collect_snowflake_credentials_sso()}
    elif auth_method == "3":
        credentials = {**credentials, **_collect_snowflake_credentials_key_pair()}
    return credentials


def _collect_snowflake_credentials_user_password():
    credentials = {}

    credentials["password"] = click.prompt(
        "What is the password for the snowflake connection?",
        default="",
        show_default=False,
        hide_input=True,
    )

    return credentials


def _collect_snowflake_credentials_sso():
    credentials = {}

    credentials["connect_args"] = {}
    credentials["connect_args"]["authenticator"] = click.prompt(
        "Valid okta URL or 'externalbrowser' used to connect through SSO",
        default="externalbrowser",
        show_default=False,
    )

    return credentials


def _collect_snowflake_credentials_key_pair():
    credentials = {}

    credentials["private_key_path"] = click.prompt(
        "Path to the private key used for authentication",
        show_default=False,
    )

    credentials["private_key_passphrase"] = click.prompt(
        "Passphrase for the private key used for authentication (optional -- leave blank for none)",
        default="",
        show_default=False,
    )

    return credentials


def _collect_bigquery_connection_string() -> str:
    sqlalchemy_url = click.prompt(
        """What is the SQLAlchemy url/connection string for the BigQuery connection?
(reference: https://github.com/mxmzdlv/pybigquery#connection-string-parameters)
""",
        show_default=False,
    ).strip()
    return sqlalchemy_url


def _collect_sqlalchemy_connection_string() -> str:
    sqlalchemy_url = click.prompt(
        """What is the url/connection string for the sqlalchemy connection?
(reference: https://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls)
""",
        show_default=False,
    ).strip()
    return sqlalchemy_url


def _collect_mysql_credentials() -> dict:
    # We are insisting on pymysql driver when adding a MySQL datasource through the CLI
    # to avoid overcomplication of this flow.
    # If user wants to use another driver, they must create the sqlalchemy connection
    # URL by themselves in config_variables.yml

    credentials = {"drivername": "mysql+pymysql"}

    db_hostname = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
    credentials["host"] = click.prompt(
        "What is the host for the MySQL connection?",
        default=db_hostname,
    ).strip()
    credentials["port"] = click.prompt(
        "What is the port for the MySQL connection?",
        default="3306",
    ).strip()
    credentials["username"] = click.prompt(
        "What is the username for the MySQL connection?",
    ).strip()
    credentials["password"] = click.prompt(
        "What is the password for the MySQL connection?",
        show_default=False,
        hide_input=True,
    )
    credentials["database"] = click.prompt(
        "What is the database name for the MySQL connection?",
    ).strip()

    return credentials


def _collect_redshift_credentials() -> dict:
    # We are insisting on psycopg2 driver when adding a Redshift datasource through the CLI
    # to avoid overcomplication of this flow.
    # If user wants to use another driver, they must create the sqlalchemy connection
    # URL by themselves in config_variables.yml

    # required
    credentials: dict = {
        "drivername": "postgresql+psycopg2",
        "query": {},
        "host": click.prompt(
            "What is the host for the Redshift connection?",
        ).strip(),
        "port": click.prompt(
            "What is the port for the Redshift connection?",
            default="5439",
        ).strip(),
        "username": click.prompt(
            "What is the username for the Redshift connection?",
        ).strip(),
        # This is a minimal workaround we're doing to deal with hidden input
        # problems using Git Bash on Windows
        # TODO: Revisit this if we decide to fully support Windows and identify
        #  if there is a better solution
        "password": click.prompt(
            "What is the password for the Redshift connection?",
            default="",
            show_default=False,
            hide_input=_should_hide_input(),
        ),
        "database": click.prompt(
            "What is the database name for the Redshift connection?",
        ).strip(),
    }

    # optional
    credentials["query"]["sslmode"] = click.prompt(
        "What is sslmode name for the Redshift connection?",
        default="prefer",
    )

    return credentials


def _create_spark_datasource_notebook(
    context: DataContext, datasource_name: str, base_path: str
) -> Optional[str]:
    toolkit.send_usage_message(
        data_context=context,
        event="cli.new_ds_choice",
        event_payload={"type": "spark"},
        success=True,
    )
    if not _verify_pyspark_dependent_modules():
        return None

    spark_config = _build_file_like_datasource_config(
        "SparkDFExecutionEngine", datasource_name, base_path
    )
    notebook_path = _build_notebook_from_datasource_config_dict_and_write_to_disk(
        context, DatasourceTypes.SPARK, datasource_name, spark_config
    )
    return notebook_path


def _verify_sqlalchemy_dependent_modules() -> bool:
    return verify_library_dependent_modules(
        python_import_name="sqlalchemy", pip_library_name="sqlalchemy"
    )


def _verify_mysql_dependent_modules() -> bool:
    return verify_library_dependent_modules(
        python_import_name="pymysql",
        pip_library_name="pymysql",
        module_names_to_reload=CLI_ONLY_SQLALCHEMY_ORDERED_DEPENDENCY_MODULE_NAMES,
    )


def _verify_postgresql_dependent_modules() -> bool:
    psycopg2_success: bool = verify_library_dependent_modules(
        python_import_name="psycopg2",
        pip_library_name="psycopg2-binary",
        module_names_to_reload=CLI_ONLY_SQLALCHEMY_ORDERED_DEPENDENCY_MODULE_NAMES,
    )
    # noinspection SpellCheckingInspection
    postgresql_psycopg2_success: bool = verify_library_dependent_modules(
        python_import_name="sqlalchemy.dialects.postgresql.psycopg2",
        pip_library_name="psycopg2-binary",
        module_names_to_reload=CLI_ONLY_SQLALCHEMY_ORDERED_DEPENDENCY_MODULE_NAMES,
    )
    return psycopg2_success and postgresql_psycopg2_success


def _verify_redshift_dependent_modules() -> bool:
    # noinspection SpellCheckingInspection
    postgresql_success: bool = _verify_postgresql_dependent_modules()
    redshift_success: bool = verify_library_dependent_modules(
        python_import_name="sqlalchemy_redshift.dialect",
        pip_library_name="sqlalchemy-redshift",
        module_names_to_reload=CLI_ONLY_SQLALCHEMY_ORDERED_DEPENDENCY_MODULE_NAMES,
    )
    return redshift_success or postgresql_success


def _verify_snowflake_dependent_modules() -> bool:
    return verify_library_dependent_modules(
        python_import_name="snowflake.sqlalchemy.snowdialect",
        pip_library_name="snowflake-sqlalchemy",
        module_names_to_reload=CLI_ONLY_SQLALCHEMY_ORDERED_DEPENDENCY_MODULE_NAMES,
    )


def _verify_bigquery_dependent_modules() -> bool:
    return verify_library_dependent_modules(
        python_import_name="pybigquery.sqlalchemy_bigquery",
        pip_library_name="pybigquery",
        module_names_to_reload=CLI_ONLY_SQLALCHEMY_ORDERED_DEPENDENCY_MODULE_NAMES,
    )


def _verify_pyspark_dependent_modules() -> bool:
    return verify_library_dependent_modules(
        python_import_name="pyspark", pip_library_name="pyspark"
    )


PROMPT_FILES_BASE_PATH = """
Enter the path (relative or absolute) of the root directory where the data files are stored.
"""

PROMPT_DATASOURCE_NAME = "Give your new Datasource a short name."

CLI_ONLY_SQLALCHEMY_ORDERED_DEPENDENCY_MODULE_NAMES: list = [
    # 'great_expectations.datasource.batch_kwargs_generator.query_batch_kwargs_generator',
    "great_expectations.datasource.batch_kwargs_generator.table_batch_kwargs_generator",
    "great_expectations.dataset.sqlalchemy_dataset",
    "great_expectations.validator.validator",
    "great_expectations.datasource.sqlalchemy_datasource",
]
