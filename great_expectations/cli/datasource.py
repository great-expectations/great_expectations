import enum
import logging
import os
import platform
import sys
import textwrap

import click

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext, rtd_url_ge_version
from great_expectations.cli import toolkit
from great_expectations.cli.pretty_printing import (
    cli_message,
    cli_message_dict,
    display_not_implemented_message_and_exit,
)
from great_expectations.cli.util import verify_library_dependent_modules
from great_expectations.data_context.types.base import DatasourceConfigSchema
from great_expectations.datasource import (
    PandasDatasource,
    SparkDFDatasource,
    SqlAlchemyDatasource,
)
from great_expectations.datasource.batch_kwargs_generator import (
    ManualBatchKwargsGenerator,
)
from great_expectations.exceptions import DatasourceInitializationError

logger = logging.getLogger(__name__)

try:
    import sqlalchemy
except ImportError:
    logger.debug(
        "Unable to load SqlAlchemy context; install optional sqlalchemy dependency for support"
    )
    sqlalchemy = None


class DatasourceTypes(enum.Enum):
    PANDAS = "pandas"
    SQL = "sql"
    SPARK = "spark"
    # TODO DBT = "dbt"


MANUAL_GENERATOR_CLASSES = ManualBatchKwargsGenerator


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
    """Add a new datasource to the data context."""
    display_not_implemented_message_and_exit()
    context = ctx.obj.data_context
    datasource_name, data_source_type = add_datasource(context)

    if datasource_name:
        cli_message(
            "A new datasource '{}' was added to your project.".format(datasource_name)
        )
        toolkit.send_usage_message(
            data_context=context, event="cli.datasource.new", success=True
        )
    else:  # no datasource was created
        toolkit.send_usage_message(
            data_context=context, event="cli.datasource.new", success=False
        )
        sys.exit(1)


@datasource.command(name="delete")
@click.argument("datasource")
@click.pass_context
def delete_datasource(ctx, datasource):
    """Delete the datasource specified as an argument"""
    context = ctx.obj.data_context
    try:
        context.delete_datasource(datasource)
    except ValueError:
        cli_message(
            "<red>{}</red>".format(
                "Datasource {} could not be found.".format(datasource)
            )
        )
        sys.exit(1)
    try:
        context.get_datasource(datasource)
    except ValueError:
        cli_message("<green>{}</green>".format("Datasource deleted successfully."))
        sys.exit(1)
    else:
        cli_message("<red>{}</red>".format("Datasource not deleted."))
        sys.exit(1)


@datasource.command(name="list")
@click.pass_context
def datasource_list(ctx):
    """List known datasources."""
    display_not_implemented_message_and_exit()
    context = ctx.obj.data_context
    datasources = context.list_datasources()
    datasource_count = len(datasources)

    if datasource_count == 0:
        list_intro_string = "No Datasources found"
    else:
        list_intro_string = _build_datasource_intro_string(datasource_count)

    cli_message(list_intro_string)
    for datasource in datasources:
        cli_message("")
        cli_message_dict(datasource)

    toolkit.send_usage_message(
        data_context=context, event="cli.datasource.list", success=True
    )


def _build_datasource_intro_string(datasource_count):
    if datasource_count == 1:
        list_intro_string = "1 Datasource found:"
    if datasource_count > 1:
        list_intro_string = f"{datasource_count} Datasources found:"
    return list_intro_string


def add_datasource(context, choose_one_data_asset=False):
    """
    Interactive flow for adding a datasource to an existing context.

    :param context:
    :param choose_one_data_asset: optional - if True, this signals the method that the intent
            is to let user choose just one data asset (e.g., a file) and there is no need
            to configure a batch kwargs generator that comprehensively scans the datasource for data assets
    :return: a tuple: datasource_name, data_source_type
    """

    msg_prompt_where_is_your_data = """
What data would you like Great Expectations to connect to?
    1. Files on a filesystem (for processing with Pandas or Spark)
    2. Relational database (SQL)
"""

    msg_prompt_files_compute_engine = """
What are you processing your files with?
    1. Pandas
    2. PySpark
"""

    data_source_location_selection = click.prompt(
        msg_prompt_where_is_your_data, type=click.Choice(["1", "2"]), show_choices=False
    )

    datasource_name = None
    data_source_type = None

    if data_source_location_selection == "1":
        data_source_compute_selection = click.prompt(
            msg_prompt_files_compute_engine,
            type=click.Choice(["1", "2"]),
            show_choices=False,
        )

        if data_source_compute_selection == "1":  # pandas

            data_source_type = DatasourceTypes.PANDAS

            datasource_name = _add_pandas_datasource(
                context, passthrough_generator_only=choose_one_data_asset
            )

        elif data_source_compute_selection == "2":  # Spark

            data_source_type = DatasourceTypes.SPARK

            datasource_name = _add_spark_datasource(
                context, passthrough_generator_only=choose_one_data_asset
            )
    else:
        data_source_type = DatasourceTypes.SQL
        datasource_name = _add_sqlalchemy_datasource(context)

    return datasource_name, data_source_type


def _add_pandas_datasource(
    context, passthrough_generator_only=True, prompt_for_datasource_name=True
):
    toolkit.send_usage_message(
        data_context=context,
        event="cli.new_ds_choice",
        event_payload={"type": "pandas"},
        success=True,
    )

    if passthrough_generator_only:
        datasource_name = "files_datasource"
        configuration = PandasDatasource.build_configuration()

    else:
        path = click.prompt(
            msg_prompt_filesys_enter_base_path,
            type=click.Path(exists=True, file_okay=False),
        )

        if path.startswith("./"):
            path = path[2:]

        if path.endswith("/"):
            basenamepath = path[:-1]
        else:
            basenamepath = path

        datasource_name = os.path.basename(basenamepath) + "__dir"
        if prompt_for_datasource_name:
            datasource_name = click.prompt(
                msg_prompt_datasource_name, default=datasource_name
            )

        configuration = PandasDatasource.build_configuration(
            batch_kwargs_generators={
                "subdir_reader": {
                    "class_name": "SubdirReaderBatchKwargsGenerator",
                    "base_directory": os.path.join("..", path),
                }
            }
        )

        configuration["class_name"] = "PandasDatasource"
        configuration["module_name"] = "great_expectations.datasource"
        errors = DatasourceConfigSchema().validate(configuration)
        if len(errors) != 0:
            raise ge_exceptions.GreatExpectationsError(
                "Invalid Datasource configuration: {:s}".format(errors)
            )

    cli_message(
        """
Great Expectations will now add a new Datasource '{:s}' to your deployment, by adding this entry to your great_expectations.yml:

{:s}
""".format(
            datasource_name,
            textwrap.indent(toolkit.yaml.dump({datasource_name: configuration}), "  "),
        )
    )

    toolkit.confirm_proceed_or_exit(
        continuation_message="Okay, exiting now. To learn more about adding datasources, run great_expectations "
        "datasource --help or visit https://docs.greatexpectations.io/"
    )

    context.add_datasource(name=datasource_name, **configuration)
    return datasource_name


def _add_sqlalchemy_datasource(context, prompt_for_datasource_name=True):

    msg_success_database = (
        "\n<green>Great Expectations connected to your database!</green>"
    )

    if not _verify_sqlalchemy_dependent_modules():
        return None

    db_choices = [str(x) for x in list(range(1, 1 + len(SupportedDatabases)))]
    selected_database = (
        int(
            click.prompt(
                msg_prompt_choose_database,
                type=click.Choice(db_choices),
                show_choices=False,
            )
        )
        - 1
    )  # don't show user a zero index list :)

    selected_database = list(SupportedDatabases)[selected_database]

    toolkit.send_usage_message(
        data_context=context,
        event="cli.new_ds_choice",
        event_payload={"type": "sqlalchemy", "db": selected_database.name},
        success=True,
    )

    datasource_name = "my_{}_db".format(selected_database.value.lower())
    if selected_database == SupportedDatabases.OTHER:
        datasource_name = "my_database"
    if prompt_for_datasource_name:
        datasource_name = click.prompt(
            msg_prompt_datasource_name, default=datasource_name
        )

    credentials = {}
    # Since we don't want to save the database credentials in the config file that will be
    # committed in the repo, we will use our Variable Substitution feature to store the credentials
    # in the credentials file (that will not be committed, since it is in the uncommitted directory)
    # with the datasource's name as the variable name.
    # The value of the datasource's "credentials" key in the config file (great_expectations.yml) will
    # be ${datasource name}.
    # Great Expectations will replace the ${datasource name} with the value from the credentials file in runtime.

    while True:
        cli_message(msg_db_config.format(datasource_name))

        if selected_database == SupportedDatabases.MYSQL:
            if not _verify_mysql_dependent_modules():
                return None

            credentials = _collect_mysql_credentials(default_credentials=credentials)
        elif selected_database == SupportedDatabases.POSTGRES:
            if not _verify_postgresql_dependent_modules():
                return None

            credentials = _collect_postgres_credentials(default_credentials=credentials)
        elif selected_database == SupportedDatabases.REDSHIFT:
            if not _verify_redshift_dependent_modules():
                return None

            credentials = _collect_redshift_credentials(default_credentials=credentials)
        elif selected_database == SupportedDatabases.SNOWFLAKE:
            if not _verify_snowflake_dependent_modules():
                return None

            credentials = _collect_snowflake_credentials(
                default_credentials=credentials
            )
        elif selected_database == SupportedDatabases.BIGQUERY:
            if not _verify_bigquery_dependent_modules():
                return None

            credentials = _collect_bigquery_credentials(default_credentials=credentials)
        elif selected_database == SupportedDatabases.OTHER:
            sqlalchemy_url = click.prompt(
                """What is the url/connection string for the sqlalchemy connection?
(reference: https://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls)
""",
                show_default=False,
            ).strip()
            credentials = {"url": sqlalchemy_url}

        context.save_config_variable(datasource_name, credentials)

        message = """
<red>Cannot connect to the database.</red>
  - Please check your environment and the configuration you provided.
  - Database Error: {0:s}"""
        try:
            cli_message(
                "<cyan>Attempting to connect to your database. This may take a moment...</cyan>"
            )

            configuration = SqlAlchemyDatasource.build_configuration(
                credentials="${" + datasource_name + "}"
            )

            configuration["class_name"] = "SqlAlchemyDatasource"
            configuration["module_name"] = "great_expectations.datasource"
            errors = DatasourceConfigSchema().validate(configuration)
            if len(errors) != 0:
                raise ge_exceptions.GreatExpectationsError(
                    "Invalid Datasource configuration: {:s}".format(errors)
                )

            cli_message(
                """
Great Expectations will now add a new Datasource '{0:s}' to your deployment, by adding this entry to your great_expectations.yml:

{1:s}
The credentials will be saved in uncommitted/config_variables.yml under the key '{0:s}'
""".format(
                    datasource_name,
                    textwrap.indent(
                        toolkit.yaml.dump({datasource_name: configuration}), "  "
                    ),
                )
            )

            toolkit.confirm_proceed_or_exit()
            context.add_datasource(name=datasource_name, **configuration)
            cli_message(msg_success_database)
            break
        except ModuleNotFoundError as de:
            cli_message(message.format(str(de)))
            return None

        except DatasourceInitializationError as de:
            cli_message(message.format(str(de)))
            if not click.confirm("Enter the credentials again?", default=True):
                context.add_datasource(
                    datasource_name,
                    initialize=False,
                    module_name="great_expectations.datasource",
                    class_name="SqlAlchemyDatasource",
                    data_asset_type={"class_name": "SqlAlchemyDataset"},
                    credentials="${" + datasource_name + "}",
                )
                # TODO this message about continuing may not be accurate
                cli_message(
                    """
We saved datasource {:s} in {:s} and the credentials you entered in {:s}.
Since we could not connect to the database, you can complete troubleshooting in the configuration files documented in the how-to guides here:
<blue>https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_datasources.html?utm_source=cli&utm_medium=init&utm_campaign={:s}#{:s}</blue> .

After you connect to the datasource, run great_expectations init to continue.

""".format(
                        datasource_name,
                        DataContext.GE_YML,
                        context.get_config()["config_variables_file_path"],
                        rtd_url_ge_version,
                        selected_database.value.lower(),
                    )
                )
                return None

    return datasource_name


def _should_hide_input():
    """
    This is a workaround to help identify Windows and adjust the prompts accordingly
    since hidden prompts may freeze in certain Windows terminals
    """
    if "windows" in platform.platform().lower():
        return False
    return True


def _collect_postgres_credentials(default_credentials=None):
    if default_credentials is None:
        default_credentials = {}

    credentials = {"drivername": "postgresql"}

    db_hostname = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
    credentials["host"] = click.prompt(
        "What is the host for the postgres connection?",
        default=default_credentials.get("host", db_hostname),
    ).strip()
    credentials["port"] = click.prompt(
        "What is the port for the postgres connection?",
        default=default_credentials.get("port", "5432"),
    ).strip()
    credentials["username"] = click.prompt(
        "What is the username for the postgres connection?",
        default=default_credentials.get("username", "postgres"),
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
        default=default_credentials.get("database", "postgres"),
        show_default=True,
    ).strip()

    return credentials


def _collect_snowflake_credentials(default_credentials=None):
    if default_credentials is None:
        default_credentials = {}
    credentials = {"drivername": "snowflake"}

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
        default=default_credentials.get("username", ""),
    ).strip()

    credentials["host"] = click.prompt(
        "What is the account name for the snowflake connection (include region -- ex "
        "'ABCD.us-east-1')?",
        default=default_credentials.get("host", ""),
    ).strip()

    database = click.prompt(
        "What is database name for the snowflake connection? (optional -- leave blank for none)",
        default=default_credentials.get("database", ""),
    ).strip()
    if len(database) > 0:
        credentials["database"] = database

    credentials["query"] = {}
    schema = click.prompt(
        "What is schema name for the snowflake connection? (optional -- leave "
        "blank for none)",
        default=default_credentials.get("schema_name", ""),
    ).strip()

    if len(schema) > 0:
        credentials["query"]["schema"] = schema
    warehouse = click.prompt(
        "What is warehouse name for the snowflake connection? (optional "
        "-- leave blank for none)",
        default=default_credentials.get("warehouse", ""),
    ).strip()

    if len(warehouse) > 0:
        credentials["query"]["warehouse"] = warehouse

    role = click.prompt(
        "What is role name for the snowflake connection? (optional -- leave blank for none)",
        default=default_credentials.get("role", ""),
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


def _collect_bigquery_credentials(default_credentials=None):
    sqlalchemy_url = click.prompt(
        """What is the SQLAlchemy url/connection string for the BigQuery connection?
(reference: https://github.com/mxmzdlv/pybigquery#connection-string-parameters)
""",
        show_default=False,
    ).strip()
    credentials = {"url": sqlalchemy_url}

    return credentials


def _collect_mysql_credentials(default_credentials=None):
    # We are insisting on pymysql driver when adding a MySQL datasource through the CLI
    # to avoid overcomplication of this flow.
    # If user wants to use another driver, they must create the sqlalchemy connection
    # URL by themselves in config_variables.yml
    if default_credentials is None:
        default_credentials = {}

    credentials = {"drivername": "mysql+pymysql"}

    db_hostname = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
    credentials["host"] = click.prompt(
        "What is the host for the MySQL connection?",
        default=default_credentials.get("host", db_hostname),
    ).strip()
    credentials["port"] = click.prompt(
        "What is the port for the MySQL connection?",
        default=default_credentials.get("port", "3306"),
    ).strip()
    credentials["username"] = click.prompt(
        "What is the username for the MySQL connection?",
        default=default_credentials.get("username", ""),
    ).strip()
    credentials["password"] = click.prompt(
        "What is the password for the MySQL connection?",
        default="",
        show_default=False,
        hide_input=True,
    )
    credentials["database"] = click.prompt(
        "What is the database name for the MySQL connection?",
        default=default_credentials.get("database", ""),
    ).strip()

    return credentials


def _collect_redshift_credentials(default_credentials=None):
    # We are insisting on psycopg2 driver when adding a Redshift datasource through the CLI
    # to avoid overcomplication of this flow.
    # If user wants to use another driver, they must create the sqlalchemy connection
    # URL by themselves in config_variables.yml
    if default_credentials is None:
        default_credentials = {}

    credentials = {"drivername": "postgresql+psycopg2"}

    # required

    credentials["host"] = click.prompt(
        "What is the host for the Redshift connection?",
        default=default_credentials.get("host", ""),
    ).strip()
    credentials["port"] = click.prompt(
        "What is the port for the Redshift connection?",
        default=default_credentials.get("port", "5439"),
    ).strip()
    credentials["username"] = click.prompt(
        "What is the username for the Redshift connection?",
        default=default_credentials.get("username", ""),
    ).strip()
    # This is a minimal workaround we're doing to deal with hidden input problems using Git Bash on Windows
    # TODO: Revisit this if we decide to fully support Windows and identify if there is a better solution
    credentials["password"] = click.prompt(
        "What is the password for the Redshift connection?",
        default="",
        show_default=False,
        hide_input=_should_hide_input(),
    )
    credentials["database"] = click.prompt(
        "What is the database name for the Redshift connection?",
        default=default_credentials.get("database", ""),
    ).strip()

    # optional

    credentials["query"] = {}
    credentials["query"]["sslmode"] = click.prompt(
        "What is sslmode name for the Redshift connection?",
        default=default_credentials.get("sslmode", "prefer"),
    )

    return credentials


def _add_spark_datasource(
    context, passthrough_generator_only=True, prompt_for_datasource_name=True
):
    toolkit.send_usage_message(
        data_context=context,
        event="cli.new_ds_choice",
        event_payload={"type": "spark"},
        success=True,
    )

    if not _verify_pyspark_dependent_modules():
        return None

    if passthrough_generator_only:
        datasource_name = "files_spark_datasource"

        # configuration = SparkDFDatasource.build_configuration(batch_kwargs_generators={
        #     "default": {
        #         "class_name": "PassthroughGenerator",
        #     }
        # }
        # )
        configuration = SparkDFDatasource.build_configuration()

    else:
        path = click.prompt(
            msg_prompt_filesys_enter_base_path,
            type=click.Path(exists=True, file_okay=False),
        ).strip()
        if path.startswith("./"):
            path = path[2:]

        if path.endswith("/"):
            basenamepath = path[:-1]
        else:
            basenamepath = path

        datasource_name = os.path.basename(basenamepath) + "__dir"
        if prompt_for_datasource_name:
            datasource_name = click.prompt(
                msg_prompt_datasource_name, default=datasource_name
            )

        configuration = SparkDFDatasource.build_configuration(
            batch_kwargs_generators={
                "subdir_reader": {
                    "class_name": "SubdirReaderBatchKwargsGenerator",
                    "base_directory": os.path.join("..", path),
                }
            }
        )
        configuration["class_name"] = "SparkDFDatasource"
        configuration["module_name"] = "great_expectations.datasource"
        errors = DatasourceConfigSchema().validate(configuration)
        if len(errors) != 0:
            raise ge_exceptions.GreatExpectationsError(
                "Invalid Datasource configuration: {:s}".format(errors)
            )

    cli_message(
        """
Great Expectations will now add a new Datasource '{:s}' to your deployment, by adding this entry to your great_expectations.yml:

{:s}
""".format(
            datasource_name,
            textwrap.indent(toolkit.yaml.dump({datasource_name: configuration}), "  "),
        )
    )
    toolkit.confirm_proceed_or_exit()

    context.add_datasource(name=datasource_name, **configuration)
    return datasource_name


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


def skip_prompt_message(skip_flag, prompt_message_text) -> bool:

    if not skip_flag:
        return click.confirm(prompt_message_text, default=True)

    return skip_flag


msg_prompt_choose_datasource = """Configure a datasource:
    1. Pandas DataFrame
    2. Relational database (SQL)
    3. Spark DataFrame
    4. Skip datasource configuration
"""

msg_prompt_choose_database = """
Which database backend are you using?
{}
""".format(
    "\n".join(
        ["    {}. {}".format(i, db.value) for i, db in enumerate(SupportedDatabases, 1)]
    )
)

msg_prompt_filesys_enter_base_path = """
Enter the path (relative or absolute) of the root directory where the data files are stored.
"""

msg_prompt_datasource_name = """
Give your new Datasource a short name.
"""

msg_db_config = """
Next, we will configure database credentials and store them in the `{0:s}` section
of this config file: great_expectations/uncommitted/config_variables.yml:
"""

msg_unknown_data_source = """
Do we not have the type of data source you want?
    - Please create a GitHub issue here so we can discuss it!
    - <blue>https://github.com/great-expectations/great_expectations/issues/new</blue>"""

CLI_ONLY_SQLALCHEMY_ORDERED_DEPENDENCY_MODULE_NAMES: list = [
    # 'great_expectations.datasource.batch_kwargs_generator.query_batch_kwargs_generator',
    "great_expectations.datasource.batch_kwargs_generator.table_batch_kwargs_generator",
    "great_expectations.dataset.sqlalchemy_dataset",
    "great_expectations.validator.validator",
    "great_expectations.datasource.sqlalchemy_datasource",
]
