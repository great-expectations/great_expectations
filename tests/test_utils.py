import logging
import os
import uuid
import warnings
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union, cast

import pandas as pd

import great_expectations.exceptions as gx_exceptions
from great_expectations.alias_types import PathStr
from great_expectations.compatibility import sqlalchemy
from great_expectations.compatibility.sqlalchemy import Engine, inspect
from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.compatibility.sqlalchemy_compatibility_wrappers import (
    add_dataframe_to_db,
)
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.data_context.abstract_data_context import AbstractDataContext
from great_expectations.data_context.store import (
    CheckpointStore,
    ConfigurationStore,
    Store,
    StoreBackend,
)
from great_expectations.data_context.types.base import BaseYamlConfig
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
)
from great_expectations.datasource.fluent.sql_datasource import SQLDatasource
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect

logger = logging.getLogger(__name__)
yaml_handler = YAMLHandler()

SQLAlchemyError = sqlalchemy.SQLAlchemyError


def safe_remove(path):
    if path is not None:
        try:
            os.remove(path)  # noqa: PTH107
        except OSError as e:
            print(e)


def create_files_in_directory(
    directory: str, file_name_list: List[str], file_content_fn=lambda: "x,y\n1,2\n2,3"
):
    subdirectories = []
    for file_name in file_name_list:
        splits = file_name.split("/")
        for i in range(1, len(splits)):
            subdirectories.append(os.path.join(*splits[:i]))  # noqa: PTH118
    subdirectories = set(subdirectories)

    for subdirectory in subdirectories:
        os.makedirs(  # noqa: PTH103
            os.path.join(directory, subdirectory),  # noqa: PTH118
            exist_ok=True,
        )

    for file_name in file_name_list:
        file_path = os.path.join(directory, file_name)  # noqa: PTH118
        with open(file_path, "w") as f_:
            f_.write(file_content_fn())


def validate_uuid4(uuid_string: str) -> bool:
    """
    Validate that a UUID string is in fact a valid uuid4.
    Happily, the uuid module does the actual checking for us.
    It is vital that the 'version' kwarg be passed
    to the UUID() call, otherwise any 32-character
    hex string is considered valid.
    From https://gist.github.com/ShawnMilo/7777304

    Args:
        uuid_string: string to check whether it is a valid UUID or not

    Returns:
        True if uuid_string is a valid UUID or False if not
    """
    try:
        val = uuid.UUID(uuid_string, version=4)
    except ValueError:
        # If it's a value error, then the string
        # is not a valid hex code for a UUID.
        return False

    # If the uuid_string is a valid hex code,
    # but an invalid uuid4,
    # the UUID.__init__ will convert it to a
    # valid uuid4. This is bad for validation purposes.

    return val.hex == uuid_string.replace("-", "")


def get_sqlite_temp_table_names(execution_engine: SqlAlchemyExecutionEngine):
    statement = sa.text("SELECT name FROM sqlite_temp_master")

    rows = execution_engine.execute_query(statement).fetchall()
    return {row[0] for row in rows}


def get_sqlite_table_names(execution_engine: SqlAlchemyExecutionEngine):
    statement = sa.text("SELECT name FROM sqlite_master")

    rows = execution_engine.execute_query(statement).fetchall()

    return {row[0] for row in rows}


def get_sqlite_temp_table_names_from_engine(engine: Engine):
    statement = sa.text("SELECT name FROM sqlite_temp_master")

    with engine.connect() as connection:
        rows = connection.execute(statement).fetchall()
    return {row[0] for row in rows}


def build_tuple_filesystem_store_backend(
    base_directory: str,
    *,
    module_name: str = "great_expectations.data_context.store",
    class_name: str = "TupleFilesystemStoreBackend",
    **kwargs,
) -> StoreBackend:
    logger.debug(
        f"""Starting data_context/store/util.py#build_tuple_filesystem_store_backend using base_directory:
"{base_directory}"""  # noqa: E501
    )
    store_backend_config: dict = {
        "module_name": module_name,
        "class_name": class_name,
        "base_directory": base_directory,
    }
    store_backend_config.update(**kwargs)
    return Store.build_store_from_config(
        config=store_backend_config,
        module_name=module_name,
        runtime_environment=None,
    )


def save_config_to_filesystem(
    configuration_store_class_name: str,
    configuration_store_module_name: str,
    store_name: str,
    base_directory: str,
    configuration_key: str,
    configuration: BaseYamlConfig,
):
    store_config: dict = {"base_directory": base_directory}
    store_backend_obj: StoreBackend = build_tuple_filesystem_store_backend(**store_config)
    save_config_to_store_backend(
        class_name=configuration_store_class_name,
        module_name=configuration_store_module_name,
        store_name=store_name,
        store_backend=store_backend_obj,
        configuration_key=configuration_key,
        configuration=configuration,
    )


def load_config_from_filesystem(
    configuration_store_class_name: str,
    configuration_store_module_name: str,
    store_name: str,
    base_directory: str,
    configuration_key: str,
) -> BaseYamlConfig:
    store_config: dict = {"base_directory": base_directory}
    store_backend_obj: StoreBackend = build_tuple_filesystem_store_backend(**store_config)
    return load_config_from_store_backend(
        class_name=configuration_store_class_name,
        module_name=configuration_store_module_name,
        store_name=store_name,
        store_backend=store_backend_obj,
        configuration_key=configuration_key,
    )


def build_configuration_store(
    class_name: str,
    store_name: str,
    store_backend: Union[StoreBackend, dict],
    *,
    module_name: str = "great_expectations.data_context.store",
    overwrite_existing: bool = False,
    **kwargs,
) -> ConfigurationStore:
    logger.debug(
        f"Starting data_context/store/util.py#build_configuration_store for store_name {store_name}"
    )

    if store_backend is not None and isinstance(store_backend, StoreBackend):
        store_backend = store_backend.config
    elif not isinstance(store_backend, dict):
        raise gx_exceptions.DataContextError(
            "Invalid configuration: A store_backend needs to be a dictionary or inherit from the StoreBackend class."  # noqa: E501
        )

    store_backend.update(**kwargs)

    store_config: dict = {
        "store_name": store_name,
        "module_name": module_name,
        "class_name": class_name,
        "overwrite_existing": overwrite_existing,
        "store_backend": store_backend,
    }
    configuration_store: ConfigurationStore = Store.build_store_from_config(  # type: ignore[assignment]
        config=store_config,
        module_name=module_name,
        runtime_environment=None,
    )
    return configuration_store


def delete_config_from_store_backend(
    class_name: str,
    module_name: str,
    store_name: str,
    store_backend: Union[StoreBackend, dict],
    configuration_key: str,
) -> None:
    config_store: ConfigurationStore = build_configuration_store(
        class_name=class_name,
        module_name=module_name,
        store_name=store_name,
        store_backend=store_backend,
        overwrite_existing=True,
    )
    key = ConfigurationIdentifier(
        configuration_key=configuration_key,
    )
    config_store.remove_key(key=key)


def delete_checkpoint_config_from_store_backend(
    store_name: str,
    store_backend: Union[StoreBackend, dict],
    checkpoint_name: str,
) -> None:
    config_store: CheckpointStore = build_checkpoint_store_using_store_backend(
        store_name=store_name,
        store_backend=store_backend,
    )
    key = ConfigurationIdentifier(
        configuration_key=checkpoint_name,
    )
    config_store.remove_key(key=key)


def build_checkpoint_store_using_store_backend(
    store_name: str,
    store_backend: Union[StoreBackend, dict],
    overwrite_existing: bool = False,
) -> CheckpointStore:
    return cast(
        CheckpointStore,
        build_configuration_store(
            class_name="CheckpointStore",
            module_name="great_expectations.data_context.store",
            store_name=store_name,
            store_backend=store_backend,
            overwrite_existing=overwrite_existing,
        ),
    )


def save_config_to_store_backend(
    class_name: str,
    module_name: str,
    store_name: str,
    store_backend: Union[StoreBackend, dict],
    configuration_key: str,
    configuration: BaseYamlConfig,
) -> None:
    config_store: ConfigurationStore = build_configuration_store(
        class_name=class_name,
        module_name=module_name,
        store_name=store_name,
        store_backend=store_backend,
        overwrite_existing=True,
    )
    key = ConfigurationIdentifier(
        configuration_key=configuration_key,
    )
    config_store.set(key=key, value=configuration)


def load_config_from_store_backend(
    class_name: str,
    module_name: str,
    store_name: str,
    store_backend: Union[StoreBackend, dict],
    configuration_key: str,
) -> BaseYamlConfig:
    config_store: ConfigurationStore = build_configuration_store(
        class_name=class_name,
        module_name=module_name,
        store_name=store_name,
        store_backend=store_backend,
        overwrite_existing=False,
    )
    key = ConfigurationIdentifier(
        configuration_key=configuration_key,
    )
    return config_store.get(key=key)  # type: ignore[return-value]


def delete_config_from_filesystem(
    configuration_store_class_name: str,
    configuration_store_module_name: str,
    store_name: str,
    base_directory: str,
    configuration_key: str,
):
    store_config: dict = {"base_directory": base_directory}
    store_backend_obj: StoreBackend = build_tuple_filesystem_store_backend(**store_config)
    delete_config_from_store_backend(
        class_name=configuration_store_class_name,
        module_name=configuration_store_module_name,
        store_name=store_name,
        store_backend=store_backend_obj,
        configuration_key=configuration_key,
    )


# noinspection PyPep8Naming
def get_snowflake_connection_url() -> str:
    """Get snowflake connection url from environment variables.

    Returns:
        String of the snowflake connection URL.
    """
    sfUser = os.environ.get("SNOWFLAKE_USER")
    sfPswd = os.environ.get("SNOWFLAKE_PW")
    sfAccount = os.environ.get("SNOWFLAKE_ACCOUNT")
    sfDatabase = os.environ.get("SNOWFLAKE_DATABASE")
    sfSchema = os.environ.get("SNOWFLAKE_SCHEMA")
    sfWarehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")
    sfRole = os.environ.get("SNOWFLAKE_ROLE") or "PUBLIC"

    return f"snowflake://{sfUser}:{sfPswd}@{sfAccount}/{sfDatabase}/{sfSchema}?warehouse={sfWarehouse}&role={sfRole}"


def get_redshift_connection_url() -> str:
    """Get Amazon Redshift connection url from environment variables.

    Returns:
        String of the Amazon Redshift connection URL.
    """
    host = os.environ.get("REDSHIFT_HOST")
    port = os.environ.get("REDSHIFT_PORT")
    user = os.environ.get("REDSHIFT_USERNAME")
    pswd = os.environ.get("REDSHIFT_PASSWORD")
    db = os.environ.get("REDSHIFT_DATABASE")
    ssl = os.environ.get("REDSHIFT_SSLMODE")

    return f"redshift+psycopg2://{user}:{pswd}@{host}:{port}/{db}?sslmode={ssl}"


def get_bigquery_table_prefix() -> str:
    """Get table_prefix that will be used by the BigQuery client in loading a BigQuery table from DataFrame.

    Reference link
        https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe

    Returns:
        String of table prefix, which is the gcp_project and dataset concatenated by a "."
    """  # noqa: E501
    gcp_project = os.environ.get("GE_TEST_GCP_PROJECT")
    if not gcp_project:
        raise ValueError(
            "Environment Variable GE_TEST_GCP_PROJECT is required to run BigQuery integration tests"
        )
    bigquery_dataset = os.environ.get("GE_TEST_BIGQUERY_DATASET", "test_ci")
    return f"""{gcp_project}.{bigquery_dataset}"""


def get_bigquery_connection_url() -> str:
    """Get bigquery connection url from environment variables.

    Note: dataset defaults to "demo" if not set.

    Returns:
        String of the bigquery connection url.
    """
    gcp_project = os.environ.get("GE_TEST_GCP_PROJECT")
    if not gcp_project:
        raise ValueError(
            "Environment Variable GE_TEST_GCP_PROJECT is required to run BigQuery integration tests"
        )
    bigquery_dataset = os.environ.get("GE_TEST_BIGQUERY_DATASET", "demo")

    return f"bigquery://{gcp_project}/{bigquery_dataset}"


@dataclass
class LoadedTable:
    """Output of loading a table via load_data_into_test_database."""

    table_name: str
    inserted_dataframe: pd.DataFrame


def load_and_concatenate_csvs(
    csv_paths: List[str],
    load_full_dataset: bool = False,
    convert_column_names_to_datetime: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Utility method that is used in loading test data into a pandas dataframe.

    It includes several parameters used to describe the output data.

    Args:
        csv_paths: list of paths of csvs to write, can be a single path. These should all be the same shape.
        load_full_dataset: if False, load only the first 10 rows.
        convert_column_names_to_datetime: List of column names to convert to datetime before writing to db.

    Returns:
        A pandas dataframe concatenating data loaded from all csvs.
    """  # noqa: E501

    if convert_column_names_to_datetime is None:
        convert_column_names_to_datetime = []

    import pandas as pd

    dfs: List[pd.DataFrame] = []
    for csv_path in csv_paths:
        df = pd.read_csv(csv_path)
        convert_string_columns_to_datetime(
            df=df, column_names_to_convert=convert_column_names_to_datetime
        )
        if not load_full_dataset:
            # Improving test performance by only loading the first 10 rows of our test data into the db  # noqa: E501
            df = df.head(10)

        dfs.append(df)

    all_dfs_concatenated: pd.DataFrame = pd.concat(dfs)

    return all_dfs_concatenated


def convert_string_columns_to_datetime(
    df: pd.DataFrame, column_names_to_convert: Optional[List[str]] = None
) -> None:
    """
    Converts specified columns (e.g., "pickup_datetime" and "dropoff_datetime") to datetime column type.
    Side-effect: Passed DataFrame is modified (in-place).
    """  # noqa: E501
    if column_names_to_convert is None:
        column_names_to_convert = []

    column_name_to_convert: str
    for column_name_to_convert in column_names_to_convert:
        df[column_name_to_convert] = pd.to_datetime(df[column_name_to_convert])


def load_data_into_test_database(  # noqa: C901, PLR0912, PLR0915
    table_name: str,
    connection_string: str,
    schema_name: Optional[str] = None,
    csv_path: Optional[str] = None,
    csv_paths: Optional[List[str]] = None,
    load_full_dataset: bool = False,
    convert_colnames_to_datetime: Optional[List[str]] = None,
    random_table_suffix: bool = False,
    to_sql_method: Optional[str] = None,
    drop_existing_table: bool = True,
) -> LoadedTable:
    """Utility method that is used in loading test data into databases that can be accessed through SqlAlchemy.

    This includes local Dockerized DBs like postgres, but also cloud-dbs like BigQuery and Redshift.

    Args:
        table_name: name of the table to write to.
        connection_string: used to connect to the database.
        schema_name: optional name of schema
        csv_path: path of a single csv to write.
        csv_paths: list of paths of csvs to write.
        load_full_dataset: if False, load only the first 10 rows.
        convert_colnames_to_datetime: List of column names to convert to datetime before writing to db.
        random_table_suffix: If true, add 8 random characters to the table suffix and remove other tables with the
            same prefix.
        to_sql_method: Method to pass to method param of pd.to_sql()
        drop_existing_table: boolean value. If set to false, will append to existing table
    Returns:
        LoadedTable which for convenience, contains the pandas dataframe that was used to load the data.
    """  # noqa: E501
    if csv_path and csv_paths:
        csv_paths.append(csv_path)
    elif csv_path and not csv_paths:
        csv_paths = [csv_path]

    all_dfs_concatenated: pd.DataFrame = load_and_concatenate_csvs(
        csv_paths, load_full_dataset, convert_colnames_to_datetime
    )

    if random_table_suffix:
        table_name: str = f"{table_name}_{str(uuid.uuid4())[:8]}"

    return_value: LoadedTable = LoadedTable(
        table_name=table_name, inserted_dataframe=all_dfs_concatenated
    )
    connection = None
    if sa:
        engine = sa.create_engine(connection_string)
    else:
        logger.debug(
            "Attempting to load data in to tests SqlAlchemy database, but unable to load SqlAlchemy context; "  # noqa: E501
            "install optional sqlalchemy dependency for support."
        )
        return return_value

    if engine.dialect.name.lower().startswith("mysql"):
        # Don't attempt to DROP TABLE IF EXISTS on a table that doesn't exist in mysql because it will error  # noqa: E501
        inspector = inspect(engine)
        db_name = connection_string.split("/")[-1]
        table_names = [name for name in inspector.get_table_names(schema=db_name)]
        drop_existing_table = table_name in table_names

    if engine.dialect.name.lower() == "bigquery":
        # bigquery is handled in a special way
        load_data_into_test_bigquery_database_with_bigquery_client(
            dataframe=all_dfs_concatenated, table_name=table_name
        )
        return return_value
    elif engine.dialect.name.lower() == "awsathena":
        try:
            connection = engine.connect()
            load_dataframe_into_test_athena_database_as_table(
                df=all_dfs_concatenated,
                table_name=table_name,
                connection=connection,
            )
            return return_value
        except SQLAlchemyError:
            error_message: str = """Docs integration tests encountered an error while loading test-data into test-database."""  # noqa: E501
            logger.error(error_message)  # noqa: TRY400
            raise gx_exceptions.DatabaseConnectionError(error_message)
            # Normally we would call `raise` to re-raise the SqlAlchemyError but we don't to make sure that  # noqa: E501
            # sensitive information does not make it into our CI logs.
        finally:
            connection.close()
            engine.dispose()
    else:
        try:
            if drop_existing_table:
                print(f"Dropping table {table_name}")
                with engine.begin() as connection:
                    connection.execute(sa.text(f"DROP TABLE IF EXISTS {table_name}"))
                print(f"Creating table {table_name} and adding data from {csv_paths}")
            else:
                print(f"Adding to existing table {table_name} and adding data from {csv_paths}")

            with engine.connect() as connection:
                add_dataframe_to_db(
                    df=all_dfs_concatenated,
                    name=table_name,
                    con=connection,
                    schema=schema_name,
                    index=False,
                    if_exists="append",
                    method=to_sql_method,
                )
            return return_value
        except SQLAlchemyError:
            error_message: str = """Docs integration tests encountered an error while loading test-data into test-database."""  # noqa: E501
            logger.error(error_message)  # noqa: TRY400
            raise gx_exceptions.DatabaseConnectionError(error_message)
            # Normally we would call `raise` to re-raise the SqlAlchemyError but we don't to make sure that  # noqa: E501
            # sensitive information does not make it into our CI logs.
        finally:
            if connection:
                connection.close()
            if engine:
                engine.dispose()


def load_data_into_test_bigquery_database_with_bigquery_client(
    dataframe: pd.DataFrame, table_name: str
) -> None:
    """
    Loads dataframe into bigquery table using BigQuery client. Follows pattern specified in the GCP documentation here:
        - https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe
    Args:
        dataframe (pd.DataFrame): DataFrame to load
        table_name (str): table to load DataFrame to. Prefix containing project and dataset are loaded
                        by helper function.
    """  # noqa: E501
    prefix: str = get_bigquery_table_prefix()
    table_id: str = f"""{prefix}.{table_name}"""
    from google.cloud import bigquery

    gcp_project: Optional[str] = os.environ.get("GE_TEST_GCP_PROJECT")
    if not gcp_project:
        raise ValueError(
            "Environment Variable GE_TEST_GCP_PROJECT is required to run BigQuery integration tests"
        )
    client: bigquery.Client = bigquery.Client(project=gcp_project)
    job: bigquery.LoadJob = client.load_table_from_dataframe(
        dataframe, table_id
    )  # Make an API request.
    load_job = job.result()  # Wait for the job to complete
    if load_job.errors or load_job.error_result:
        raise gx_exceptions.DatabaseConnectionError(
            "Unable to load data into BigQuery. Load job errors: "
            + str(load_job.errors)
            + " Load job error result: "
            + str(load_job.error_result)
        )


def load_dataframe_into_test_athena_database_as_table(
    df: pd.DataFrame,
    table_name: str,
    connection,
    data_location_bucket: Optional[str] = None,
    data_location: Optional[str] = None,
) -> None:
    """

    Args:
        df: dataframe containing data.
        table_name: name of table to write.
        connection: connection to database.
        data_location_bucket: name of bucket where data is located.
        data_location: path to data from bucket without leading / e.g.
            "data/stuff/" in path "s3://my-bucket/data/stuff/"

    Returns:
        None
    """

    from pyathena.pandas.util import to_sql

    if not data_location_bucket:
        data_location_bucket = os.getenv("ATHENA_DATA_BUCKET")
    if not data_location:
        data_location = "data/ten_trips_from_each_month/"
    location: str = f"s3://{data_location_bucket}/{data_location}"
    to_sql(
        df=df,
        name=table_name,
        conn=connection,
        location=location,
        if_exists="replace",
    )


def clean_up_tables_with_prefix(connection_string: str, table_prefix: str) -> List[str]:
    """Drop all tables starting with the provided table_prefix.
    Note: Uses private method InferredAssetSqlDataConnector._introspect_db()
    to get the table names to not duplicate code, but should be refactored in the
    future to not use a private method.

    Args:
        connection_string: To connect to the database.
        table_prefix: First characters of the tables you want to remove.

    Returns:
        List of deleted tables.
    """
    execution_engine: SqlAlchemyExecutionEngine = SqlAlchemyExecutionEngine(
        connection_string=connection_string
    )
    introspection_output = introspect_db(execution_engine=execution_engine)

    tables_to_drop: List[str] = []
    tables_dropped: List[str] = []

    for table in introspection_output:
        if table["table_name"].startswith(table_prefix):
            tables_to_drop.append(table["table_name"])

    for table_name in tables_to_drop:
        print(f"Dropping table {table_name}")
        execution_engine.execute_query_in_transaction(sa.text(f"DROP TABLE IF EXISTS {table_name}"))
        tables_dropped.append(table_name)

    tables_skipped: List[str] = list(set(tables_to_drop) - set(tables_dropped))
    if len(tables_skipped) > 0:
        warnings.warn(f"Warning: Tables skipped: {tables_skipped}")

    return tables_dropped


def introspect_db(  # noqa: C901, PLR0912
    execution_engine: SqlAlchemyExecutionEngine,
    schema_name: Union[str, None] = None,
    ignore_information_schemas_and_system_tables: bool = True,
    information_schemas: Optional[List[str]] = None,
    system_tables: Optional[List[str]] = None,
    include_views=True,
) -> List[Dict[str, str]]:
    # This code was broken out from the InferredAssetSqlDataConnector when it was removed
    if information_schemas is None:
        information_schemas = [
            "INFORMATION_SCHEMA",  # snowflake, mssql, mysql, oracle
            "information_schema",  # postgres, redshift, mysql
            "performance_schema",  # mysql
            "sys",  # mysql
            "mysql",  # mysql
        ]

    if system_tables is None:
        system_tables = ["sqlite_master"]  # sqlite

    engine: sqlalchemy.Engine = execution_engine.engine
    inspector: sqlalchemy.Inspector = sa.inspect(engine)

    selected_schema_name = schema_name

    tables: List[Dict[str, str]] = []
    all_schema_names: List[str] = inspector.get_schema_names()
    for schema in all_schema_names:
        if ignore_information_schemas_and_system_tables and schema_name in information_schemas:
            continue

        if selected_schema_name is not None and schema_name != selected_schema_name:
            continue

        table_names: List[str] = inspector.get_table_names(schema=schema)
        for table_name in table_names:
            if ignore_information_schemas_and_system_tables and (table_name in system_tables):
                continue

            tables.append(
                {
                    "schema_name": schema,
                    "table_name": table_name,
                    "type": "table",
                }
            )

        # Note Abe 20201112: This logic is currently untested.
        if include_views:
            # Note: this is not implemented for bigquery
            try:
                view_names = inspector.get_view_names(schema=schema)
            except NotImplementedError:
                # Not implemented by Athena dialect
                pass
            else:
                for view_name in view_names:
                    if ignore_information_schemas_and_system_tables and (
                        view_name in system_tables
                    ):
                        continue

                    tables.append(
                        {
                            "schema_name": schema,
                            "table_name": view_name,
                            "type": "view",
                        }
                    )

    # SQLAlchemy's introspection does not list "external tables" in Redshift Spectrum (tables whose data is stored on S3).  # noqa: E501
    # The following code fetches the names of external schemas and tables from a special table
    # 'svv_external_tables'.
    try:
        if engine.dialect.name.lower() == GXSqlDialect.REDSHIFT:
            # noinspection SqlDialectInspection,SqlNoDataSourceInspection
            result = execution_engine.execute_query(
                sa.text("select schemaname, tablename from svv_external_tables")
            ).fetchall()
            for row in result:
                tables.append(
                    {
                        "schema_name": row[0],
                        "table_name": row[1],
                        "type": "table",
                    }
                )
    except Exception as e:
        # Our testing shows that 'svv_external_tables' table is present in all Redshift clusters. This means that this  # noqa: E501
        # exception is highly unlikely to fire.
        if "UndefinedTable" not in str(e):
            raise e  # noqa: TRY201

    return tables


def check_athena_table_count(
    connection_string: str, db_name: str, expected_table_count: int
) -> bool:
    """
    Helper function used by awsathena integration test. Checks whether expected number of tables exist in database
    """  # noqa: E501
    if sa:
        engine = sa.create_engine(connection_string)
    else:
        logger.debug(
            "Attempting to perform test on AWSAthena database, but unable to load SqlAlchemy context; "  # noqa: E501
            "install optional sqlalchemy dependency for support."
        )
        return False

    connection = None
    try:
        connection = engine.connect()
        result = connection.execute(sa.text(f"SHOW TABLES in {db_name}")).fetchall()
        return len(result) == expected_table_count
    except SQLAlchemyError:
        error_message: str = """Docs integration tests encountered an error while loading test-data into test-database."""  # noqa: E501
        logger.error(error_message)  # noqa: TRY400
        raise gx_exceptions.DatabaseConnectionError(error_message)
        # Normally we would call `raise` to re-raise the SqlAlchemyError but we don't to make sure that  # noqa: E501
        # sensitive information does not make it into our CI logs.
    finally:
        connection.close()
        engine.dispose()


def clean_athena_db(connection_string: str, db_name: str, table_to_keep: str) -> None:
    """
    Helper function used by awsathena integration test. Cleans up "temp" tables that were created.
    """
    if sa:
        engine = sa.create_engine(connection_string)
    else:
        logger.debug(
            "Attempting to perform test on AWSAthena database, but unable to load SqlAlchemy context; "  # noqa: E501
            "install optional sqlalchemy dependency for support."
        )
        return

    connection = None
    try:
        connection = engine.connect()
        result = connection.execute(sa.text(f"SHOW TABLES in {db_name}")).fetchall()
        for table_tuple in result:
            table = table_tuple[0]
            if table != table_to_keep:
                connection.execute(sa.text(f"DROP TABLE `{table}`;"))
    finally:
        connection.close()
        engine.dispose()


def get_default_postgres_url() -> str:
    """Get connection string to postgres container
    Returns:
        String of default connection to Docker container
    """
    return "postgresql+psycopg2://postgres:@localhost/test_ci"


def get_default_mysql_url() -> str:
    """Get connection string to mysql container
    Returns:
        String of default connection to Docker container
    """
    return "mysql+pymysql://root@127.0.0.1/test_ci"


def get_default_trino_url() -> str:
    """Get connection string to trino container
    Returns:
        String of default connection to Docker container
    """
    return "trino://test@localhost:8088/memory/schema"


def get_default_mssql_url() -> str:
    """Get connection string to mssql container
    Returns:
        String of default connection to Docker container
    """
    db_hostname = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
    connection_string = f"mssql+pyodbc://sa:ReallyStrongPwd1234%^&*@{db_hostname}:1433/test_ci?driver=ODBC Driver 17 for SQL Server&charset=utf8&autocommit=true"  # noqa: E501
    return connection_string


def get_awsathena_db_name(db_name_env_var: str = "ATHENA_DB_NAME") -> str:
    """Get awsathena database name from environment variables.

    Returns:
        String of the awsathena database name.
    """
    athena_db_name: str = os.getenv(db_name_env_var)
    if not athena_db_name:
        raise ValueError(
            f"Environment Variable {db_name_env_var} is required to run integration tests against AWS Athena"  # noqa: E501
        )
    return athena_db_name


def get_awsathena_connection_url(db_name_env_var: str = "ATHENA_DB_NAME") -> str:
    """Get awsathena connection url from environment variables.

    Returns:
        String of the awsathena connection url.
    """
    ATHENA_DB_NAME: str = get_awsathena_db_name(db_name_env_var)
    ATHENA_STAGING_S3: Optional[str] = os.getenv("ATHENA_STAGING_S3")
    if not ATHENA_STAGING_S3:
        raise ValueError(
            "Environment Variable ATHENA_STAGING_S3 is required to run integration tests against AWS Athena"  # noqa: E501
        )

    return f"awsathena+rest://@athena.us-east-1.amazonaws.com/{ATHENA_DB_NAME}?s3_staging_dir={ATHENA_STAGING_S3}"


def get_connection_string_and_dialect(
    athena_db_name_env_var: str = "ATHENA_DB_NAME",
) -> Tuple[str, str]:
    with open("./connection_string.yml") as f:
        db_config: dict = yaml_handler.load(f)

    dialect: str = db_config["dialect"]
    if dialect == "snowflake":
        connection_string: str = get_snowflake_connection_url()
    elif dialect == "redshift":
        connection_string: str = get_redshift_connection_url()
    elif dialect == "bigquery":
        connection_string: str = get_bigquery_connection_url()
    elif dialect == "awsathena":
        connection_string: str = get_awsathena_connection_url(athena_db_name_env_var)
    else:
        connection_string: str = db_config["connection_string"]

    return dialect, connection_string


def add_datasource(
    context: AbstractDataContext, *, name: str, connection_string: str
) -> SQLDatasource:
    """Add a datasource to the context based on the dialect from config file.

    Needed because context.data_sources.add_sql is prohibitted when
    more specific methods are available.
    """
    with open("./connection_string.yml") as f:
        db_config: dict = yaml_handler.load(f)

    dialect: str = db_config["dialect"]
    if dialect == "snowflake":
        return context.data_sources.add_snowflake(name=name, connection_string=connection_string)
    elif dialect == "postgres":
        return context.data_sources.add_postgres(name=name, connection_string=connection_string)
    else:
        return context.data_sources.add_sql(name=name, connection_string=connection_string)


@contextmanager
def working_directory(directory: PathStr):
    """
    Resets working directory to cwd() after changing to 'directory' passed in as parameter.
    Reference:
    https://stackoverflow.com/questions/431684/equivalent-of-shell-cd-command-to-change-the-working-directory/431747#431747
    """
    owd = os.getcwd()  # noqa: PTH109
    try:
        os.chdir(directory)
        yield directory
    finally:
        os.chdir(owd)
