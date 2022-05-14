import logging
import os
import uuid
import warnings
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generator, List, Optional, Set, Tuple, Union, cast

import numpy as np
import pandas as pd
import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.store import (
    CheckpointStore,
    ProfilerStore,
    StoreBackend,
)
from great_expectations.data_context.store.util import (
    build_checkpoint_store_using_store_backend,
    build_configuration_store,
    delete_checkpoint_config_from_store_backend,
    delete_config_from_store_backend,
    load_checkpoint_config_from_store_backend,
    load_config_from_store_backend,
    save_checkpoint_config_to_store_backend,
    save_config_to_store_backend,
)
from great_expectations.data_context.types.base import BaseYamlConfig, CheckpointConfig
from great_expectations.data_context.util import (
    build_store_from_config,
    instantiate_class_from_config,
)
from great_expectations.execution_engine import SqlAlchemyExecutionEngine

logger = logging.getLogger(__name__)

try:
    import sqlalchemy as sa
    from sqlalchemy.exc import SQLAlchemyError

except ImportError:
    logger.debug(
        "Unable to load SqlAlchemy context; install optional sqlalchemy dependency for support"
    )
    sa = None
    reflection = None
    Table = None
    Select = None
    SQLAlchemyError = None


logger = logging.getLogger(__name__)
yaml_handler: YAMLHandler = YAMLHandler()

# Taken from the following stackoverflow:
# https://stackoverflow.com/questions/23549419/assert-that-two-dictionaries-are-almost-equal
# noinspection PyPep8Naming
def assertDeepAlmostEqual(expected, actual, *args, **kwargs):
    """
    Assert that two complex structures have almost equal contents.

    Compares lists, dicts and tuples recursively. Checks numeric values
    using pyteset.approx and checks all other values with an assertion equality statement
    Accepts additional positional and keyword arguments and pass those
    intact to pytest.approx() (that's how you specify comparison
    precision).

    """
    is_root = "__trace" not in kwargs
    trace = kwargs.pop("__trace", "ROOT")
    try:
        # if isinstance(expected, (int, float, long, complex)):
        if isinstance(expected, (int, float, complex)):
            assert expected == pytest.approx(actual, *args, **kwargs)
        elif isinstance(expected, (list, tuple, np.ndarray)):
            assert len(expected) == len(actual)
            for index in range(len(expected)):
                v1, v2 = expected[index], actual[index]
                assertDeepAlmostEqual(v1, v2, __trace=repr(index), *args, **kwargs)
        elif isinstance(expected, dict):
            assert set(expected) == set(actual)
            for key in expected:
                assertDeepAlmostEqual(
                    expected[key], actual[key], __trace=repr(key), *args, **kwargs
                )
        else:
            assert expected == actual
    except AssertionError as exc:
        exc.__dict__.setdefault("traces", []).append(trace)
        if is_root:
            trace = " -> ".join(reversed(exc.traces))
            exc = AssertionError(f"{str(exc)}\nTRACE: {trace}")
        raise exc


def safe_remove(path):
    if path is not None:
        try:
            os.remove(path)
        except OSError as e:
            print(e)


def create_files_for_regex_partitioner(
    root_directory_path: str, directory_paths: list = None, test_file_names: list = None
):
    if not directory_paths:
        return

    if not test_file_names:
        test_file_names: list = [
            "alex_20200809_1000.csv",
            "eugene_20200809_1500.csv",
            "james_20200811_1009.csv",
            "abe_20200809_1040.csv",
            "will_20200809_1002.csv",
            "james_20200713_1567.csv",
            "eugene_20201129_1900.csv",
            "will_20200810_1001.csv",
            "james_20200810_1003.csv",
            "alex_20200819_1300.csv",
        ]

    base_directories = []
    for dir_path in directory_paths:
        if dir_path is None:
            base_directories.append(dir_path)
        else:
            data_dir_path = os.path.join(root_directory_path, dir_path)
            os.makedirs(data_dir_path, exist_ok=True)
            base_dir = str(data_dir_path)
            # Put test files into the directories.
            for file_name in test_file_names:
                file_path = os.path.join(base_dir, file_name)
                with open(file_path, "w") as fp:
                    fp.writelines([f'The name of this file is: "{file_path}".\n'])
            base_directories.append(base_dir)


def create_files_in_directory(
    directory: str, file_name_list: List[str], file_content_fn=lambda: "x,y\n1,2\n2,3"
):
    subdirectories = []
    for file_name in file_name_list:
        splits = file_name.split("/")
        for i in range(1, len(splits)):
            subdirectories.append(os.path.join(*splits[:i]))
    subdirectories = set(subdirectories)

    for subdirectory in subdirectories:
        os.makedirs(os.path.join(directory, subdirectory), exist_ok=True)

    for file_name in file_name_list:
        file_path = os.path.join(directory, file_name)
        with open(file_path, "w") as f_:
            f_.write(file_content_fn())


def create_fake_data_frame():
    return pd.DataFrame(
        {
            "x": range(10),
            "y": list("ABCDEFGHIJ"),
        }
    )


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


def get_sqlite_temp_table_names(engine):
    result = engine.execute(
        """
SELECT
    name
FROM
    sqlite_temp_master
"""
    )
    rows = result.fetchall()
    return {row[0] for row in rows}


def get_sqlite_table_names(engine):
    result = engine.execute(
        """
SELECT
    name
FROM
    sqlite_master
"""
    )
    rows = result.fetchall()
    return {row[0] for row in rows}


def build_in_memory_store_backend(
    module_name: str = "great_expectations.data_context.store",
    class_name: str = "InMemoryStoreBackend",
    **kwargs,
) -> StoreBackend:
    logger.debug("Starting data_context/store/util.py#build_in_memory_store_backend")
    store_backend_config: dict = {"module_name": module_name, "class_name": class_name}
    store_backend_config.update(**kwargs)
    return build_store_from_config(
        store_config=store_backend_config,
        module_name=module_name,
        runtime_environment=None,
    )


def build_tuple_filesystem_store_backend(
    base_directory: str,
    *,
    module_name: str = "great_expectations.data_context.store",
    class_name: str = "TupleFilesystemStoreBackend",
    **kwargs,
) -> StoreBackend:
    logger.debug(
        f"""Starting data_context/store/util.py#build_tuple_filesystem_store_backend using base_directory:
"{base_directory}"""
    )
    store_backend_config: dict = {
        "module_name": module_name,
        "class_name": class_name,
        "base_directory": base_directory,
    }
    store_backend_config.update(**kwargs)
    return build_store_from_config(
        store_config=store_backend_config,
        module_name=module_name,
        runtime_environment=None,
    )


def build_tuple_s3_store_backend(
    bucket: str,
    *,
    module_name: str = "great_expectations.data_context.store",
    class_name: str = "TupleS3StoreBackend",
    **kwargs,
) -> StoreBackend:
    logger.debug(
        f"""Starting data_context/store/util.py#build_tuple_s3_store_backend using bucket: {bucket}
        """
    )
    store_backend_config: dict = {
        "module_name": module_name,
        "class_name": class_name,
        "bucket": bucket,
    }
    store_backend_config.update(**kwargs)
    return build_store_from_config(
        store_config=store_backend_config,
        module_name=module_name,
        runtime_environment=None,
    )


def build_checkpoint_store_using_filesystem(
    store_name: str,
    base_directory: str,
    overwrite_existing: bool = False,
) -> CheckpointStore:
    store_config: dict = {"base_directory": base_directory}
    store_backend_obj: StoreBackend = build_tuple_filesystem_store_backend(
        **store_config
    )
    return build_checkpoint_store_using_store_backend(
        store_name=store_name,
        store_backend=store_backend_obj,
        overwrite_existing=overwrite_existing,
    )


def build_profiler_store_using_store_backend(
    store_name: str,
    store_backend: Union[StoreBackend, dict],
    overwrite_existing: bool = False,
) -> ProfilerStore:
    return cast(
        ProfilerStore,
        build_configuration_store(
            class_name="ProfilerStore",
            module_name="great_expectations.data_context.store",
            store_name=store_name,
            store_backend=store_backend,
            overwrite_existing=overwrite_existing,
        ),
    )


def build_profiler_store_using_filesystem(
    store_name: str,
    base_directory: str,
    overwrite_existing: bool = False,
) -> ProfilerStore:
    store_config: dict = {"base_directory": base_directory}
    store_backend_obj: StoreBackend = build_tuple_filesystem_store_backend(
        **store_config
    )
    store = build_profiler_store_using_store_backend(
        store_name=store_name,
        store_backend=store_backend_obj,
        overwrite_existing=overwrite_existing,
    )
    return store


def save_checkpoint_config_to_filesystem(
    store_name: str,
    base_directory: str,
    checkpoint_name: str,
    checkpoint_configuration: CheckpointConfig,
):
    store_config: dict = {"base_directory": base_directory}
    store_backend_obj: StoreBackend = build_tuple_filesystem_store_backend(
        **store_config
    )
    save_checkpoint_config_to_store_backend(
        store_name=store_name,
        store_backend=store_backend_obj,
        checkpoint_name=checkpoint_name,
        checkpoint_configuration=checkpoint_configuration,
    )


def load_checkpoint_config_from_filesystem(
    store_name: str,
    base_directory: str,
    checkpoint_name: str,
) -> CheckpointConfig:
    store_config: dict = {"base_directory": base_directory}
    store_backend_obj: StoreBackend = build_tuple_filesystem_store_backend(
        **store_config
    )
    return load_checkpoint_config_from_store_backend(
        store_name=store_name,
        store_backend=store_backend_obj,
        checkpoint_name=checkpoint_name,
    )


def delete_checkpoint_config_from_filesystem(
    store_name: str,
    base_directory: str,
    checkpoint_name: str,
):
    store_config: dict = {"base_directory": base_directory}
    store_backend_obj: StoreBackend = build_tuple_filesystem_store_backend(
        **store_config
    )
    delete_checkpoint_config_from_store_backend(
        store_name=store_name,
        store_backend=store_backend_obj,
        checkpoint_name=checkpoint_name,
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
    store_backend_obj: StoreBackend = build_tuple_filesystem_store_backend(
        **store_config
    )
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
    store_backend_obj: StoreBackend = build_tuple_filesystem_store_backend(
        **store_config
    )
    return load_config_from_store_backend(
        class_name=configuration_store_class_name,
        module_name=configuration_store_module_name,
        store_name=store_name,
        store_backend=store_backend_obj,
        configuration_key=configuration_key,
    )


def delete_config_from_filesystem(
    configuration_store_class_name: str,
    configuration_store_module_name: str,
    store_name: str,
    base_directory: str,
    configuration_key: str,
):
    store_config: dict = {"base_directory": base_directory}
    store_backend_obj: StoreBackend = build_tuple_filesystem_store_backend(
        **store_config
    )
    delete_config_from_store_backend(
        class_name=configuration_store_class_name,
        module_name=configuration_store_module_name,
        store_name=store_name,
        store_backend=store_backend_obj,
        configuration_key=configuration_key,
    )


def get_snowflake_connection_url() -> str:
    """Get snowflake connection url from environment variables.

    Returns:
        String of the snowflake connection url.
    """
    sfAccount = os.environ.get("SNOWFLAKE_ACCOUNT")
    sfUser = os.environ.get("SNOWFLAKE_USER")
    sfPswd = os.environ.get("SNOWFLAKE_PW")
    sfDatabase = os.environ.get("SNOWFLAKE_DATABASE")
    sfSchema = os.environ.get("SNOWFLAKE_SCHEMA")
    sfWarehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")

    return f"snowflake://{sfUser}:{sfPswd}@{sfAccount}/{sfDatabase}/{sfSchema}?warehouse={sfWarehouse}"


def get_bigquery_table_prefix() -> str:
    """Get table_prefix that will be used by the BigQuery client in loading a BigQuery table from DataFrame.

    Reference link
        https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe

    Returns:
        String of table prefix, which is the gcp_project and dataset concatenated by a "."
    """
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
    """

    if convert_column_names_to_datetime is None:
        convert_column_names_to_datetime = []

    import pandas as pd

    dfs: List[pd.DataFrame] = []
    for csv_path in csv_paths:
        df = pd.read_csv(csv_path)
        for column_name_to_convert in convert_column_names_to_datetime:
            df[column_name_to_convert] = pd.to_datetime(df[column_name_to_convert])
        if not load_full_dataset:
            # Improving test performance by only loading the first 10 rows of our test data into the db
            df = df.head(10)

        dfs.append(df)

    all_dfs_concatenated: pd.DataFrame = pd.concat(dfs)

    return all_dfs_concatenated


def load_data_into_test_database(
    table_name: str,
    connection_string: str,
    csv_path: Optional[str] = None,
    csv_paths: Optional[List[str]] = None,
    load_full_dataset: bool = False,
    convert_colnames_to_datetime: Optional[List[str]] = None,
    random_table_suffix: bool = False,
    to_sql_method: Optional[str] = None,
) -> LoadedTable:
    """Utility method that is used in loading test data into databases that can be accessed through SqlAlchemy.

    This includes local Dockerized DBs like postgres, but also cloud-dbs like BigQuery and Redshift.

    Args:
        table_name: name of the table to write to.
        connection_string: used to connect to the database.
        csv_path: path of a single csv to write.
        csv_paths: list of paths of csvs to write.
        load_full_dataset: if False, load only the first 10 rows.
        convert_colnames_to_datetime: List of column names to convert to datetime before writing to db.
        random_table_suffix: If true, add 8 random characters to the table suffix and remove other tables with the
            same prefix.
        to_sql_method: Method to pass to method param of pd.to_sql()

    Returns:
        LoadedTable which for convenience, contains the pandas dataframe that was used to load the data.
    """
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
            "Attempting to load data in to tests SqlAlchemy database, but unable to load SqlAlchemy context; "
            "install optional sqlalchemy dependency for support."
        )
        return return_value
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
        except SQLAlchemyError as e:
            error_message: str = """Docs integration tests encountered an error while loading test-data into test-database."""
            logger.error(error_message)
            raise ge_exceptions.DatabaseConnectionError(error_message)
            # Normally we would call `raise` to re-raise the SqlAlchemyError but we don't to make sure that
            # sensitive information does not make it into our CI logs.
        finally:
            connection.close()
            engine.dispose()
    else:
        try:
            connection = engine.connect()
            print(f"Dropping table {table_name}")
            connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            print(f"Creating table {table_name} and adding data from {csv_paths}")
            all_dfs_concatenated.to_sql(
                name=table_name,
                con=engine,
                index=False,
                if_exists="append",
                method=to_sql_method,
            )
            return return_value
        except SQLAlchemyError as e:
            error_message: str = """Docs integration tests encountered an error while loading test-data into test-database."""
            logger.error(error_message)
            raise ge_exceptions.DatabaseConnectionError(error_message)
            # Normally we would call `raise` to re-raise the SqlAlchemyError but we don't to make sure that
            # sensitive information does not make it into our CI logs.
        finally:
            connection.close()
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
    """
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
    job.result()  # Wait for the job to complete


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
    data_connector = instantiate_class_from_config(
        config={
            "class_name": "InferredAssetSqlDataConnector",
            "name": "temp_data_connector",
        },
        runtime_environment={
            "execution_engine": execution_engine,
            "datasource_name": "temp_datasource",
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )
    introspection_output = data_connector._introspect_db()

    tables_to_drop: List[str] = []
    tables_dropped: List[str] = []

    for table in introspection_output:
        if table["table_name"].startswith(table_prefix):
            tables_to_drop.append(table["table_name"])

    connection = execution_engine.engine.connect()
    for table_name in tables_to_drop:
        print(f"Dropping table {table_name}")
        connection.execute(f"DROP TABLE IF EXISTS {table_name}")
        tables_dropped.append(table_name)

    tables_skipped: List[str] = list(set(tables_to_drop) - set(tables_dropped))
    if len(tables_skipped) > 0:
        warnings.warn(f"Warning: Tables skipped: {tables_skipped}")

    return tables_dropped


@contextmanager
def set_directory(path: str) -> Generator:
    """Sets the cwd within the context

    Args:
        path: The string representation of the desired path to cd into

    Yields:
        None
    """
    origin = Path().absolute()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(origin)


def check_athena_table_count(
    connection_string: str, db_name: str, expected_table_count: int
) -> bool:
    """
    Helper function used by awsathena integration test. Checks whether expected number of tables exist in database
    """
    if sa:
        engine = sa.create_engine(connection_string)
    else:
        logger.debug(
            "Attempting to perform test on AWSAthena database, but unable to load SqlAlchemy context; "
            "install optional sqlalchemy dependency for support."
        )
        return False

    connection = None
    try:
        connection = engine.connect()
        result = connection.execute(sa.text(f"SHOW TABLES in {db_name}")).fetchall()
        return len(result) == expected_table_count
    except SQLAlchemyError as e:
        error_message: str = """Docs integration tests encountered an error while loading test-data into test-database."""
        logger.error(error_message)
        raise ge_exceptions.DatabaseConnectionError(error_message)
        # Normally we would call `raise` to re-raise the SqlAlchemyError but we don't to make sure that
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
            "Attempting to perform test on AWSAthena database, but unable to load SqlAlchemy context; "
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


def get_awsathena_db_name(db_name_env_var: str = "ATHENA_DB_NAME") -> str:
    """Get awsathena database name from environment variables.

    Returns:
        String of the awsathena database name.
    """
    athena_db_name: str = os.getenv(db_name_env_var)
    if not athena_db_name:
        raise ValueError(
            f"Environment Variable {db_name_env_var} is required to run integration tests against AWS Athena"
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
            "Environment Variable ATHENA_STAGING_S3 is required to run integration tests against AWS Athena"
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
    elif dialect == "bigquery":
        connection_string: str = get_bigquery_connection_url()
    elif dialect == "awsathena":
        connection_string: str = get_awsathena_connection_url(athena_db_name_env_var)
    else:
        connection_string: str = db_config["connection_string"]

    return dialect, connection_string


def find_strings_in_nested_obj(obj: Any, target_strings: List[str]) -> bool:
    """Recursively traverse a nested structure to find all strings in an input string.

    Args:
        obj (Any): The object to traverse (generally a dict to start with)
        target_strings (List[str]): The collection of strings to find.

    Returns:
        True if ALL target strings are found. Otherwise, will return False.
    """

    strings: Set[str] = set(target_strings)

    def _find_string(data: Any) -> bool:
        if isinstance(data, list):
            for val in data:
                if _find_string(val):
                    return True
        elif isinstance(data, dict):
            for key, val in data.items():
                if _find_string(key) or _find_string(val):
                    return True
        elif isinstance(data, str):
            string_to_remove: Optional[str] = None
            for string in strings:
                if string in data:
                    string_to_remove = string
                    break
            if string_to_remove:
                strings.remove(string_to_remove)
                if not strings:
                    return True
        return False

    success: bool = _find_string(obj)
    if not success:
        logger.info(f"Could not find the following target strings: {strings}")
    return success
