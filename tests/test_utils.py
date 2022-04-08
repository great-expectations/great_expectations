import logging
import os
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, List, Optional, Union, cast

import numpy as np
import pandas as pd
import pytest

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
from great_expectations.data_context.util import build_store_from_config

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


def load_data_into_test_database(
    table_name: str,
    connection_string: str,
    csv_path: Optional[str] = None,
    csv_paths: Optional[List[str]] = None,
    load_full_dataset: bool = False,
    convert_colnames_to_datetime: Optional[List[str]] = None,
) -> None:
    """
    Utility method that is used in loading test data into databases that can be accessed through SqlAlchemy.
    This includes local Dockerized DBs like postgres, but also cloud-dbs like BigQuery and Redshift.
    """
    if csv_path and csv_paths:
        csv_paths.append(csv_path)
    elif csv_path and not csv_paths:
        csv_paths = [csv_path]

    if convert_colnames_to_datetime is None:
        convert_colnames_to_datetime = []

    import pandas as pd

    connection = None

    if sa:
        engine = sa.create_engine(connection_string)
    else:
        logger.debug(
            "Attempting to load data in to tests SqlAlchemy database, but unable to load SqlAlchemy context; "
            "install optional sqlalchemy dependency for support."
        )
        return
    try:
        connection = engine.connect()
        print(f"Dropping table {table_name}")
        connection.execute(f"DROP TABLE IF EXISTS {table_name}")
        for csv_path in csv_paths:
            df = pd.read_csv(csv_path)
            for colname_to_convert in convert_colnames_to_datetime:
                df[colname_to_convert] = pd.to_datetime(df[colname_to_convert])
            if not load_full_dataset:
                # Improving test performance by only loading the first 10 rows of our test data into the db
                df = df.head(10)
            print(f"Creating table {table_name} and adding data from {csv_path}")
            df.to_sql(name=table_name, con=engine, index=False, if_exists="append")
    except SQLAlchemyError as e:
        logger.error(
            """Docs integration tests encountered an error while loading test-data into test-database."""
        )
        raise
    finally:
        connection.close()
        engine.dispose()


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
        logger.error(
            """Docs integration tests encountered an error while loading test-data into test-database."""
        )
        raise
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
