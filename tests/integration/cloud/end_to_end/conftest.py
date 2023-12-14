from __future__ import annotations

import logging
import os
from pprint import pformat as pf
from typing import TYPE_CHECKING, Final, Iterator, Literal, Protocol

import numpy as np
import pytest

import great_expectations as gx
import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility.sqlalchemy import TextClause
from great_expectations.data_context import CloudDataContext
from great_expectations.execution_engine import (
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)

if TYPE_CHECKING:
    import py

    from great_expectations.compatibility import pyspark
    from great_expectations.compatibility.sqlalchemy import engine

LOGGER: Final = logging.getLogger("tests")


@pytest.fixture(scope="module")
def context() -> CloudDataContext:
    context = gx.get_context(
        mode="cloud",
        cloud_base_url=os.environ.get("GX_CLOUD_BASE_URL"),
        cloud_organization_id=os.environ.get("GX_CLOUD_ORGANIZATION_ID"),
        cloud_access_token=os.environ.get("GX_CLOUD_ACCESS_TOKEN"),
    )
    assert isinstance(context, CloudDataContext)
    return context


@pytest.fixture(scope="module")
def tmp_dir(tmpdir_factory) -> py.path:
    return tmpdir_factory.mktemp("project")


@pytest.fixture(scope="module")
def get_missing_datasource_error_type() -> type[Exception]:
    return ValueError


@pytest.fixture(scope="module")
def get_missing_data_asset_error_type() -> type[Exception]:
    return LookupError


@pytest.fixture(scope="module")
def get_missing_expectation_suite_error_type() -> type[Exception]:
    return gx_exceptions.DataContextError


@pytest.fixture(scope="module")
def get_missing_checkpoint_error_type() -> type[Exception]:
    return gx_exceptions.DataContextError


@pytest.fixture(scope="module")
def in_memory_batch_request_missing_dataframe_error_type() -> type[Exception]:
    return ValueError


class TableFactory(Protocol):
    def __call__(
        self,
        gx_engine: SqlAlchemyExecutionEngine,
        table_names: set[str],
        schema_name: str | None = None,
    ) -> None:
        ...


@pytest.fixture(scope="module")
def table_factory() -> Iterator[TableFactory]:
    """
    Given a SQLAlchemy engine, table_name and schema,
    create the table if it does not exist and drop it after the test class.
    """
    all_created_tables: dict[
        str, list[dict[Literal["table_name", "schema_name"], str | None]]
    ] = {}
    engines: dict[str, engine.Engine] = {}

    def _table_factory(
        gx_engine: SqlAlchemyExecutionEngine,
        table_names: set[str],
        schema_name: str | None = None,
    ) -> None:
        sa_engine = gx_engine.engine
        LOGGER.info(
            f"Creating `{sa_engine.dialect.name}` table for {table_names} if it does not exist"
        )
        created_tables: list[
            dict[Literal["table_name", "schema_name"], str | None]
        ] = []

        with gx_engine.get_connection() as conn:
            transaction = conn.begin()
            if schema_name:
                conn.execute(TextClause(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            for name in table_names:
                qualified_table_name = f"{schema_name}.{name}" if schema_name else name
                create_tables: str = (
                    f"CREATE TABLE IF NOT EXISTS {qualified_table_name}"
                    f" (id INTEGER, name VARCHAR(255), value INTEGER)"
                )
                conn.execute(TextClause(create_tables))

                created_tables.append(dict(table_name=name, schema_name=schema_name))
            transaction.commit()
        all_created_tables[sa_engine.dialect.name] = created_tables
        engines[sa_engine.dialect.name] = sa_engine

    yield _table_factory

    # teardown
    print(f"dropping tables\n{pf(all_created_tables)}")
    for dialect, tables in all_created_tables.items():
        engine = engines[dialect]
        with engine.connect() as conn:
            transaction = conn.begin()
            for table in tables:
                name = table["table_name"]
                schema = table["schema_name"]
                qualified_table_name = f"{schema}.{name}" if schema else name
                conn.execute(TextClause(f"DROP TABLE IF EXISTS {qualified_table_name}"))
            if schema:
                conn.execute(TextClause(f"DROP SCHEMA IF EXISTS {schema}"))
            transaction.commit()


@pytest.fixture(scope="module")
def spark_df_from_pandas_df():
    """
    Construct a spark dataframe from pandas dataframe.
    Returns:
        Function that can be used in your test e.g.:
        spark_df = spark_df_from_pandas_df(spark_session, pandas_df)
    """

    def _construct_spark_df_from_pandas(
        spark_session,
        pandas_df,
    ):
        spark_df = spark_session.createDataFrame(
            [
                tuple(
                    None if isinstance(x, (float, int)) and np.isnan(x) else x
                    for x in record.tolist()
                )
                for record in pandas_df.to_records(index=False)
            ],
            pandas_df.columns.tolist(),
        )
        return spark_df

    return _construct_spark_df_from_pandas


@pytest.fixture(scope="module")
def spark_session() -> pyspark.SparkSession:
    from great_expectations.compatibility import pyspark

    if pyspark.SparkSession:  # type: ignore[truthy-function]
        return SparkDFExecutionEngine.get_or_create_spark_session()

    raise ValueError("spark tests are requested, but pyspark is not installed")
