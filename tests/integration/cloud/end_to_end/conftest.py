from __future__ import annotations

import logging
import os
from pprint import pformat as pf
from typing import TYPE_CHECKING, Final, Generator, Literal, Protocol

import numpy as np
import pytest

import great_expectations as gx
from great_expectations.compatibility.sqlalchemy import TextClause
from great_expectations.core.util import get_or_create_spark_application
from great_expectations.data_context import CloudDataContext
from great_expectations.execution_engine import SqlAlchemyExecutionEngine

if TYPE_CHECKING:
    from great_expectations.compatibility import pyspark
    from great_expectations.compatibility.sqlalchemy import engine

LOGGER: Final = logging.getLogger("tests")


@pytest.fixture
def context() -> CloudDataContext:
    context = gx.get_context(
        mode="cloud",
        cloud_base_url=os.environ.get("GX_CLOUD_BASE_URL"),
        cloud_organization_id=os.environ.get("GX_CLOUD_ORGANIZATION_ID"),
        cloud_access_token=os.environ.get("GX_CLOUD_ACCESS_TOKEN"),
    )
    assert isinstance(context, CloudDataContext)
    return context


class TableFactory(Protocol):
    def __call__(
        self,
        gx_engine: SqlAlchemyExecutionEngine,
        table_names: set[str],
        schema_name: str | None = None,
    ) -> None:
        ...


@pytest.fixture(scope="class")
def table_factory() -> Generator[TableFactory, None, None]:
    """
    Class scoped.
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


@pytest.fixture
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


@pytest.fixture
def spark_session() -> pyspark.SparkSession:
    from great_expectations.compatibility import pyspark

    if pyspark.SparkSession:  # type: ignore[truthy-function]
        return get_or_create_spark_application(
            spark_config={
                "spark.sql.catalogImplementation": "hive",
                "spark.executor.memory": "450m",
            }
        )

    raise ValueError("spark tests are requested, but pyspark is not installed")
