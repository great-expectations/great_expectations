from __future__ import annotations

import logging
import os
import pathlib
import uuid
from typing import TYPE_CHECKING, Final, Generator, Iterator, Literal, Protocol

import numpy as np
import pandas as pd
import pytest

import great_expectations as gx
import great_expectations.exceptions as gx_exceptions
from great_expectations.checkpoint import Checkpoint
from great_expectations.compatibility.sqlalchemy import TextClause
from great_expectations.core import ExpectationSuite
from great_expectations.data_context import CloudDataContext
from great_expectations.execution_engine import (
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations import ExpectColumnValuesToNotBeNull

if TYPE_CHECKING:
    from great_expectations.compatibility import pyspark, sqlalchemy

LOGGER: Final = logging.getLogger("tests")


@pytest.fixture(scope="package")
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
def test_data() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "name": [1, 2, 3, 4],
        },
    )


@pytest.fixture(scope="module")
def datasource_name(
    context: CloudDataContext,
) -> Iterator[str]:
    datasource_name = f"ds_{uuid.uuid4().hex}"
    yield datasource_name
    # if the test was skipped, we may not have a datasource to clean up
    # in that case, we create one simply to test get and delete
    try:
        _ = context.data_sources.get(name=datasource_name)
    except ValueError:
        _ = context.data_sources.add_pandas(name=datasource_name)
        context.data_sources.get(name=datasource_name)
    context.delete_datasource(name=datasource_name)
    with pytest.raises(KeyError):
        _ = context.data_sources.get(name=datasource_name)


@pytest.fixture(scope="module")
def expectation_suite(
    context: CloudDataContext,
) -> Iterator[ExpectationSuite]:
    """This ExpectationSuite is shared by each E2E test, so its expected that the data
    used by each test follows the same shape."""
    expectation_suite_name = f"es_{uuid.uuid4().hex}"
    context.suites.add(
        ExpectationSuite(
            name=expectation_suite_name,
            expectations=[
                ExpectColumnValuesToNotBeNull(column="name", mostly=1)  # type: ignore[arg-type]  # todo: fix in core-412
            ],
        )
    )
    yield context.suites.get(name=expectation_suite_name)
    context.suites.delete(expectation_suite_name)
    with pytest.raises(gx_exceptions.DataContextError):
        context.suites.get(name=expectation_suite_name)


@pytest.fixture(scope="module")
def checkpoint(
    context: CloudDataContext,
) -> Generator[Checkpoint, None, None]:
    """This Checkpoint is used by each E2E test. It's expected that each test
    will override its list of validation definitions within the test module."""
    checkpoint_name = f"E2E Test Checkpoint {uuid.uuid4().hex}"

    checkpoint = Checkpoint(name=checkpoint_name, validation_definitions=[])
    checkpoint = context.checkpoints.add(checkpoint=checkpoint)
    yield checkpoint
    context.checkpoints.delete(name=checkpoint_name)

    with pytest.raises(gx_exceptions.DataContextError):
        context.checkpoints.get(name=checkpoint_name)


@pytest.fixture(scope="module")
def tmp_path(tmp_path_factory) -> pathlib.Path:
    return tmp_path_factory.mktemp("project")


class TableFactory(Protocol):
    def __call__(
        self,
        gx_engine: SqlAlchemyExecutionEngine,
        table_names: set[str],
        schema_name: str | None = None,
    ) -> None: ...


@pytest.fixture(scope="module")
def table_factory() -> Iterator[TableFactory]:
    """
    Given a SQLAlchemy engine, table_name and schema,
    create the table if it does not exist and drop it after the test class.
    """
    all_created_tables: dict[str, list[dict[Literal["table_name", "schema_name"], str | None]]] = {}
    engines: dict[str, sqlalchemy.engine.Engine] = {}

    def _table_factory(
        gx_engine: SqlAlchemyExecutionEngine,
        table_names: set[str],
        schema_name: str | None = None,
    ) -> None:
        sa_engine = gx_engine.engine
        LOGGER.info(
            f"Creating `{sa_engine.dialect.name}` table for {table_names} if it does not exist"
        )
        created_tables: list[dict[Literal["table_name", "schema_name"], str | None]] = []

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
    for dialect, tables in all_created_tables.items():
        engine = engines[dialect]
        with engine.connect() as conn:
            transaction = conn.begin()
            schema: str | None = None
            for table in tables:
                name = table["table_name"]
                schema = table["schema_name"]
                qualified_table_name = f"{schema}.{name}" if schema else name
                conn.execute(TextClause(f"DROP TABLE IF EXISTS {qualified_table_name}"))
            if schema:
                conn.execute(TextClause(f"DROP SCHEMA IF EXISTS {schema}"))
            transaction.commit()


def construct_spark_df_from_pandas(
    spark_session: pyspark.SparkSession,
    pandas_df: pd.DataFrame,
) -> pyspark.DataFrame:
    spark_df = spark_session.createDataFrame(
        [
            tuple(
                None if isinstance(x, (float, int)) and np.isnan(x) else x for x in record.tolist()
            )
            for record in pandas_df.to_records(index=False)
        ],
        pandas_df.columns.tolist(),
    )
    return spark_df


@pytest.fixture(scope="module")
def spark_session() -> pyspark.SparkSession:
    from great_expectations.compatibility import pyspark

    if pyspark.SparkSession:  # type: ignore[truthy-function]
        return SparkDFExecutionEngine.get_or_create_spark_session()

    raise ValueError("spark tests are requested, but pyspark is not installed")
