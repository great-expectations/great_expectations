from __future__ import annotations

import logging
import os
import pathlib
import uuid
from pprint import pformat as pf
from typing import TYPE_CHECKING, Final, Iterator, Literal, Protocol

import numpy as np
import pytest

import great_expectations as gx
import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility.sqlalchemy import TextClause
from great_expectations.core import ExpectationSuite
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context import CloudDataContext
from great_expectations.exceptions.exceptions import BuildBatchRequestError
from great_expectations.execution_engine import (
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)

if TYPE_CHECKING:
    from great_expectations.checkpoint import Checkpoint
    from great_expectations.compatibility import pyspark, sqlalchemy
    from great_expectations.datasource.fluent import BatchRequest
    from great_expectations.validator.validator import Validator

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
    with pytest.raises(ValueError):
        _ = context.data_sources.get(name=datasource_name)


@pytest.fixture(scope="module")
def expectation_suite(
    context: CloudDataContext,
) -> Iterator[ExpectationSuite]:
    expectation_suite_name = f"es_{uuid.uuid4().hex}"
    expectation_suite = context.suites.add(ExpectationSuite(name=expectation_suite_name))
    assert len(expectation_suite.expectations) == 0
    yield expectation_suite
    expectation_suite = context.suites.get(name=expectation_suite_name)
    assert len(expectation_suite.expectations) > 0
    context.suites.delete(expectation_suite_name)
    with pytest.raises(gx_exceptions.DataContextError):
        _ = context.suites.get(name=expectation_suite_name)


@pytest.fixture(scope="module")
def validator(
    context: CloudDataContext,
    batch_request: BatchRequest,
    expectation_suite: ExpectationSuite,
) -> Iterator[Validator]:
    validator: Validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite.name,
    )
    validator.head()
    yield validator
    validator.save_expectation_suite()
    expectation_suite = validator.get_expectation_suite()
    _ = context.suites.get(
        name=expectation_suite.name,
    )


@pytest.fixture(scope="module")
def checkpoint(
    context: CloudDataContext,
    batch_request: BatchRequest,
    expectation_suite: ExpectationSuite,
) -> Iterator[Checkpoint]:
    checkpoint_name = f"{batch_request.data_asset_name} | {expectation_suite.name}"

    validation_definitions: list[ValidationDefinition] = []
    checkpoint = Checkpoint(name=checkpoint_name, validation_definitions=validation_definitions)
    checkpoint = context.checkpoints.add(checkpoint=checkpoint)
    yield checkpoint
    context.checkpoints.delete(name=checkpoint_name)

    with pytest.raises(gx_exceptions.DataContextError):
        context.checkpoints.get(name=checkpoint_name)


@pytest.fixture(scope="module")
def tmp_path(tmp_path_factory) -> pathlib.Path:
    return tmp_path_factory.mktemp("project")


@pytest.fixture(scope="package")
def get_missing_data_asset_error_type() -> type[Exception]:
    return LookupError


@pytest.fixture(scope="package")
def in_memory_batch_request_missing_dataframe_error_type() -> type[Exception]:
    return BuildBatchRequestError


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
    print(f"dropping tables\n{pf(all_created_tables)}")
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
