from __future__ import annotations

import logging
import pathlib
from datetime import datetime
from typing import Callable, Dict, Tuple

import pytest
from pytest import MonkeyPatch

from great_expectations.data_context import AbstractDataContext
from great_expectations.execution_engine import (
    ExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.experimental.datasources.interfaces import (
    BatchRequest,
    DataAsset,
    Datasource,
)
from great_expectations.experimental.datasources.metadatasource import MetaDatasource

LOGGER = logging.getLogger(__name__)

from contextlib import contextmanager

from great_expectations.core.batch import BatchData
from great_expectations.core.batch_spec import (
    BatchMarkers,
    SqlAlchemyDatasourceBatchSpec,
)
from great_expectations.experimental.datasources.sources import _SourceFactories

# This is the default min/max time that we are using in our mocks.
# They are made global so our tests can reference them directly.
DEFAULT_MIN_DT = datetime(2021, 1, 1, 0, 0, 0)
DEFAULT_MAX_DT = datetime(2022, 12, 31, 0, 0, 0)


class Dialect:
    def __init__(self, dialect: str):
        self.name = dialect


class _MockConnection:
    def __init__(self, dialect: Dialect):
        self.dialect = dialect

    def execute(self, query):
        """Execute a query over a sqlalchemy engine connection.

        Currently this mock assumes the query is always of the form:
        "select min(col), max(col) from table"
        where col is a datetime column since that's all that's necessary.
        This can be generalized if needed.

        Args:
            query: The SQL query to execute.
        """
        return [(DEFAULT_MIN_DT, DEFAULT_MAX_DT)]


class _MockSaEngine:
    def __init__(self, dialect: Dialect):
        self.dialect = dialect

    @contextmanager
    def connect(self):
        """A contextmanager that yields a _MockConnection"""
        yield _MockConnection(self.dialect)


def sqlachemy_execution_engine_mock_cls(
    validate_batch_spec: Callable[[SqlAlchemyDatasourceBatchSpec], None], dialect: str
):
    """Creates a mock gx sql alchemy engine class

    Args:
        validate_batch_spec: A hook that can be used to validate the generated the batch spec
            passed into get_batch_data_and_markers
        dialect: A string representing the SQL Engine dialect. Examples include: postgresql, sqlite
    """

    class MockSqlAlchemyExecutionEngine(SqlAlchemyExecutionEngine):
        def __init__(self, *args, **kwargs):
            # We should likely let the user pass in an engine. In a SqlAlchemyExecutionEngine used in
            # non-mocked code the engine property is of the type:
            # from sqlalchemy.engine import Engine as SaEngine
            self.engine = _MockSaEngine(dialect=Dialect(dialect))

        def get_batch_data_and_markers(  # type: ignore[override]
            self, batch_spec: SqlAlchemyDatasourceBatchSpec
        ) -> Tuple[BatchData, BatchMarkers]:
            validate_batch_spec(batch_spec)
            return BatchData(self), BatchMarkers(ge_load_time=None)

    return MockSqlAlchemyExecutionEngine


class ExecutionEngineDouble:
    def __init__(self, *args, **kwargs):
        pass

    def get_batch_data_and_markers(self, batch_spec) -> Tuple[BatchData, BatchMarkers]:
        return BatchData(self), BatchMarkers(ge_load_time=None)


@pytest.fixture
def inject_engine_lookup_double(monkeypatch: MonkeyPatch) -> ExecutionEngineDouble:  # type: ignore[misc]
    """
    Inject an execution engine test double into the _SourcesFactory.engine_lookup
    so that all Datasources use the execution engine double.
    Dynamically create a new subclass so that runtime type validation does not fail.
    """
    original_engine_override: Dict[MetaDatasource, ExecutionEngine] = {}
    for key in _SourceFactories.type_lookup.keys():
        if issubclass(type(key), MetaDatasource):
            original_engine_override[key] = key.execution_engine_override

    try:
        for source in original_engine_override.keys():
            source.execution_engine_override = ExecutionEngineDouble
        yield ExecutionEngineDouble
    finally:
        for source, engine in original_engine_override.items():
            source.execution_engine_override = engine


def pandas_data(
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    csv_path = (
        pathlib.Path(__file__) / "../../../test_sets/taxi_yellow_tripdata_samples"
    ).resolve()
    pandas_ds = context.sources.add_pandas(name="my_pandas")
    asset = pandas_ds.add_csv_asset(
        name="csv_asset",
        base_directory=csv_path,
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
        order_by=["year", "month"],
    )
    batch_request = asset.get_batch_request({"year": "2019", "month": "01"})
    return context, pandas_ds, asset, batch_request


def sql_data(
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    db_file = (
        pathlib.Path(__file__)
        / "../../../test_sets/taxi_yellow_tripdata_samples/sqlite/yellow_tripdata.db"
    ).resolve()
    datasource = context.sources.add_sqlite(
        name="test_datasource",
        connection_string=f"sqlite:///{db_file}",
    )
    asset = (
        datasource.add_table_asset(
            name="my_asset",
            table_name="yellow_tripdata_sample_2019_01",
        )
        .add_year_and_month_splitter(column_name="pickup_datetime")
        .add_sorters(["year", "month"])
    )
    batch_request = asset.get_batch_request({"year": 2019, "month": 1})
    return context, datasource, asset, batch_request


def spark_data(
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    csv_path = (
        pathlib.Path(__file__) / "../../../test_sets/taxi_yellow_tripdata_samples"
    ).resolve()
    spark_ds = context.sources.add_spark(name="my_spark")
    asset = spark_ds.add_csv_asset(
        name="csv_asset",
        base_directory=csv_path,
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
        order_by=["year", "month"],
    )
    batch_request = asset.get_batch_request({"year": "2019", "month": "01"})
    return context, spark_ds, asset, batch_request


def multibatch_pandas_data(
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    csv_path = (
        pathlib.Path(__file__) / "../../../test_sets/taxi_yellow_tripdata_samples"
    ).resolve()
    pandas_ds = context.sources.add_pandas(name="my_pandas")
    asset = pandas_ds.add_csv_asset(
        name="csv_asset",
        base_directory=csv_path,
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
        order_by=["year", "month"],
    )
    batch_request = asset.get_batch_request({"year": "2020"})
    return context, pandas_ds, asset, batch_request


def multibatch_sql_data(
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    db_file = (
        pathlib.Path(__file__)
        / "../../../test_sets/taxi_yellow_tripdata_samples/sqlite/yellow_tripdata_sample_2020_all_months_combined.db"
    ).resolve()
    datasource = context.sources.add_sqlite(
        name="test_datasource",
        connection_string=f"sqlite:///{db_file}",
    )
    asset = (
        datasource.add_table_asset(
            name="my_asset",
            table_name="yellow_tripdata_sample_2020",
        )
        .add_year_and_month_splitter(column_name="pickup_datetime")
        .add_sorters(["year", "month"])
    )
    batch_request = asset.get_batch_request({"year": 2020})
    return context, datasource, asset, batch_request


def multibatch_spark_data(
    context: AbstractDataContext,
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    csv_path = (
        pathlib.Path(__file__) / "../../../test_sets/taxi_yellow_tripdata_samples"
    ).resolve()
    spark_ds = context.sources.add_spark(name="my_spark")
    asset = spark_ds.add_csv_asset(
        name="csv_asset",
        base_directory=csv_path,
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
        order_by=["year", "month"],
    )
    batch_request = asset.get_batch_request({"year": "2020"})
    return context, spark_ds, asset, batch_request


@pytest.fixture(params=[pandas_data, sql_data, spark_data])
def datasource_test_data(
    empty_data_context, request
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    return request.param(empty_data_context)


@pytest.fixture(
    params=[multibatch_pandas_data, multibatch_sql_data, multibatch_spark_data]
)
def multibatch_datasource_test_data(
    empty_data_context, request
) -> tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest]:
    return request.param(empty_data_context)
