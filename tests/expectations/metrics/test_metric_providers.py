from __future__ import annotations

import copy
from typing import Any, Dict, List
from unittest import mock

import pytest

from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations import registry
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    ColumnPairMapMetricProvider,
    MulticolumnMapMetricProvider,
)
from great_expectations.expectations.metrics.map_metric_provider.column_condition_partial import (
    column_condition_partial,
)
from great_expectations.expectations.metrics.map_metric_provider.column_pair_condition_partial import (  # noqa: E501
    column_pair_condition_partial,
)
from great_expectations.expectations.metrics.map_metric_provider.multicolumn_condition_partial import (  # noqa: E501
    multicolumn_condition_partial,
)
from great_expectations.expectations.metrics.metric_provider import (
    MetricProvider,
    metric_value,
)
from great_expectations.expectations.metrics.query_metric_provider import (
    QueryMetricProvider,
    QueryParameters,
)
from great_expectations.expectations.metrics.table_metric_provider import (
    TableMetricProvider,
)
from great_expectations.expectations.metrics.util import MAX_RESULT_RECORDS

pytestmark = pytest.mark.unit


class Dialect:
    def __init__(self, dialect: str):
        self.name = dialect


class MockSaEngine:
    def __init__(self, dialect: Dialect):
        self.dialect = dialect

    def connect(self) -> None:
        pass


class MockResult:
    def fetchmany(self, recordcount: int):
        return None


class MockConnection:
    def execute(self, query: str):
        return MockResult()


class MockSqlAlchemyExecutionEngine(SqlAlchemyExecutionEngine):
    def __init__(self, create_temp_table: bool = True, *args, **kwargs):
        self.engine = MockSaEngine(dialect=Dialect("sqlite"))  # type: ignore[assignment]
        self._create_temp_table = create_temp_table
        self._connection = MockConnection()


@pytest.fixture
def mock_registry(monkeypatch: pytest.MonkeyPatch):
    """Ensures consistent `_registered_metrics` state among test cases runs."""
    monkeypatch.setattr(
        registry,
        "_registered_metrics",
        copy.deepcopy(registry._registered_metrics),
        raising=True,
    )
    yield registry


def test__base_metric_provider__registration(mock_registry):
    """This tests whether the MetricProvider class registers the correct metrics."""
    registered_metric_keys = list(mock_registry._registered_metrics.keys())
    for key in registered_metric_keys:
        assert "custom_metric" not in key

    prev_registered_metric_key_count = len(registered_metric_keys)

    class CustomMetricProvider(MetricProvider):
        metric_name = "custom_metric"
        value_keys = ()

        @metric_value(engine=PandasExecutionEngine)
        def _pandas(
            cls,
            execution_engine: PandasExecutionEngine,
            metric_domain_kwargs: dict,
            metric_value_kwargs: dict,
            metrics: Dict[str, Any],
            runtime_configuration: dict,
        ):
            raise NotImplementedError

        @metric_value(engine=SqlAlchemyExecutionEngine)
        def _sqlalchemy(
            cls,
            execution_engine: SqlAlchemyExecutionEngine,
            metric_domain_kwargs: dict,
            metric_value_kwargs: dict,
            metrics: Dict[str, Any],
            runtime_configuration: dict,
        ):
            raise NotImplementedError

        @metric_value(engine=SparkDFExecutionEngine)
        def _spark(
            cls,
            execution_engine: SparkDFExecutionEngine,
            metric_domain_kwargs: dict,
            metric_value_kwargs: dict,
            metrics: Dict[str, Any],
            runtime_configuration: dict,
        ):
            raise NotImplementedError

    CustomMetricProvider()

    assert len(mock_registry._registered_metrics.keys()) == prev_registered_metric_key_count + 1
    assert "custom_metric" in mock_registry._registered_metrics


def test__table_metric_provider__registration(mock_registry):
    """This tests whether the TableMetricProvider class registers the correct metrics."""
    registered_metric_keys = list(mock_registry._registered_metrics.keys())
    for key in registered_metric_keys:
        assert "table.custom_metric" not in key

    prev_registered_metric_key_count = len(registered_metric_keys)

    class CustomTableMetricProvider(TableMetricProvider):
        metric_name = "table.custom_metric"

        @metric_value(engine=PandasExecutionEngine)
        def _pandas(
            cls,
            execution_engine: PandasExecutionEngine,
            metric_domain_kwargs: dict,
            metric_value_kwargs: dict,
            metrics: Dict[str, Any],
            runtime_configuration: dict,
        ):
            raise NotImplementedError

        @metric_value(engine=SqlAlchemyExecutionEngine)
        def _sqlalchemy(
            cls,
            execution_engine: SqlAlchemyExecutionEngine,
            metric_domain_kwargs: dict,
            metric_value_kwargs: dict,
            metrics: Dict[str, Any],
            runtime_configuration: dict,
        ):
            raise NotImplementedError

        @metric_value(engine=SparkDFExecutionEngine)
        def _spark(
            cls,
            execution_engine: SparkDFExecutionEngine,
            metric_domain_kwargs: dict,
            metric_value_kwargs: dict,
            metrics: Dict[str, Any],
            runtime_configuration: dict,
        ):
            raise NotImplementedError

    CustomTableMetricProvider()

    assert len(mock_registry._registered_metrics.keys()) == prev_registered_metric_key_count + 1
    assert "table.custom_metric" in mock_registry._registered_metrics


def test__column_map_metric__registration(mock_registry):
    """This tests whether the ColumnMapMetricProvider class registers the correct metrics.

    The actual logic for this lives in the private method: `_register_metric_functions`, which is invoked from within `__new__` for the ancestor class `MetricProvider`.

    Since _register_metric_functions is private, we don't want to test it directly. Instead, we declare a custom ColumnMapMetricProvider, and test that the correct metrics are registered.
    """  # noqa: E501
    registered_metric_keys = list(mock_registry._registered_metrics.keys())
    for key in registered_metric_keys:
        assert "column_values.equal_seven" not in key

    prev_registered_metric_key_count = len(registered_metric_keys)

    class CustomColumnValuesEqualSeven(ColumnMapMetricProvider):
        condition_metric_name = "column_values.equal_seven"

        @column_condition_partial(engine=PandasExecutionEngine)
        def _pandas(cls, column, **kwargs):
            return column == 7

        @column_condition_partial(engine=SqlAlchemyExecutionEngine)
        def _sqlalchemy(cls, column, **kwargs):
            # return column.in_([3])
            return column.is_(7)

        @column_condition_partial(engine=SparkDFExecutionEngine)
        def _spark(cls, column, **kwargs):
            return column.contains(7)

    CustomColumnValuesEqualSeven()

    assert len(mock_registry._registered_metrics.keys()) == prev_registered_metric_key_count + 8

    new_keys = [
        "column_values.equal_seven.condition",
        "column_values.equal_seven.unexpected_count",
        "column_values.equal_seven.unexpected_index_list",
        "column_values.equal_seven.unexpected_index_query",
        "column_values.equal_seven.unexpected_rows",
        "column_values.equal_seven.unexpected_values",
        "column_values.equal_seven.unexpected_value_counts",
        "column_values.equal_seven.unexpected_count.aggregate_fn",
    ]
    for key in new_keys:
        assert key in mock_registry._registered_metrics


def test__column_pair_map_metric__registration(mock_registry):
    """This tests whether the ColumnPairMapMetricProvider class registers the correct metrics."""
    registered_metric_keys = list(mock_registry._registered_metrics.keys())
    for key in registered_metric_keys:
        assert "column_pair_values.equal_seven" not in key

    prev_registered_metric_key_count = len(registered_metric_keys)

    class CustomColumnPairValuesEqualSeven(ColumnPairMapMetricProvider):
        condition_metric_name = "column_pair_values.equal_seven"

        @column_pair_condition_partial(engine=PandasExecutionEngine)
        def _pandas(cls, column_A, column_B, **kwargs):
            raise NotImplementedError

        @column_pair_condition_partial(engine=SqlAlchemyExecutionEngine)
        def _sqlalchemy(cls, column_A, column_B, _dialect, **kwargs):
            raise NotImplementedError

        @column_pair_condition_partial(engine=SparkDFExecutionEngine)
        def _spark(cls, column_A, column_B, **kwargs):
            raise NotImplementedError

    CustomColumnPairValuesEqualSeven()

    assert len(mock_registry._registered_metrics.keys()) == prev_registered_metric_key_count + 7

    for key in mock_registry._registered_metrics:
        if "column_pair_values.equal_seven" in key:
            print(key)

    new_keys = [
        "column_pair_values.equal_seven.condition",
        "column_pair_values.equal_seven.unexpected_count",
        "column_pair_values.equal_seven.unexpected_index_list",
        "column_pair_values.equal_seven.unexpected_index_query",
        "column_pair_values.equal_seven.unexpected_rows",
        "column_pair_values.equal_seven.unexpected_values",
        "column_pair_values.equal_seven.filtered_row_count",
    ]
    for key in new_keys:
        assert key in mock_registry._registered_metrics


def test__multicolumn_map_metric__registration(mock_registry):
    """This tests whether the MultiColumnMapMetricProvider class registers the correct metrics."""
    registered_metric_keys = list(mock_registry._registered_metrics.keys())
    for key in registered_metric_keys:
        assert "multicolumn_values.equal_seven" not in key

    prev_registered_metric_key_count = len(registered_metric_keys)

    class CustomMultiColumnValuesEqualSeven(MulticolumnMapMetricProvider):
        condition_metric_name = "multicolumn_values.equal_seven"

        condition_domain_keys = (
            "batch_id",
            "table",
            "column_list",
            "row_condition",
            "condition_parser",
            "ignore_row_if",
        )
        condition_value_keys = ()

        @multicolumn_condition_partial(engine=PandasExecutionEngine)
        def _pandas(cls, column_list, **kwargs):
            raise NotImplementedError

        @multicolumn_condition_partial(engine=SqlAlchemyExecutionEngine)
        def _sqlalchemy(cls, column_list, **kwargs):
            raise NotImplementedError

        @multicolumn_condition_partial(engine=SparkDFExecutionEngine)
        def _spark(cls, column_list, **kwargs):
            raise NotImplementedError

    CustomMultiColumnValuesEqualSeven()

    assert len(mock_registry._registered_metrics.keys()) == prev_registered_metric_key_count + 7

    new_keys = [
        "multicolumn_values.equal_seven.condition",
        "multicolumn_values.equal_seven.unexpected_count",
        "multicolumn_values.equal_seven.unexpected_index_list",
        "multicolumn_values.equal_seven.unexpected_index_query",
        "multicolumn_values.equal_seven.unexpected_rows",
        "multicolumn_values.equal_seven.unexpected_values",
        "multicolumn_values.equal_seven.filtered_row_count",
    ]
    for key in new_keys:
        assert key in mock_registry._registered_metrics


def test__query_metric_provider__registration(mock_registry):
    """This tests whether the QueryMetricProvider class registers the correct metrics."""
    registered_metric_keys = list(mock_registry._registered_metrics.keys())
    for key in registered_metric_keys:
        assert "query.custom_metric" not in key

    prev_registered_metric_key_count = len(registered_metric_keys)

    class CustomQueryMetricProvider(QueryMetricProvider):
        metric_name = "query.custom_metric"

        @metric_value(engine=SqlAlchemyExecutionEngine)
        def _sqlalchemy(
            cls,
            execution_engine: SqlAlchemyExecutionEngine,
            metric_domain_kwargs: dict,
            metric_value_kwargs: dict,
            metrics: Dict[str, Any],
            runtime_configuration: dict,
        ) -> List[dict]:
            raise NotImplementedError

        @metric_value(engine=SparkDFExecutionEngine)
        def _spark(
            cls,
            execution_engine: SparkDFExecutionEngine,
            metric_domain_kwargs: dict,
            metric_value_kwargs: dict,
            metrics: Dict[str, Any],
            runtime_configuration: dict,
        ) -> List[dict]:
            raise NotImplementedError

    CustomQueryMetricProvider()

    assert len(mock_registry._registered_metrics.keys()) == prev_registered_metric_key_count + 1
    assert "query.custom_metric" in mock_registry._registered_metrics


@pytest.mark.unit
@pytest.mark.parametrize(
    "input_query,expected_query",
    [
        (
            "SELECT * FROM {batch}",
            "SELECT * FROM iris WHERE datetime_column = '01/12/2024'",
        ),
        (
            "SELECT * FROM {batch} WHERE passenger_count > 7",
            "SELECT * FROM iris WHERE datetime_column = '01/12/2024' AND passenger_count > 7",
        ),
        (
            "SELECT * FROM {batch} WHERE passenger_count > 7 ORDER BY iris.'PetalLengthCm' DESC",
            "SELECT * FROM iris WHERE datetime_column = '01/12/2024' "
            "AND passenger_count > 7 ORDER BY iris.'PetalLengthCm' DESC",
        ),
        (
            "SELECT * FROM {batch} WHERE passenger_count > 7 GROUP BY iris.'Species' DESC",
            "SELECT * FROM iris WHERE datetime_column = '01/12/2024' "
            "AND passenger_count > 7 GROUP BY iris.'Species' DESC",
        ),
    ],
)
def test__get_query_string_with_substituted_batch_parameters(input_query: str, expected_query: str):
    batch_subquery = (
        sa.select("*")
        .select_from(sa.text("iris"))
        .where(sa.text("datetime_column = '01/12/2024'"))
        .subquery()
    )
    actual_query = QueryMetricProvider._get_query_string_with_substituted_batch_parameters(
        query=input_query,
        batch_subquery=batch_subquery,
    )
    assert actual_query == expected_query


@pytest.mark.unit
@pytest.mark.parametrize(
    "query_parameters,expected_dict",
    [
        (
            None,
            {},
        ),
        (
            QueryParameters(),
            {},
        ),
        (
            QueryParameters(column="my_column"),
            {"column": "my_column"},
        ),
        (
            QueryParameters(column_A="my_column_A", column_B="my_column_B"),
            {"column_A": "my_column_A", "column_B": "my_column_B"},
        ),
        (
            QueryParameters(columns=["my_column_A", "my_column_B", "my_column_C"]),
            {"col_1": "my_column_A", "col_2": "my_column_B", "col_3": "my_column_C"},
        ),
    ],
)
def test__get_parameters_dict_from_query_parameters(
    query_parameters: QueryParameters, expected_dict: dict
):
    actual_dict = QueryMetricProvider._get_parameters_dict_from_query_parameters(query_parameters)
    assert actual_dict == expected_dict


@pytest.mark.unit
@mock.patch.object(sa, "text")
@pytest.mark.parametrize(
    "batch_selectable,expected_query",
    [
        (
            sa.table("my_table"),
            "SELECT my_column FROM (my_table) WHERE passenger_count > 7",
        ),
        (
            sa.select("*").select_from(sa.text("my_table")).subquery(),
            "SELECT my_column FROM (SELECT * \nFROM my_table) WHERE passenger_count > 7",
        ),
        (
            sa.select("*").select_from(sa.text("my_table")),
            "SELECT my_column FROM (SELECT * \nFROM my_table) AS subselect "
            "WHERE passenger_count > 7",
        ),
    ],
)
def test__get_sqlalchemy_records_from_query_and_batch_selectable__query(
    mock_sqlalchemy_text, batch_selectable: sa.Selectable, expected_query: str
):
    execution_engine = MockSqlAlchemyExecutionEngine()
    mock_sqlalchemy_text.return_value = "*"
    with mock.patch.object(execution_engine, "execute_query"):
        QueryMetricProvider._get_sqlalchemy_records_from_query_and_batch_selectable(
            query="SELECT {column} FROM {batch} WHERE passenger_count > 7",
            batch_selectable=batch_selectable,
            execution_engine=execution_engine,
            query_parameters=QueryParameters(column="my_column"),
        )
    mock_sqlalchemy_text.assert_called_with(expected_query)


@pytest.mark.unit
@mock.patch.object(MockResult, "fetchmany")
def test__get_sqlalchemy_records_from_query_and_batch_selectable__record_count(
    mock_sqlalchemy_fetchmany,
):
    execution_engine = MockSqlAlchemyExecutionEngine()
    mock_sqlalchemy_fetchmany.return_value = []
    QueryMetricProvider._get_sqlalchemy_records_from_query_and_batch_selectable(
        query="SELECT * FROM {batch} WHERE passenger_count > 7",
        batch_selectable=sa.select("*").select_from(sa.text("my_table")).subquery(),
        execution_engine=execution_engine,
    )
    mock_sqlalchemy_fetchmany.assert_called_with(MAX_RESULT_RECORDS)
