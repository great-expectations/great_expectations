from __future__ import annotations

import copy
from typing import Any, Dict, List, Type
from unittest import mock

import pytest

from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.constants import MAX_RESULT_RECORDS
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations import registry
from great_expectations.expectations.metrics.column_map_metrics.column_values_unique import (
    _sqlalchemy_unique_unexpected_index_list,
    _sqlalchemy_unique_unexpected_index_query,
    _sqlalchemy_unique_unexpected_rows,
)
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    ColumnPairMapMetricProvider,
    MulticolumnMapMetricProvider,
)
from great_expectations.expectations.metrics.map_metric_provider.column_condition_partial import (
    column_condition_partial,
)
from great_expectations.expectations.metrics.map_metric_provider.column_pair_condition_partial import (  # noqa: E501 # FIXME CoP
    column_pair_condition_partial,
)
from great_expectations.expectations.metrics.map_metric_provider.multicolumn_condition_partial import (  # noqa: E501 # FIXME CoP
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
from tests.expectations.metrics.conftest import MockResult, MockSqlAlchemyExecutionEngine

pytestmark = pytest.mark.unit


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


def _make_custom_metric_provider() -> Type[MetricProvider]:
    # A module-level, fully-typed factory rather than a class nested directly in the test:
    # mypy's disallow-untyped-decorators can't resolve `metric_value`'s ParamSpec for a class
    # defined inside an (intentionally unannotated) test function, and reports the decorated
    # methods below as untyped even though `metric_value` itself is fully typed.
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

    return CustomMetricProvider


def test__base_metric_provider__registration(mock_registry):
    """This tests whether the MetricProvider class registers the correct metrics."""
    registered_metric_keys = list(mock_registry._registered_metrics.keys())
    for key in registered_metric_keys:
        assert "custom_metric" not in key

    prev_registered_metric_key_count = len(registered_metric_keys)

    CustomMetricProvider = _make_custom_metric_provider()
    CustomMetricProvider()

    assert len(mock_registry._registered_metrics.keys()) == prev_registered_metric_key_count + 1
    assert "custom_metric" in mock_registry._registered_metrics


def _make_custom_table_metric_provider() -> Type[TableMetricProvider]:
    # See _make_custom_metric_provider's comment: kept out of the test function's body so
    # mypy can resolve metric_value's ParamSpec against the decorated methods below.
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

    return CustomTableMetricProvider


def test__table_metric_provider__registration(mock_registry):
    """This tests whether the TableMetricProvider class registers the correct metrics."""
    registered_metric_keys = list(mock_registry._registered_metrics.keys())
    for key in registered_metric_keys:
        assert "table.custom_metric" not in key

    prev_registered_metric_key_count = len(registered_metric_keys)

    CustomTableMetricProvider = _make_custom_table_metric_provider()
    CustomTableMetricProvider()

    assert len(mock_registry._registered_metrics.keys()) == prev_registered_metric_key_count + 1
    assert "table.custom_metric" in mock_registry._registered_metrics


def test__column_map_metric__registration(mock_registry):
    """This tests whether the ColumnMapMetricProvider class registers the correct metrics.

    The actual logic for this lives in the private method: `_register_metric_functions`, which is invoked from within `__new__` for the ancestor class `MetricProvider`.

    Since _register_metric_functions is private, we don't want to test it directly. Instead, we declare a custom ColumnMapMetricProvider, and test that the correct metrics are registered.
    """  # noqa: E501 # FIXME CoP
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


def test__map_metric_provider__sqlalchemy_row_retrieval_provider_hooks(
    mock_registry, caplog: pytest.LogCaptureFixture
):
    """MapMetricProvider exposes a per-metric override surface for the three SqlAlchemy
    row-retrieval providers (unexpected_rows / unexpected_index_list / unexpected_index_query):
    `sqlalchemy_unexpected_rows_provider`, `sqlalchemy_unexpected_index_list_provider`, and
    `sqlalchemy_unexpected_index_query_provider`. A subclass overriding these class attributes
    gets its custom provider registered exactly once -- no super()-then-re-register, no
    "overwriting metric_provider" warning.
    """  # FIXME CoP

    def _custom_rows(cls, **kwargs):
        raise NotImplementedError

    def _custom_index_list(cls, **kwargs):
        raise NotImplementedError

    def _custom_index_query(cls, **kwargs):
        raise NotImplementedError

    class CustomColumnValuesEqualEight(ColumnMapMetricProvider):
        condition_metric_name = "column_values.equal_eight"

        sqlalchemy_unexpected_rows_provider = staticmethod(_custom_rows)
        sqlalchemy_unexpected_index_list_provider = staticmethod(_custom_index_list)
        sqlalchemy_unexpected_index_query_provider = staticmethod(_custom_index_query)

        @column_condition_partial(engine=PandasExecutionEngine)
        def _pandas(cls, column, **kwargs):
            return column == 8

        @column_condition_partial(engine=SqlAlchemyExecutionEngine)
        def _sqlalchemy(cls, column, **kwargs):
            return column.is_(8)

        @column_condition_partial(engine=SparkDFExecutionEngine)
        def _spark(cls, column, **kwargs):
            return column.contains(8)

    with caplog.at_level("WARNING"):
        CustomColumnValuesEqualEight()

    assert "overwriting metric_provider" not in caplog.text

    _, rows_fn = mock_registry.get_sqlalchemy_metric_provider(
        "column_values.equal_eight.unexpected_rows"
    )
    _, index_list_fn = mock_registry.get_sqlalchemy_metric_provider(
        "column_values.equal_eight.unexpected_index_list"
    )
    _, index_query_fn = mock_registry.get_sqlalchemy_metric_provider(
        "column_values.equal_eight.unexpected_index_query"
    )

    assert rows_fn is _custom_rows
    assert index_list_fn is _custom_index_list
    assert index_query_fn is _custom_index_query


def test__column_values_unique__sqlalchemy_row_retrieval_providers_are_narrow(mock_registry):
    """Regression guard for `column_values.unique`'s SqlAlchemy row-retrieval providers.

    `ColumnValuesUnique` overrides the `MapMetricProvider` row-retrieval provider hooks
    (`sqlalchemy_unexpected_rows_provider` / `sqlalchemy_unexpected_index_list_provider` /
    `sqlalchemy_unexpected_index_query_provider`) with narrow, single-scan providers that
    join a dup-keys subquery back to source instead of dragging every table column
    through the window sort. This test guards against silently reverting to the generic
    (wide-row) providers.
    """  # FIXME CoP
    _, rows_fn = mock_registry.get_sqlalchemy_metric_provider(
        "column_values.unique.unexpected_rows"
    )
    _, index_list_fn = mock_registry.get_sqlalchemy_metric_provider(
        "column_values.unique.unexpected_index_list"
    )
    _, index_query_fn = mock_registry.get_sqlalchemy_metric_provider(
        "column_values.unique.unexpected_index_query"
    )

    assert rows_fn is _sqlalchemy_unique_unexpected_rows
    assert index_list_fn is _sqlalchemy_unique_unexpected_index_list
    assert index_query_fn is _sqlalchemy_unique_unexpected_index_query


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


def _make_custom_query_metric_provider() -> Type[QueryMetricProvider]:
    # See _make_custom_metric_provider's comment: kept out of the test function's body so
    # mypy can resolve metric_value's ParamSpec against the decorated methods below.
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

    return CustomQueryMetricProvider


def test__query_metric_provider__registration(mock_registry):
    """This tests whether the QueryMetricProvider class registers the correct metrics."""
    registered_metric_keys = list(mock_registry._registered_metrics.keys())
    for key in registered_metric_keys:
        assert "query.custom_metric" not in key

    prev_registered_metric_key_count = len(registered_metric_keys)

    CustomQueryMetricProvider = _make_custom_query_metric_provider()
    CustomQueryMetricProvider()

    assert len(mock_registry._registered_metrics.keys()) == prev_registered_metric_key_count + 1
    assert "query.custom_metric" in mock_registry._registered_metrics


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
            "SELECT my_column FROM (SELECT * \nFROM my_table) AS subselect "
            "WHERE passenger_count > 7",
        ),
        (
            sa.select("*").select_from(sa.text("my_table")),
            "SELECT my_column FROM (SELECT * \nFROM my_table) AS subselect "
            "WHERE passenger_count > 7",
        ),
    ],
)
def test_get_sqlalchemy_records_from_query_and_batch_selectable__query(
    mock_sqlalchemy_text,
    mock_sqlalchemy_execution_engine: MockSqlAlchemyExecutionEngine,
    batch_selectable: sa.Selectable,
    expected_query: str,
):
    mock_sqlalchemy_text.return_value = "*"
    with mock.patch.object(mock_sqlalchemy_execution_engine, "execute_query"):
        substituted_batch_subquery = (
            QueryMetricProvider._get_substituted_batch_subquery_from_query_and_batch_selectable(
                query="SELECT {column} FROM {batch} WHERE passenger_count > 7",
                batch_selectable=batch_selectable,
                execution_engine=mock_sqlalchemy_execution_engine,
                query_parameters=QueryParameters(column="my_column"),
            )
        )
        QueryMetricProvider._get_sqlalchemy_records_from_substituted_batch_subquery(
            substituted_batch_subquery=substituted_batch_subquery,
            execution_engine=mock_sqlalchemy_execution_engine,
        )
    assert substituted_batch_subquery == expected_query
    mock_sqlalchemy_text.assert_called_with(expected_query)


@pytest.mark.unit
@mock.patch.object(MockResult, "fetchmany")
def test_get_sqlalchemy_records_from_query_and_batch_selectable__record_count(
    mock_sqlalchemy_fetchmany,
    mock_sqlalchemy_execution_engine: MockSqlAlchemyExecutionEngine,
):
    mock_sqlalchemy_fetchmany.return_value = []
    substituted_batch_subquery = (
        QueryMetricProvider._get_substituted_batch_subquery_from_query_and_batch_selectable(
            query="SELECT * FROM {batch} WHERE passenger_count > 7",
            batch_selectable=sa.select("*").select_from(sa.text("my_table")).subquery(),
            execution_engine=mock_sqlalchemy_execution_engine,
            query_parameters=QueryParameters(column="my_column"),
        )
    )
    QueryMetricProvider._get_sqlalchemy_records_from_substituted_batch_subquery(
        substituted_batch_subquery=substituted_batch_subquery,
        execution_engine=mock_sqlalchemy_execution_engine,
    )
    mock_sqlalchemy_fetchmany.assert_called_with(MAX_RESULT_RECORDS)
