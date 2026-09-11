"""A regex metric that cannot compile for the active dialect must say so.

Six ``_sqlalchemy`` implementations share one fallback: when
``get_dialect_regex_expression`` has no branch for the active dialect it returns
``None`` and the metric raises ``NotImplementedError``. The message that exception
carries is the only statement of the cause that reaches
``ExpectationValidationResult.exception_info`` -- a bare ``raise`` records an empty
``exception_message``, which is indistinguishable from a regex that simply matched
nothing.

These tests pin the message at each of the six raise sites. The end-to-end
propagation of that message through the validation machinery is covered against a
live SQL Server in
``tests/integration/data_sources_and_expectations/expectations/test_expect_column_values_to_match_regex.py``.

Those six sites are reachable from two dialect shapes, and both must behave alike.
``SqlAlchemyExecutionEngine`` resolves a dialect *module* for most backends, but leaves
``dialect_module`` as ``None`` for five of them (``awsathena``, ``exasol``, ``hive``,
``vertica``, and the ``other`` fallback), whose
callers hand the helper the live dialect *instance* instead. The message builder reads
``name`` off either shape, so both must yield the same named ``NotImplementedError``.
``StubSqlAlchemyExecutionEngine`` covers the module shape;
``ModuleLessStubSqlAlchemyExecutionEngine`` covers the instance shape. The latter's
cases are the regression guard for the two aggregate providers' former
``assert _dialect is not None``, which turned every module-less dialect into a bare
``AssertionError`` before any message could be built.

The instance shape has a positive half too. For Exasol the helper *does* have a branch,
so a module-less engine must build a query rather than raise at all:
``test_module_less_exasol_dialect_reaches_the_regex_branch`` pins the ``WHERE`` clause
the two aggregate providers emit for it. That pins a rendering only. Whether the server
reads the predicate as the substring search the metric contract requires is pinned by
the live-backend cases in
``tests/integration/metrics/column/test_values_match_regex_values.py`` and its
not-match twin.
"""

from types import SimpleNamespace
from typing import Any, Callable

import pytest

from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.expectations.metrics.column_aggregate_metrics import (
    ColumnValuesMatchRegexValues,
    ColumnValuesNotMatchRegexValues,
)
from great_expectations.expectations.metrics.column_map_metrics import (
    ColumnValuesMatchRegex,
    ColumnValuesMatchRegexList,
    ColumnValuesNotMatchRegex,
    ColumnValuesNotMatchRegexList,
)

# SQL Server has no branch in get_dialect_regex_expression, and this module is what
# SqlAlchemyExecutionEngine hands the metrics as `_dialect` for that backend.
MSSQL_DIALECT_MODULE = sa.dialects.mssql

EXPECTED_MESSAGE = "Regex is not supported for dialect mssql"

# The same backend as a live dialect *instance* -- the shape a module-less engine offers.
# It carries no `dialect` attribute of its own, so the raise sites' message builder,
# `getattr(getattr(_dialect, "dialect", _dialect), "name", str(_dialect))`, falls through
# to `.name` and yields the same "mssql" as the module above.
MSSQL_DIALECT_INSTANCE = MSSQL_DIALECT_MODULE.dialect()

# What the Exasol branch of `get_dialect_regex_expression` renders for the regex `test` on
# the column `a`. The not-match provider wraps the positive expression in `sa.not_()`, which
# parenthesises the infix predicate rather than switching to the `NOT REGEXP_LIKE` token.
# `great_expectations/expectations/metrics/util.py` explains why each part of the wrapped
# pattern is load-bearing.
EXASOL_MATCH_WHERE = "a REGEXP_LIKE '(*LF)(?s:.*)(?:test)(?s:.*)'"
EXASOL_NOT_MATCH_WHERE = "NOT (a REGEXP_LIKE '(*LF)(?s:.*)(?:test)(?s:.*)')"


def _undecorated(metric_fn: Callable) -> Callable:
    """Peel the metric decorators off, leaving the implementation itself."""
    while hasattr(metric_fn, "__wrapped__"):
        metric_fn = metric_fn.__wrapped__
    return metric_fn


class StubSqlAlchemyExecutionEngine:
    """The little of the execution engine the aggregate metrics touch before raising."""

    dialect_module = MSSQL_DIALECT_MODULE

    def get_compute_domain(
        self, metric_domain_kwargs: dict, domain_type: Any
    ) -> tuple[Any, dict, dict]:
        return sa.table("test_table"), {}, {"column": "test_column"}


class NamedDialectStub:
    """A dialect instance offering only `name`, the attribute the Exasol branch reads.

    There is no Exasol dialect class in core to instantiate, and `sqlalchemy_exasol` is
    not a test dependency. `get_dialect_regex_expression` reaches its Exasol entry by
    name, after every earlier branch's `dialect.dialect` lookup raises `AttributeError`
    and every `hasattr` probe returns False -- which is exactly what this object does.
    """

    def __init__(self, name: str) -> None:
        self.name = name


class ModuleLessStubSqlAlchemyExecutionEngine:
    """The engine shape a module-less backend presents to the aggregate metrics.

    `dialect_module` is None, as it is left unset for Exasol and four other backends, and
    the live dialect instance is offered on `dialect` instead.
    Deliberately not a subclass of `StubSqlAlchemyExecutionEngine`: that stub sets
    `dialect_module`, so it never met the assertion this pair of stubs exists to pin, and
    it must keep proving the module path unchanged.

    `execute_query` records what it was handed rather than running it, so a test can
    assert on the query a provider built once it gets past dialect resolution.
    """

    dialect_module = None

    def __init__(self, dialect: Any) -> None:
        self.dialect = dialect
        self.captured_query: Any = None

    def get_compute_domain(
        self, metric_domain_kwargs: dict, domain_type: Any
    ) -> tuple[Any, dict, dict]:
        return sa.table("test_table"), {}, {"column": "a"}

    def execute_query(self, query: Any) -> Any:
        self.captured_query = query
        return SimpleNamespace(fetchall=lambda: [])


def _call_column_map_metric(metric_cls: Any, **kwargs: Any) -> None:
    _undecorated(metric_cls._sqlalchemy)(
        metric_cls,
        sa.column("test_column"),
        _dialect=MSSQL_DIALECT_MODULE,
        **kwargs,
    )


def _call_column_aggregate_metric(metric_cls: Any) -> None:
    _undecorated(metric_cls._sqlalchemy)(
        metric_cls,
        execution_engine=StubSqlAlchemyExecutionEngine(),
        metric_domain_kwargs={},
        metric_value_kwargs={"regex": "^abc$", "limit": None},
        metrics={},
        runtime_configuration={},
    )


def _call_module_less_aggregate_metric(
    metric_cls: Any, dialect: Any, regex: str
) -> ModuleLessStubSqlAlchemyExecutionEngine:
    """Drive an aggregate provider with an engine whose `dialect_module` is None."""
    execution_engine = ModuleLessStubSqlAlchemyExecutionEngine(dialect)
    _undecorated(metric_cls._sqlalchemy)(
        metric_cls,
        execution_engine=execution_engine,
        metric_domain_kwargs={},
        metric_value_kwargs={"regex": regex, "limit": None},
        metrics={},
        runtime_configuration={},
    )
    return execution_engine


@pytest.mark.unit
@pytest.mark.parametrize(
    "call_metric",
    [
        pytest.param(
            lambda: _call_column_map_metric(ColumnValuesMatchRegex, regex="^abc$"),
            id="column_values.match_regex",
        ),
        pytest.param(
            lambda: _call_column_map_metric(ColumnValuesNotMatchRegex, regex="^abc$"),
            id="column_values.not_match_regex",
        ),
        pytest.param(
            lambda: _call_column_map_metric(
                ColumnValuesMatchRegexList, regex_list=["^abc$"], match_on="any"
            ),
            id="column_values.match_regex_list",
        ),
        pytest.param(
            lambda: _call_column_map_metric(ColumnValuesNotMatchRegexList, regex_list=["^abc$"]),
            id="column_values.not_match_regex_list",
        ),
        pytest.param(
            lambda: _call_column_aggregate_metric(ColumnValuesMatchRegexValues),
            id="column_values.match_regex_values",
        ),
        pytest.param(
            lambda: _call_column_aggregate_metric(ColumnValuesNotMatchRegexValues),
            id="column_values.not_match_regex_values",
        ),
        # The same two providers reached through the module-less shape. Before the live
        # dialect fallback these raised `AssertionError` with an empty message instead,
        # so they never got as far as the raise site the other cases pin.
        pytest.param(
            lambda: _call_module_less_aggregate_metric(
                ColumnValuesMatchRegexValues, MSSQL_DIALECT_INSTANCE, "^abc$"
            ),
            id="column_values.match_regex_values-module-less",
        ),
        pytest.param(
            lambda: _call_module_less_aggregate_metric(
                ColumnValuesNotMatchRegexValues, MSSQL_DIALECT_INSTANCE, "^abc$"
            ),
            id="column_values.not_match_regex_values-module-less",
        ),
    ],
)
def test_regex_metric_names_the_unsupported_dialect(call_metric: Callable[[], None]) -> None:
    """Every raise site carries the same message, naming the dialect, not a module repr."""
    with pytest.raises(NotImplementedError) as exc_info:
        call_metric()

    assert str(exc_info.value) == EXPECTED_MESSAGE


@pytest.mark.unit
@pytest.mark.parametrize(
    ("metric_cls", "expected_where"),
    [
        pytest.param(ColumnValuesMatchRegexValues, EXASOL_MATCH_WHERE, id="match"),
        pytest.param(ColumnValuesNotMatchRegexValues, EXASOL_NOT_MATCH_WHERE, id="not_match"),
    ],
)
def test_module_less_exasol_dialect_reaches_the_regex_branch(
    metric_cls: Any, expected_where: str
) -> None:
    """The positive half of the module-less path: Exasol has a branch, so nothing raises.

    Exasol is the only dialect that is both module-less and owns a branch in
    `get_dialect_regex_expression`; the other four module-less backends have none, and the
    other twelve branches belong to backends the engine resolves a module for. So for
    Exasol alone the former `assert _dialect is not None` cost the capability rather than
    only the diagnostic that the cases above pin.

    Merely completing proves the fix: an `AssertionError` from the old line 33 or a
    `NotImplementedError` from the raise site would surface here as a failure. What is
    asserted is the query the provider went on to build -- and the two providers differ
    only here, the not-match one wrapping the same expression in `sa.not_()`.

    This pins a rendering. It says nothing about how the server reads the predicate, since
    a whole-string and a substring reading compile to identical text; that is settled by
    `test_partial_match_characters[exasol]` in
    `tests/integration/metrics/column/test_values_match_regex_values.py` and its not-match
    twin, which run these providers against a live container.
    """
    execution_engine = _call_module_less_aggregate_metric(
        metric_cls, NamedDialectStub("exasol"), "test"
    )

    assert execution_engine.captured_query is not None, (
        "the provider resolved a dialect but never reached execute_query, so no query was "
        "built to assert on"
    )
    rendered = str(
        execution_engine.captured_query.whereclause.compile(compile_kwargs={"literal_binds": True})
    )
    assert rendered == expected_where
