from inspect import unwrap
from types import ModuleType
from typing import Any

import pytest
from sqlalchemy.dialects.mysql.base import MySQLDialect
from sqlalchemy.dialects.oracle.base import OracleDialect

from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.expectations.metrics.column_map_metrics import (
    column_values_match_like_pattern,
    column_values_match_like_pattern_list,
    column_values_not_match_like_pattern,
    column_values_not_match_like_pattern_list,
)
from great_expectations.expectations.metrics.util import (
    get_dialect_like_pattern_expression,
)


class _DialectModule(ModuleType):
    dialect: type[Any]


class _OracleDialectSubclass(OracleDialect):
    pass


class _SingleStoreDialectSubclass(MySQLDialect):
    name = "singlestoredb"


@pytest.mark.unit
@pytest.mark.parametrize(
    "dialect_class",
    [
        pytest.param(_OracleDialectSubclass, id="oracle"),
        pytest.param(_SingleStoreDialectSubclass, id="singlestore"),
    ],
)
@pytest.mark.parametrize("positive", [True, False], ids=["match", "not-match"])
def test_like_pattern_expression_supports_oracle_and_singlestore_dialect_subclasses(
    dialect_class: type[Any],
    positive: bool,
) -> None:
    dialect = _DialectModule("test_dialect")
    dialect.dialect = dialect_class

    expression = get_dialect_like_pattern_expression(
        column=sa.Column("value", sa.String()),
        dialect=dialect,
        like_pattern="foo%",
        positive=positive,
    )

    assert expression is not None
    sql = str(
        expression.compile(
            dialect=dialect_class(paramstyle="named"), compile_kwargs={"literal_binds": True}
        )
    )
    if positive:
        expected_sql = "value LIKE 'foo%'"
    else:
        expected_sql = "value NOT LIKE 'foo%'"
    assert sql == expected_sql


@pytest.mark.unit
@pytest.mark.parametrize(
    ("metric_provider", "metric_kwargs"),
    [
        pytest.param(
            column_values_match_like_pattern.ColumnValuesMatchLikePattern,
            {"like_pattern": "foo%"},
            id="match-one",
        ),
        pytest.param(
            column_values_not_match_like_pattern.ColumnValuesNotMatchLikePattern,
            {"like_pattern": "foo%"},
            id="not-match-one",
        ),
        pytest.param(
            column_values_match_like_pattern_list.ColumnValuesMatchLikePatternList,
            {"like_pattern_list": ["foo%"], "match_on": "any"},
            id="match-list",
        ),
        pytest.param(
            column_values_not_match_like_pattern_list.ColumnValuesNotMatchLikePatternList,
            {"like_pattern_list": ["foo%"]},
            id="not-match-list",
        ),
    ],
)
@pytest.mark.parametrize("as_module", [True, False], ids=["module", "instance"])
def test_unsupported_dialect_raises_not_implemented_error(
    metric_provider: Any,
    metric_kwargs: dict[str, Any],
    as_module: bool,
) -> None:
    class UnsupportedDialect:
        name = "unsupported"

    metric_fn = unwrap(metric_provider._sqlalchemy)

    dialect: _DialectModule | UnsupportedDialect
    if as_module:
        dialect = _DialectModule("test_unsupported_dialect")
        dialect.dialect = UnsupportedDialect
    else:
        dialect = UnsupportedDialect()

    with pytest.raises(
        NotImplementedError, match=r"^Like patterns are not supported for dialect unsupported$"
    ):
        metric_fn(
            metric_provider,
            column=sa.Column("value", sa.String()),
            _dialect=dialect,
            **metric_kwargs,
        )


@pytest.mark.unit
@pytest.mark.parametrize("positive", [True, False], ids=["match", "not-match"])
def test_singlestore_translates_explicit_escape(positive: bool) -> None:
    """SingleStore has no ESCAPE clause, so the pattern is rewritten to backslash escapes."""
    dialect = _DialectModule("test_singlestore_dialect")
    dialect.dialect = _SingleStoreDialectSubclass

    expression = get_dialect_like_pattern_expression(
        column=sa.Column("value", sa.String()),
        dialect=dialect,
        like_pattern="a!_b",
        positive=positive,
        escape="!",
    )

    assert expression is not None
    assert expression.modifiers.get("escape") is None
    assert expression.right.value == r"a\_b"


@pytest.mark.unit
@pytest.mark.parametrize("dialect_class", [OracleDialect, MySQLDialect], ids=["oracle", "mysql"])
@pytest.mark.parametrize("positive", [True, False], ids=["match", "not-match"])
def test_oracle_and_mysql_still_support_explicit_escape(
    dialect_class: type[Any], positive: bool
) -> None:
    dialect = _DialectModule("test_escape_dialect")
    dialect.dialect = dialect_class
    expression = get_dialect_like_pattern_expression(
        column=sa.Column("value", sa.String()),
        dialect=dialect,
        like_pattern="a!_b",
        positive=positive,
        escape="!",
    )

    assert expression is not None
    sql = str(
        expression.compile(
            dialect=dialect_class(paramstyle="named"), compile_kwargs={"literal_binds": True}
        )
    )
    if positive:
        expected_sql = "value LIKE 'a!_b' ESCAPE '!'"
    else:
        expected_sql = "value NOT LIKE 'a!_b' ESCAPE '!'"
    assert sql == expected_sql
