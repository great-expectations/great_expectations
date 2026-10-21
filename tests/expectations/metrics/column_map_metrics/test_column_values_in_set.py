from types import ModuleType
from typing import List, Optional

import pytest

from great_expectations.expectations.metrics import ColumnValuesInSet

try:
    import sqlalchemy
    from sqlalchemy.dialects import mysql, postgresql, sqlite
except ImportError:
    sqlalchemy = None  # type: ignore[assignment]
    mysql = postgresql = sqlite = None  # type: ignore[assignment]

try:
    import sqlalchemy_bigquery
except ImportError:
    sqlalchemy_bigquery = None


@pytest.mark.unit
@pytest.mark.skipif(
    sqlalchemy is None or sqlalchemy_bigquery is None,
    reason="sqlalchemy or sqlalchemy_bigquery is not installed",
)
@pytest.mark.parametrize("value_set", [[False, True], [False, True, None], [True], [False], [None]])
def test_sqlalchemy_impl_bigquery_bool(value_set: List[Optional[bool]]):
    column_name = "my_bool_col"
    column: sqlalchemy.ColumnClause = sqlalchemy.column(column_name)
    kwargs = _make_sqlalchemy_kwargs(column_name, sqlalchemy_bigquery)
    predicate = ColumnValuesInSet._sqlalchemy_impl(column, value_set, **kwargs)
    # If a value in value_set is None we expect "column_name is null" otherwise we expect
    # "column_name = value"
    expected_predicates = [
        f"{column_name} {'is' if value is None else '='} {'null' if value is None else value}"
        for value in value_set
    ]
    assert str(predicate).lower() == " or ".join(expected_predicates).lower()


@pytest.mark.unit
@pytest.mark.skipif(sqlalchemy is None, reason="sqlalchemy is not installed")
@pytest.mark.parametrize("dialect", [mysql, postgresql, sqlite])
@pytest.mark.parametrize("value_set", [[False, True], [False, True, None], [True], [False], [None]])
def test_sqlalchemy_impl_not_bigquery_bool(dialect: ModuleType, value_set: List[Optional[bool]]):
    column_name = "my_bool_col"
    column: sqlalchemy.ColumnClause = sqlalchemy.column(column_name)
    kwargs = _make_sqlalchemy_kwargs(column_name, dialect)
    predicate = ColumnValuesInSet._sqlalchemy_impl(column, value_set, **kwargs)
    # A None in value_set must not render a NULL literal in the IN list: the
    # negation of `column IN (..., NULL)` is NULL (not TRUE), so every
    # non-matching row would count as expected. NULL rows never reach this
    # condition, so dropping None is semantically identical. GH #12273.
    non_null_values = [value for value in value_set if value is not None]
    if non_null_values:
        expected_predicates = ", ".join(str(value) for value in non_null_values).lower()
        expected = f"{column_name} in ({expected_predicates})"
        actual = str(predicate.compile(compile_kwargs={"literal_binds": True})).lower()
    else:
        # An all-None value_set must compile to the dialect's own empty-set
        # expression, which never matches. Compile both sides with the real
        # dialect so the check is dialect-specific and does not depend on a
        # hard-coded string tied to one SQLAlchemy version's rendering.
        dialect_instance = dialect.dialect()
        expected = str(
            column.in_([]).compile(dialect=dialect_instance, compile_kwargs={"literal_binds": True})
        ).lower()
        actual = str(
            predicate.compile(dialect=dialect_instance, compile_kwargs={"literal_binds": True})
        ).lower()
    assert actual == expected


def _make_sqlalchemy_kwargs(column_name, dialect):
    return {
        "_dialect": dialect,
        "_metrics": {
            "table.column_types": [
                {
                    "comment": None,
                    "default": None,
                    "max_length": None,
                    "name": column_name,
                    "nullable": True,
                    "precision": None,
                    "scale": None,
                    "type": sqlalchemy.Boolean(),
                },
            ],
            "table.columns": [column_name],
            "table.row_count": 10000,
        },
        "parse_strings_as_datetimes": False,
        # The following key/values to be passed into _sqlalchemy_impl, but they aren't
        # actually used
        "_table": sqlalchemy.Table("my_table", sqlalchemy.MetaData()),
        "_sqlalchemy_engine": "DummySqlalchemyEngine",
    }
