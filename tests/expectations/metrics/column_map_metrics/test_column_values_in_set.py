from types import ModuleType
from typing import List, Optional

import pytest

from great_expectations.expectations.metrics import ColumnValuesInSet

try:
    import sqlalchemy
    from sqlalchemy.dialects import mysql, postgresql, sqlite
except ImportError:
    sqlalchemy = None

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
    column = sqlalchemy.column(column_name)
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
    column = sqlalchemy.column(column_name)
    kwargs = _make_sqlalchemy_kwargs(column_name, dialect)
    predicate = ColumnValuesInSet._sqlalchemy_impl(column, value_set, **kwargs)
    expected_predicates = ", ".join(
        ["null" if value is None else str(value) for value in value_set]
    ).lower()
    # We expect the predicate to look similar to "column_name in (true, false, null)"
    assert (
        str(predicate.compile(compile_kwargs={"literal_binds": True})).lower()
        == f"{column_name} in ({expected_predicates})"
    )


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
