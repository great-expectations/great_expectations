from __future__ import annotations

from typing import Any

from great_expectations.compatibility.not_imported import NotImported

SQLALCHEMY_BIGQUERY_NOT_IMPORTED = NotImported(
    "sqlalchemy-bigquery is not installed, please 'pip install sqlalchemy-bigquery'"
)
_BIGQUERY_MODULE_NAME = "sqlalchemy_bigquery"
BIGQUERY_GEO_SUPPORT = False

bigquery_types_tuple = None


try:
    import sqlalchemy_bigquery
except (ImportError, AttributeError):
    sqlalchemy_bigquery = SQLALCHEMY_BIGQUERY_NOT_IMPORTED

try:
    from sqlalchemy_bigquery import INTEGER
except (ImportError, AttributeError):
    INTEGER = SQLALCHEMY_BIGQUERY_NOT_IMPORTED

try:
    from sqlalchemy_bigquery import NUMERIC
except (ImportError, AttributeError):
    NUMERIC = SQLALCHEMY_BIGQUERY_NOT_IMPORTED

try:
    from sqlalchemy_bigquery import STRING
except (ImportError, AttributeError):
    STRING = SQLALCHEMY_BIGQUERY_NOT_IMPORTED

try:
    from sqlalchemy_bigquery import BIGNUMERIC
except (ImportError, AttributeError):
    BIGNUMERIC = SQLALCHEMY_BIGQUERY_NOT_IMPORTED

try:
    from sqlalchemy_bigquery import BYTES
except (ImportError, AttributeError):
    BYTES = SQLALCHEMY_BIGQUERY_NOT_IMPORTED


try:
    from sqlalchemy_bigquery import BOOL
except (ImportError, AttributeError):
    BOOL = SQLALCHEMY_BIGQUERY_NOT_IMPORTED


try:
    from sqlalchemy_bigquery import BOOLEAN
except (ImportError, AttributeError):
    BOOLEAN = SQLALCHEMY_BIGQUERY_NOT_IMPORTED


try:
    from sqlalchemy_bigquery import TIMESTAMP
except (ImportError, AttributeError):
    TIMESTAMP = SQLALCHEMY_BIGQUERY_NOT_IMPORTED


try:
    from sqlalchemy_bigquery import TIME
except (ImportError, AttributeError):
    TIME = SQLALCHEMY_BIGQUERY_NOT_IMPORTED

try:
    from sqlalchemy_bigquery import FLOAT
except (ImportError, AttributeError):
    FLOAT = SQLALCHEMY_BIGQUERY_NOT_IMPORTED


try:
    from sqlalchemy_bigquery import DATE
except (ImportError, AttributeError):
    DATE = SQLALCHEMY_BIGQUERY_NOT_IMPORTED


try:
    from sqlalchemy_bigquery import DATETIME
except (ImportError, AttributeError):
    DATETIME = SQLALCHEMY_BIGQUERY_NOT_IMPORTED


class BIGQUERY_TYPES:
    """Namespace for Bigquery dialect types"""

    INTEGER = INTEGER
    NUMERIC = NUMERIC
    STRING = STRING
    BIGNUMERIC = BIGNUMERIC
    BYTES = BYTES
    BOOL = BOOL
    BOOLEAN = BOOLEAN
    TIMESTAMP = TIMESTAMP
    TIME = TIME
    FLOAT = FLOAT
    DATE = DATE
    DATETIME = DATETIME


try:
    from sqlalchemy_bigquery import GEOGRAPHY

    BIGQUERY_GEO_SUPPORT = True
except (ImportError, AttributeError):
    GEOGRAPHY = SQLALCHEMY_BIGQUERY_NOT_IMPORTED

try:
    from sqlalchemy_bigquery import parse_url
except (ImportError, AttributeError):
    parse_url = SQLALCHEMY_BIGQUERY_NOT_IMPORTED


def _render_double_as_float64() -> None:
    """Render SQLAlchemy's generic `Double` type as BigQuery's `FLOAT64`.

    BigQuery has no `DOUBLE` type; `FLOAT64` is its only binary floating-point type.
    sqlalchemy-bigquery renders `Float` as `FLOAT64` but has no rule for `Double`, which
    therefore falls through to the generic `DOUBLE`: invalid in a CAST or a column
    definition, and rejected as the declared type of a query parameter. SQLAlchemy 2.1
    types every Python `float` bind parameter as `Double` (2.0 used `Float`), so without
    this rule any query comparing against a float literal fails on BigQuery with
    "The given parameter type, DOUBLE, ... is not a valid BigQuery scalar type".

    The rule applies only when compiling for the BigQuery dialect.
    """
    try:
        from sqlalchemy.ext.compiler import compiles
        from sqlalchemy.types import Double
    except ImportError:
        # SQLAlchemy 1.4 has no generic `Double`, and binds Python floats as `Float`.
        return

    @compiles(Double, "bigquery")
    def _compile_double(type_: Double, compiler: Any, **kw: Any) -> str:
        return compiler.visit_FLOAT(type_, **kw)


if sqlalchemy_bigquery:
    _render_double_as_float64()
