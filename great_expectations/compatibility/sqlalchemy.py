from __future__ import annotations

import warnings

from packaging.version import Version

from great_expectations.compatibility.not_imported import NotImported


def sqlalchemy_version_check(version: str | Version) -> None:
    """Check if the sqlalchemy version is supported or warn if not.

    Args:
        version: sqlalchemy version as a string or Version.
    """
    if isinstance(version, str):
        version = Version(version)

    if version >= Version("2.0.0"):
        warnings.warn(
            "SQLAlchemy v2.0.0 or later is not yet supported by Great Expectations.",
            UserWarning,
        )


# GX optional imports
SQLALCHEMY_NOT_IMPORTED = NotImported(
    "sqlalchemy is not installed, please 'pip install sqlalchemy'"
)

try:
    import sqlalchemy  # noqa: TID251

    sqlalchemy_version_check(sqlalchemy.__version__)
except (ImportError, AttributeError):
    sqlalchemy = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy import engine  # noqa: TID251
except (ImportError, AttributeError):
    engine = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy import dialects  # noqa: TID251
except (ImportError, AttributeError):
    dialects = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.dialects import registry  # noqa: TID251
except (ImportError, AttributeError):
    registry = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine import Dialect  # noqa: TID251
except (ImportError, AttributeError):
    Dialect = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine import Inspector  # noqa: TID251
except (ImportError, AttributeError):
    Inspector = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine import reflection  # noqa: TID251
except (ImportError, AttributeError):
    reflection = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine import Connection  # noqa: TID251
except (ImportError, AttributeError):
    Connection = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine import Engine  # noqa: TID251
except (ImportError, AttributeError):
    Engine = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine import Row  # noqa: TID251
except (ImportError, AttributeError):
    Row = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine.row import RowProxy  # noqa: TID251
except (ImportError, AttributeError):
    RowProxy = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine.row import LegacyRow  # noqa: TID251
except (ImportError, AttributeError):
    LegacyRow = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine.default import DefaultDialect  # noqa: TID251
except (ImportError, AttributeError):
    DefaultDialect = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine import url  # noqa: TID251
    from sqlalchemy.engine.url import URL  # noqa: TID251
except (ImportError, AttributeError):
    url = SQLALCHEMY_NOT_IMPORTED
    URL = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.exc import DatabaseError  # noqa: TID251
except (ImportError, AttributeError):
    DatabaseError = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.exc import IntegrityError  # noqa: TID251
except (ImportError, AttributeError):
    IntegrityError = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.exc import NoSuchTableError  # noqa: TID251
except (ImportError, AttributeError):
    NoSuchTableError = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.exc import OperationalError  # noqa: TID251
except (ImportError, AttributeError):
    OperationalError = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.exc import ProgrammingError  # noqa: TID251
except (ImportError, AttributeError):
    ProgrammingError = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.exc import SQLAlchemyError  # noqa: TID251
except (ImportError, AttributeError):
    SQLAlchemyError = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.orm import declarative_base  # noqa: TID251
except (ImportError, AttributeError):
    declarative_base = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql import functions  # noqa: TID251
except (ImportError, AttributeError):
    functions = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql import Insert  # noqa: TID251
except (ImportError, AttributeError):
    Insert = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.elements import literal  # noqa: TID251
except (ImportError, AttributeError):
    literal = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.elements import TextClause  # noqa: TID251
except (ImportError, AttributeError):
    TextClause = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.elements import quoted_name  # noqa: TID251
except (ImportError, AttributeError):
    quoted_name = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.elements import ColumnElement  # noqa: TID251
except (ImportError, AttributeError):
    ColumnElement = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import Cast  # noqa: TID251
except (ImportError, AttributeError):
    Cast = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import ColumnOperators  # noqa: TID251
except (ImportError, AttributeError):
    ColumnOperators = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import CTE  # noqa: TID251
except (ImportError, AttributeError):
    CTE = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import BinaryExpression  # noqa: TID251
except (ImportError, AttributeError):
    BinaryExpression = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import BooleanClauseList  # noqa: TID251
except (ImportError, AttributeError):
    BooleanClauseList = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import ColumnClause  # noqa: TID251
except (ImportError, AttributeError):
    ColumnClause = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import Label  # noqa: TID251
except (ImportError, AttributeError):
    Label = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import Select  # noqa: TID251
except (ImportError, AttributeError):
    Select = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import Selectable  # noqa: TID251
except (ImportError, AttributeError):
    Selectable = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import TableClause  # noqa: TID251
except (ImportError, AttributeError):
    TableClause = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import TextualSelect  # noqa: TID251
except (ImportError, AttributeError):
    TextualSelect = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import WithinGroup  # noqa: TID251
except (ImportError, AttributeError):
    WithinGroup = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.operators import custom_op  # noqa: TID251
except (ImportError, AttributeError):
    custom_op = SQLALCHEMY_NOT_IMPORTED
