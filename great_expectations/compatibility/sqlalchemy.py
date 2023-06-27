from __future__ import annotations

from great_expectations.compatibility.not_imported import NotImported

# GX optional imports
SQLALCHEMY_NOT_IMPORTED = NotImported(
    "sqlalchemy is not installed, please 'pip install sqlalchemy'"
)

try:
    import sqlalchemy
except ImportError:
    sqlalchemy = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy import engine
except ImportError:
    engine = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy import dialects
except ImportError:
    dialects = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy import inspect
except ImportError:
    inspect = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.dialects import sqlite
except (ImportError, AttributeError):
    sqlite = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.dialects import registry
except (ImportError, AttributeError):
    registry = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine import Dialect
except (ImportError, AttributeError):
    Dialect = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine import Inspector
except (ImportError, AttributeError):
    Inspector = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine import reflection
except (ImportError, AttributeError):
    reflection = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine import Connection
except (ImportError, AttributeError):
    Connection = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine import Engine
except (ImportError, AttributeError):
    Engine = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine import Row
except (ImportError, AttributeError):
    Row = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine.row import RowProxy
except (ImportError, AttributeError):
    RowProxy = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine.row import LegacyRow
except (ImportError, AttributeError):
    LegacyRow = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine.default import DefaultDialect
except (ImportError, AttributeError):
    DefaultDialect = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine import url
    from sqlalchemy.engine.url import URL
except (ImportError, AttributeError):
    url = SQLALCHEMY_NOT_IMPORTED
    URL = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.exc import DatabaseError
except (ImportError, AttributeError):
    DatabaseError = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.exc import IntegrityError
except (ImportError, AttributeError):
    IntegrityError = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.exc import NoSuchTableError
except (ImportError, AttributeError):
    NoSuchTableError = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.exc import OperationalError
except (ImportError, AttributeError):
    OperationalError = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.exc import ProgrammingError
except (ImportError, AttributeError):
    ProgrammingError = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.exc import SQLAlchemyError
except (ImportError, AttributeError):
    SQLAlchemyError = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.orm import declarative_base
except (ImportError, AttributeError):
    declarative_base = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql import functions
except (ImportError, AttributeError):
    functions = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql import Insert
except (ImportError, AttributeError):
    Insert = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.elements import literal
except (ImportError, AttributeError):
    literal = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.elements import TextClause
except (ImportError, AttributeError):
    TextClause = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.elements import quoted_name
except (ImportError, AttributeError):
    quoted_name = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.elements import _anonymous_label
except (ImportError, AttributeError):
    _anonymous_label = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.elements import ColumnElement
except (ImportError, AttributeError):
    ColumnElement = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import Cast
except (ImportError, AttributeError):
    Cast = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import ColumnOperators
except (ImportError, AttributeError):
    ColumnOperators = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import CTE
except (ImportError, AttributeError):
    CTE = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import BinaryExpression
except (ImportError, AttributeError):
    BinaryExpression = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import BooleanClauseList
except (ImportError, AttributeError):
    BooleanClauseList = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import ColumnClause
except (ImportError, AttributeError):
    ColumnClause = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import Label
except (ImportError, AttributeError):
    Label = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import Select
except (ImportError, AttributeError):
    Select = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql import Selectable
except (ImportError, AttributeError):
    Selectable = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import TableClause
except (ImportError, AttributeError):
    TableClause = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import TextualSelect
except (ImportError, AttributeError):
    TextualSelect = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.expression import WithinGroup
except (ImportError, AttributeError):
    WithinGroup = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.sql.operators import custom_op
except (ImportError, AttributeError):
    custom_op = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine.cursor import LegacyCursorResult
except (ImportError, AttributeError):
    LegacyCursorResult = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.engine.cursor import CursorResult
except (ImportError, AttributeError):
    CursorResult = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy.pool import StaticPool
except (ImportError, AttributeError):
    StaticPool = SQLALCHEMY_NOT_IMPORTED

try:
    from sqlalchemy import Table
except (ImportError, AttributeError):
    Table = SQLALCHEMY_NOT_IMPORTED
