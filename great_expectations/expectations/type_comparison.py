"""Type comparison utilities for column type validation expectations.

This module consolidates all type-comparison logic used by
ExpectColumnValuesToBeOfType and ExpectColumnValuesToBeInTypeList,
including dialect-aware dispatching for SQLAlchemy backends.
"""

from __future__ import annotations

import inspect
import logging
from types import ModuleType
from typing import TYPE_CHECKING, Any, NamedTuple, Sequence

from great_expectations.compatibility import aws, trino
from great_expectations.compatibility.bigquery import (
    BIGQUERY_GEO_SUPPORT,
    bigquery_types_tuple,
)
from great_expectations.compatibility.bigquery import (
    sqlalchemy_bigquery as BigQueryDialect,
)
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect
from great_expectations.util import (
    get_clickhouse_sqlalchemy_potential_type,
    get_pyathena_potential_type,
)

if TYPE_CHECKING:
    from great_expectations.execution_engine import SqlAlchemyExecutionEngine

logger = logging.getLogger(__name__)

try:
    import teradatasqlalchemy.dialect
    import teradatasqlalchemy.types as teradatatypes
except ImportError:
    teradatasqlalchemy = None
    teradatatypes = None

try:
    import clickhouse_sqlalchemy
    import clickhouse_sqlalchemy.types as ch_types
except (ImportError, KeyError):
    clickhouse_sqlalchemy = None
    ch_types = None


# Uses GXSqlDialect enum members (not raw strings) to match the rest of the
# codebase.  GXSqlDialect.__eq__ handles cross-type comparison with str values
# returned by SqlAlchemyExecutionEngine.dialect_name.
CASE_INSENSITIVE_DIALECTS: frozenset[GXSqlDialect] = frozenset(
    {
        GXSqlDialect.DATABRICKS,
        GXSqlDialect.POSTGRESQL,
        GXSqlDialect.SNOWFLAKE,
        GXSqlDialect.SQL_SERVER,
        GXSqlDialect.TRINO,
    }
)


def native_type_type_map(type_: str) -> tuple[type, ...] | None:  # noqa: C901, PLR0911
    """Map a string type name to a tuple of native Python types.

    Used by pandas validation paths to resolve type names like "int", "str", etc.
    Returns None for unrecognized types.
    """
    if type_.lower() == "none":
        return (type(None),)
    elif type_.lower() == "bool":
        return (bool,)
    elif type_.lower() in ["int", "long"]:
        return (int,)
    elif type_.lower() == "float":
        return (float,)
    elif type_.lower() == "bytes":
        return (bytes,)
    elif type_.lower() == "complex":
        return (complex,)
    elif type_.lower() in ["str", "string_types"]:
        return (str,)
    elif type_.lower() == "list":
        return (list,)
    elif type_.lower() == "dict":
        return (dict,)
    elif type_.lower() == "unicode":
        return None
    return None


def compare_column_type(
    execution_engine: SqlAlchemyExecutionEngine,
    actual_column_type: Any,
    expected_type: str,
    *,
    strict: bool = False,
) -> tuple[bool, Any]:
    """Compare an actual column type against an expected type string.

    Dispatches based on dialect:
    - For CASE_INSENSITIVE_DIALECTS: case-insensitive string comparison.
    - For all others: resolves expected_type to a SQLAlchemy type class and uses isinstance().

    With ``strict``, a name the dialect can conclusively not resolve raises
    InvalidExpectationConfigurationError. Without it, the name is a plain mismatch, which
    callers that fall back to comparing the observed type name rely on.

    Returns:
        (success, observed_value) where observed_value is actual_column_type as-is
        for case-insensitive dialects (typically a CaseInsensitiveString) or
        type(actual_column_type).__name__ for the isinstance path.
    """
    actual_column_type = _unwrap_clickhouse_nullable_type(
        execution_engine=execution_engine, actual_column_type=actual_column_type
    )

    if execution_engine.dialect_name in CASE_INSENSITIVE_DIALECTS:
        success = _compare_type_string(actual_column_type, expected_type)
        return success, actual_column_type
    else:
        resolution = _resolve_type_name(
            execution_engine=execution_engine, expected_type=expected_type
        )
        if strict and not resolution.types and resolution.conclusive:
            raise _unresolvable_type_error(execution_engine, [expected_type])
        success = isinstance(actual_column_type, tuple(resolution.types))
        return success, type(actual_column_type).__name__


def compare_column_type_list(
    execution_engine: SqlAlchemyExecutionEngine,
    actual_column_type: Any,
    expected_types_list: Sequence[str],
) -> tuple[bool, str]:
    """Compare an actual column type against a list of expected type strings.

    Dispatches based on dialect:
    - For CASE_INSENSITIVE_DIALECTS: case-insensitive string comparison against each type.
    - For all others: resolves each type to a SQLAlchemy type class and uses isinstance().

    Returns:
        (success, observed_value) where observed_value is the type string representation.
    """
    actual_column_type = _unwrap_clickhouse_nullable_type(
        execution_engine=execution_engine, actual_column_type=actual_column_type
    )

    if execution_engine.dialect_name in CASE_INSENSITIVE_DIALECTS:
        if isinstance(actual_column_type, str):
            success = any(
                actual_column_type.casefold() == expected_type.casefold()
                for expected_type in expected_types_list
            )
            return success, actual_column_type
        else:
            ret_type = type(actual_column_type).__name__
            success = any(
                ret_type.casefold() == expected_type.casefold()
                for expected_type in expected_types_list
            )
            return success, ret_type
    else:
        types: list = []
        conclusive = True
        for type_ in expected_types_list:
            resolution = _resolve_type_name(execution_engine=execution_engine, expected_type=type_)
            types.extend(resolution.types)
            conclusive = conclusive and resolution.conclusive
        # A type list is routinely written to span backends, so one name this dialect
        # cannot resolve is normal. Only a list that resolves to nothing at all, and
        # whose every name the dialect could speak to, is a configuration error. An
        # empty list names nothing to report, and keeps reporting a failed match.
        if expected_types_list and not types and conclusive:
            raise _unresolvable_type_error(execution_engine, expected_types_list)
        success = isinstance(actual_column_type, tuple(types))
        return success, type(actual_column_type).__name__


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _unwrap_clickhouse_nullable_type(
    execution_engine: SqlAlchemyExecutionEngine, actual_column_type: Any
) -> Any:
    """Return the nested type represented by ClickHouse ``Nullable(T)``."""
    if (
        execution_engine.dialect_name == GXSqlDialect.CLICKHOUSE
        and ch_types is not None
        and isinstance(actual_column_type, ch_types.Nullable)
    ):
        return actual_column_type.nested_type
    return actual_column_type


def _unresolvable_type_error(
    execution_engine: SqlAlchemyExecutionEngine, names: Sequence[str]
) -> InvalidExpectationConfigurationError:
    quoted = ", ".join(repr(name) for name in names)
    noun = "type" if len(names) == 1 else "types"
    return InvalidExpectationConfigurationError(
        f"Unrecognized {noun}: {quoted} is not a type name the "
        f"{execution_engine.dialect_name} dialect or sqlalchemy defines. Check the "
        f"spelling, or use a type name the dialect supports."
    )


def _compare_type_string(actual_column_type: Any, expected_type: str) -> bool:
    """Case-insensitive single-type comparison for CASE_INSENSITIVE_DIALECTS."""
    if isinstance(actual_column_type, str):
        # Preserve custom __eq__ behavior for str subclasses such as
        # CaseInsensitiveString, but normalize plain str values explicitly.
        if type(actual_column_type) is str:
            return actual_column_type.casefold() == expected_type.casefold()
        return actual_column_type == expected_type
    return str(actual_column_type).casefold() == expected_type.casefold()


class _Resolution(NamedTuple):
    """The outcome of resolving one type name against a dialect.

    ``conclusive`` is False where an empty ``types`` is not evidence of a typo: the name
    is a real sqlalchemy type the dialect does not compare against, or support for it
    is missing.
    """

    types: list
    conclusive: bool = True


_INCONCLUSIVE = _Resolution(types=[], conclusive=False)


def _resolve_type_name(
    execution_engine: SqlAlchemyExecutionEngine, expected_type: str
) -> _Resolution:
    """Resolve a type name to candidate SQLAlchemy type classes."""
    if _is_bigquery_geography_without_support(execution_engine, expected_type):
        logger.warning(
            "BigQuery GEOGRAPHY type is not supported by default. "
            + "To install support, please run:"
            + "  $ pip install 'sqlalchemy-bigquery[geography]'"
        )
        # The warning names the real cause, so the empty result that follows must not
        # be reported back to the user as a misspelled type name.
        return _INCONCLUSIVE

    type_module = _get_dialect_type_module(execution_engine=execution_engine)
    if execution_engine.dialect_module is None:
        # Athena, Hive and Vertica have no dialect module, so the sqlalchemy namespace
        # _get_dialect_type_module has just fallen back to is their whole vocabulary,
        # aliases such as INT included.
        types = _sqlalchemy_types(expected_type)
    else:
        types = _dialect_candidates(type_module, expected_type)
        # A dialect module also exports submodules and functions (sqlite and mysql export a
        # `json` module); only classes can be compared with isinstance.
        types = [potential_type for potential_type in types if inspect.isclass(potential_type)]
        if not types:
            # A dialect module re-exports only the subset of generic types it chooses to.
            # Oracle exports NUMBER and VARCHAR2 but not INTEGER, yet reflects an INTEGER
            # column as a generic sqlalchemy.INTEGER.
            types = _generic_candidates(expected_type)
        if not types and _sqlalchemy_types(expected_type):
            # A real sqlalchemy type this dialect does not compare against, such as the
            # Integer base class or the INT alias, is not a misspelling.
            return _INCONCLUSIVE
    if not types:
        logger.debug(f"Unrecognized type: {expected_type}")
    return _Resolution(types=_include_floating_point_in_numeric(types))


def _is_bigquery_geography_without_support(
    execution_engine: SqlAlchemyExecutionEngine, expected_type: str
) -> bool:
    """Whether GEOGRAPHY was asked for on BigQuery without the optional extra."""
    return (
        expected_type.lower() == "geography"
        and execution_engine.engine.dialect.name.lower() == GXSqlDialect.BIGQUERY
        and not BIGQUERY_GEO_SUPPORT
    )


def _dialect_candidates(type_module: ModuleType, expected_type: str) -> list:
    """Resolve a type name against the dialect's own type module."""
    try:
        if type_module.__name__ == "pyathena.sqlalchemy_athena":
            athena_type = get_pyathena_potential_type(type_module, expected_type)
            # In the case of the PyAthena dialect we need to verify that
            # the type returned is indeed a type and not an instance.
            if not inspect.isclass(athena_type):
                real_type = type(athena_type)
            else:
                real_type = athena_type
            return [real_type]
        if type_module.__name__ == "clickhouse_sqlalchemy.drivers.base":
            potential_type = get_clickhouse_sqlalchemy_potential_type(type_module, expected_type)
            if potential_type is sa.types.NullType:
                # What ClickHouseDialect._get_column_type returns for a spec it does
                # not recognize, alongside its own "Did not recognize type" warning.
                # Keeping it would leave the comparison an isinstance against
                # NullType, which is the silent failure this module is trying to lose.
                return []
            return [potential_type]
        if type_module.__name__ == "sqlalchemy_redshift.dialect":
            return _get_redshift_sqlalchemy_types(type_module, expected_type)
        potential_type = getattr(type_module, expected_type)
    except AttributeError:
        return []
    return [potential_type]


def _sqlalchemy_types(expected_type: str) -> list:
    """Resolve a type name against the top-level sqlalchemy namespace, aliases included.

    That namespace is far wider than its types, so a name colliding with one of its
    functions (``text``, ``cast``, ``select``) or non-type classes (``Column``) must not
    become a candidate.
    """
    generic_type = getattr(sa, expected_type, None)
    if isinstance(generic_type, type) and issubclass(generic_type, sa.types.TypeEngine):
        return [generic_type]
    return []


def _generic_candidates(expected_type: str) -> list:
    """Resolve a name a dialect module lacks to an uppercase SQL type under its own name.

    A CamelCase base type such as ``Integer`` would match every subtype (``BIGINT``,
    ``SMALLINT``) through isinstance, and an alias such as ``INT`` is ``INTEGER`` in this
    namespace although a dialect that does not export ``INT`` does not use that name.
    """
    return [
        generic_type
        for generic_type in _sqlalchemy_types(expected_type)
        if generic_type.__name__ == expected_type and expected_type.isupper()
    ]


def _include_floating_point_in_numeric(types: list) -> list:
    """Let an expected "Numeric" match floating-point columns on every SQLAlchemy version.

    SQLAlchemy 2.1 moved Float, and with it every dialect's floating-point type, out from
    under Numeric. Before 2.1 a floating-point column is an instance of Numeric, so an
    expectation naming "Numeric" matched it; adding Float keeps that result on 2.1 and
    changes nothing earlier, where Float is already a Numeric.
    """
    if sa.Numeric in types and sa.Float not in types:
        return [*types, sa.Float]
    return types


def _get_redshift_sqlalchemy_types(type_module: ModuleType, expected_type: Any) -> list:
    types: list = []
    potential_type = getattr(type_module, expected_type)
    types.append(potential_type)
    if expected_type.lower() == "decimal":
        # There is no redshift numeric type NUMERIC. It is suppose to be a synonym for
        # the official type DECIMAL, according to the docs:
        # https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
        # However we have observed the raw sqltypes.[NUMERIC|Numeric] instead so we
        # add this as an allowed matching type.
        types.append(sa.sql.sqltypes.NUMERIC)
    return types


def _get_dialect_type_module(  # noqa: C901, PLR0911
    execution_engine: SqlAlchemyExecutionEngine,
) -> ModuleType:
    if execution_engine.dialect_module is None:
        logger.warning("No sqlalchemy dialect found; relying on top-level sqlalchemy types.")
        return sa

    # Redshift does not (yet) export types to top level; only recognize base SA types
    if aws.redshiftdialect and isinstance(
        execution_engine.dialect_module,
        aws.redshiftdialect.RedshiftDialect,
    ):
        return execution_engine.dialect_module.sa
    else:
        pass

    # Bigquery works with newer versions, but use a patch if we had to define bigquery_types_tuple
    try:
        if BigQueryDialect and (
            isinstance(
                execution_engine.dialect_module,
                BigQueryDialect,
            )
            and bigquery_types_tuple is not None
        ):
            return bigquery_types_tuple
    except (TypeError, AttributeError):
        pass

    # Teradata types module
    try:
        if (
            teradatasqlalchemy is not None
            and issubclass(
                execution_engine.dialect_module,  # type: ignore[arg-type] # dialect_module can be a class
                teradatasqlalchemy.dialect.TeradataDialect,
            )
            and teradatatypes is not None
        ):
            return teradatatypes
    except (TypeError, AttributeError):
        pass

    try:
        if (
            clickhouse_sqlalchemy is not None
            and issubclass(
                execution_engine.dialect_module,  # type: ignore[arg-type] # dialect_module can be a class
                clickhouse_sqlalchemy.drivers.base.ClickHouseDialect,
            )
            and ch_types is not None
        ):
            return ch_types
    except (TypeError, AttributeError):
        pass

    # Trino types module
    try:
        if (
            trino.trinodialect
            and trino.trinotypes
            and isinstance(
                execution_engine.dialect,
                trino.trinodialect.TrinoDialect,
            )
        ):
            return trino.trinotypes
    except (TypeError, AttributeError):
        pass

    return execution_engine.dialect_module
