from __future__ import annotations

from typing import Final

from great_expectations.compatibility.not_imported import NotImported

SNOWFLAKE_NOT_IMPORTED = NotImported(
    "snowflake connection components are not installed, please 'pip install snowflake-sqlalchemy snowflake-connector-python'"  # noqa: E501 # FIXME CoP
)

try:
    import snowflake
except ImportError:
    snowflake = SNOWFLAKE_NOT_IMPORTED

try:
    from snowflake.sqlalchemy import URL
except (ImportError, AttributeError):
    URL = SNOWFLAKE_NOT_IMPORTED

try:
    import snowflake.sqlalchemy as snowflakesqlalchemy
except (ImportError, AttributeError):
    snowflakesqlalchemy = SNOWFLAKE_NOT_IMPORTED

try:
    import snowflake.sqlalchemy.snowdialect as snowflakedialect
except (ImportError, AttributeError):
    snowflakedialect = SNOWFLAKE_NOT_IMPORTED

try:
    import snowflake.sqlalchemy.custom_types as snowflaketypes
except (ImportError, AttributeError):
    snowflaketypes = SNOWFLAKE_NOT_IMPORTED

IS_SNOWFLAKE_INSTALLED: Final[bool] = snowflake is not SNOWFLAKE_NOT_IMPORTED

try:
    from snowflake.sqlalchemy.custom_types import ARRAY
except (ImportError, AttributeError):
    ARRAY = SNOWFLAKE_NOT_IMPORTED

try:
    from snowflake.sqlalchemy.custom_types import BYTEINT
except (ImportError, AttributeError):
    BYTEINT = SNOWFLAKE_NOT_IMPORTED


try:
    from snowflake.sqlalchemy.custom_types import CHARACTER
except (ImportError, AttributeError):
    CHARACTER = SNOWFLAKE_NOT_IMPORTED


try:
    from snowflake.sqlalchemy.custom_types import DEC
except (ImportError, AttributeError):
    DEC = SNOWFLAKE_NOT_IMPORTED


try:
    from snowflake.sqlalchemy.custom_types import FIXED
except (ImportError, AttributeError):
    FIXED = SNOWFLAKE_NOT_IMPORTED

try:
    from snowflake.sqlalchemy.custom_types import GEOGRAPHY
except (ImportError, AttributeError):
    GEOGRAPHY = SNOWFLAKE_NOT_IMPORTED


try:
    from snowflake.sqlalchemy.custom_types import GEOMETRY
except (ImportError, AttributeError):
    GEOMETRY = SNOWFLAKE_NOT_IMPORTED


try:
    from snowflake.sqlalchemy.custom_types import NUMBER
except (ImportError, AttributeError):
    NUMBER = SNOWFLAKE_NOT_IMPORTED


try:
    from snowflake.sqlalchemy.custom_types import OBJECT
except (ImportError, AttributeError):
    OBJECT = SNOWFLAKE_NOT_IMPORTED


try:
    from snowflake.sqlalchemy.custom_types import STRING
except (ImportError, AttributeError):
    STRING = SNOWFLAKE_NOT_IMPORTED


try:
    from snowflake.sqlalchemy.custom_types import TEXT
except (ImportError, AttributeError):
    TEXT = SNOWFLAKE_NOT_IMPORTED


try:
    from snowflake.sqlalchemy.custom_types import TIMESTAMP_LTZ
except (ImportError, AttributeError):
    TIMESTAMP_LTZ = SNOWFLAKE_NOT_IMPORTED


try:
    from snowflake.sqlalchemy.custom_types import TIMESTAMP_NTZ
except (ImportError, AttributeError):
    TIMESTAMP_NTZ = SNOWFLAKE_NOT_IMPORTED


try:
    from snowflake.sqlalchemy.custom_types import TIMESTAMP_TZ
except (ImportError, AttributeError):
    TIMESTAMP_TZ = SNOWFLAKE_NOT_IMPORTED


try:
    from snowflake.sqlalchemy.custom_types import TINYINT
except (ImportError, AttributeError):
    TINYINT = SNOWFLAKE_NOT_IMPORTED


try:
    from snowflake.sqlalchemy.custom_types import VARBINARY
except (ImportError, AttributeError):
    VARBINARY = SNOWFLAKE_NOT_IMPORTED


try:
    from snowflake.sqlalchemy.custom_types import VARIANT
except (ImportError, AttributeError):
    VARIANT = SNOWFLAKE_NOT_IMPORTED

try:
    from snowflake.sqlalchemy.custom_types import DOUBLE
except (ImportError, AttributeError):
    DOUBLE = SNOWFLAKE_NOT_IMPORTED

try:
    from snowflake.sqlalchemy.custom_types import SnowflakeType
except (ImportError, AttributeError):
    SnowflakeType = SNOWFLAKE_NOT_IMPORTED


class SNOWFLAKE_TYPES:
    """Namespace for Snowflake dialect types."""

    ARRAY = ARRAY
    BYTEINT = BYTEINT
    CHARACTER = CHARACTER
    DEC = DEC
    DOUBLE = DOUBLE
    FIXED = FIXED
    GEOGRAPHY = GEOGRAPHY
    GEOMETRY = GEOMETRY
    NUMBER = NUMBER
    OBJECT = OBJECT
    STRING = STRING
    TEXT = TEXT
    TIMESTAMP_LTZ = TIMESTAMP_LTZ
    TIMESTAMP_NTZ = TIMESTAMP_NTZ
    TIMESTAMP_TZ = TIMESTAMP_TZ
    TINYINT = TINYINT
    VARBINARY = VARBINARY
    VARIANT = VARIANT
