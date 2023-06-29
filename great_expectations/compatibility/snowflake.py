from __future__ import annotations

from great_expectations.compatibility.not_imported import NotImported

SNOWFLAKE_NOT_IMPORTED = NotImported(
    "snowflake connection components are not installed, please 'pip install snowflake-sqlalchemy snowflake-connector-python'"
)

try:
    import snowflake
except ImportError:
    snowflake = SNOWFLAKE_NOT_IMPORTED

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
