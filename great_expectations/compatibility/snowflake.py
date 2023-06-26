from __future__ import annotations

from great_expectations.compatibility.not_imported import NotImported

# GX optional imports
SNOWFLAKE_NOT_IMPORTED = NotImported(
    "snowflake is not installed, please 'pip install snowflake-sqlalchemy'"
)

try:
    import snowflake

except ImportError:
    snowflake = SNOWFLAKE_NOT_IMPORTED

try:
    from snowflake.sqlalchemy import URL
except ImportError:
    URL = SNOWFLAKE_NOT_IMPORTED
