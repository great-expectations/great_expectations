from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Union

from great_expectations.compatibility.not_imported import NotImported
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa

if TYPE_CHECKING:
    from sqlalchemy.engine import URL

POSTGRESQL_NOT_IMPORTED = NotImported(
    "postgresql connection components are not installed, please 'pip install psycopg2'"
)


def _is_importable(module_name: str) -> bool:
    # Import rather than only look the module up: an installed driver can still fail to import,
    # as psycopg (3) does without its binary extra on a host with no libpq.
    try:
        importlib.import_module(module_name)
    except ImportError:
        return False
    return True


def resolve_postgresql_driver(url: Union[str, URL]) -> Union[str, URL]:
    """Return the URL to hand `create_engine`, falling back to psycopg2 when a driverless
    PostgreSQL URL's default driver is not installed.

    SQLAlchemy 2.1 changed the driver a `postgresql://` URL with no driver selects from psycopg2
    to psycopg (3). The `postgresql` extra installs psycopg2, so without this such a URL would fail
    to import its driver on 2.1 even though a working one is installed. The URL is returned
    unchanged when it names a driver, is not a PostgreSQL URL, or its default driver is
    importable, so SQLAlchemy's own choice always wins when it can run.
    """
    if not sa:
        return url
    try:
        parsed = sa.engine.make_url(url)
    except sa.exc.ArgumentError:
        return url  # not a URL; let create_engine report it
    if parsed.drivername != "postgresql":
        return url
    # Loading the dialect class does not import its DBAPI.
    default_driver = parsed.get_dialect().driver
    if default_driver == "psycopg2" or _is_importable(default_driver):
        return url
    if not _is_importable("psycopg2"):
        return url
    return parsed.set(drivername="postgresql+psycopg2")


try:
    import psycopg2  # noqa: F401 # FIXME CoP
    import sqlalchemy.dialects.postgresql as postgresqltypes
except ImportError:
    postgresqltypes = POSTGRESQL_NOT_IMPORTED  # type: ignore[assignment] # FIXME CoP

try:
    from sqlalchemy.dialects.postgresql import TEXT
except (ImportError, AttributeError):
    TEXT = POSTGRESQL_NOT_IMPORTED  # type: ignore[misc, assignment] # FIXME CoP

try:
    from sqlalchemy.dialects.postgresql import CHAR
except (ImportError, AttributeError):
    CHAR = POSTGRESQL_NOT_IMPORTED  # type: ignore[misc, assignment] # FIXME CoP

try:
    from sqlalchemy.dialects.postgresql import INTEGER
except (ImportError, AttributeError):
    INTEGER = POSTGRESQL_NOT_IMPORTED  # type: ignore[misc, assignment] # FIXME CoP

try:
    from sqlalchemy.dialects.postgresql import SMALLINT
except (ImportError, AttributeError):
    SMALLINT = POSTGRESQL_NOT_IMPORTED  # type: ignore[misc, assignment] # FIXME CoP

try:
    from sqlalchemy.dialects.postgresql import BIGINT
except (ImportError, AttributeError):
    BIGINT = POSTGRESQL_NOT_IMPORTED  # type: ignore[misc, assignment] # FIXME CoP

try:
    from sqlalchemy.dialects.postgresql import TIMESTAMP
except (ImportError, AttributeError):
    TIMESTAMP = POSTGRESQL_NOT_IMPORTED  # type: ignore[misc, assignment] # FIXME CoP

try:
    from sqlalchemy.dialects.postgresql import DATE
except (ImportError, AttributeError):
    DATE = POSTGRESQL_NOT_IMPORTED  # type: ignore[misc, assignment] # FIXME CoP

try:
    from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
except (ImportError, AttributeError):
    DOUBLE_PRECISION = POSTGRESQL_NOT_IMPORTED  # type: ignore[misc, assignment] # FIXME CoP

try:
    from sqlalchemy.dialects.postgresql import BOOLEAN
except (ImportError, AttributeError):
    BOOLEAN = POSTGRESQL_NOT_IMPORTED  # type: ignore[misc, assignment] # FIXME CoP

try:
    from sqlalchemy.dialects.postgresql import NUMERIC
except (ImportError, AttributeError):
    NUMERIC = POSTGRESQL_NOT_IMPORTED  # type: ignore[misc, assignment] # FIXME CoP


class POSTGRESQL_TYPES:
    """Namespace for PostgreSQL dialect types."""

    TEXT = TEXT
    CHAR = CHAR
    INTEGER = INTEGER
    SMALLINT = SMALLINT
    BIGINT = BIGINT
    TIMESTAMP = TIMESTAMP
    DATE = DATE
    DOUBLE_PRECISION = DOUBLE_PRECISION
    BOOLEAN = BOOLEAN
    NUMERIC = NUMERIC
