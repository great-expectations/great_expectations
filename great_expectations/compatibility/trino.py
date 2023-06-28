from __future__ import annotations

from great_expectations.compatibility.not_imported import NotImported

TRINO_NOT_IMPORTED = NotImported(
    "trino connection components are not installed, please 'pip install trino'"
)

try:
    import trino  # noqa TID251
except ImportError:
    trino = TRINO_NOT_IMPORTED

try:
    from trino.sqlalchemy import datatype as trinotypes  # noqa TID251
except (ImportError, AttributeError):
    trinotypes = TRINO_NOT_IMPORTED

try:
    from trino.sqlalchemy import dialect as trinodialect  # noqa TID251
except (ImportError, AttributeError):
    trinodialect = TRINO_NOT_IMPORTED

try:
    import trino.drivers as trinodrivers  # noqa TID251
except (ImportError, AttributeError):
    trinodrivers = TRINO_NOT_IMPORTED

try:
    import trino.exceptions as trinoexceptions  # noqa TID251
except (ImportError, AttributeError):
    trinoexceptions = TRINO_NOT_IMPORTED
