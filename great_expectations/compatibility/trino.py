from __future__ import annotations

from great_expectations.compatibility.not_imported import NotImported

TRINO_NOT_IMPORTED = NotImported(
    "trino connection components are not installed, please 'pip install trino'"
)

try:
    import trino
except ImportError:
    trino = TRINO_NOT_IMPORTED

try:
    from trino.sqlalchemy import datatype as trinotypes
except (ImportError, AttributeError):
    trinotypes = TRINO_NOT_IMPORTED

try:
    from trino.sqlalchemy import dialect as trinodialect
except (ImportError, AttributeError):
    trinodialect = TRINO_NOT_IMPORTED

try:
    import trino.drivers as trinodrivers
except (ImportError, AttributeError):
    trinodrivers = TRINO_NOT_IMPORTED

try:
    import trino.exceptions as trinoexceptions
except (ImportError, AttributeError):
    trinoexceptions = TRINO_NOT_IMPORTED
