from __future__ import annotations

from great_expectations.compatibility.not_imported import NotImported

PYARROW_NOT_IMPORTED = NotImported(
    "pyarrow is not installed, please 'pip install pyarrow'"
)

try:
    import pyarrow
except ImportError:
    pyarrow = PYARROW_NOT_IMPORTED
