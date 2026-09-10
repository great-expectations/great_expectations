"""The execution-engine vocabulary, standing alone with no package-level dependencies.

This module sits directly under ``tests/integration/test_utils/`` rather than inside
``data_source_config/`` because ``tests/integration/test_utils/__init__.py`` is empty, while
``data_source_config/__init__.py`` eagerly imports every backend module and then the registry-
derived ``tiers`` module. Importing anything through ``data_source_config`` -- even a leaf value
with no dependencies of its own -- runs that whole chain first. A module that must stay importable
with no data-source driver installed at all (e.g. the gold-tier case table) needs a home outside
that chain. ``ExecutionEngineKind`` has no dependencies of its own, so it lives here instead.

``data_source_config/__init__.py`` re-exports it, so the package's public surface is unchanged.
``data_source_spec`` itself keeps only a ``TYPE_CHECKING`` reference: it carries a guarantee that
it imports nothing beyond the standard library -- not even this repository's own ``tests``
package -- and a runtime import here would break it. Runtime import sites therefore name this
module directly rather than reaching the enum through ``data_source_spec``.
"""

from __future__ import annotations

from enum import Enum


class ExecutionEngineKind(Enum):
    """The engine that executes a data source's tests."""

    PANDAS = "pandas"
    SPARK = "spark"
    SQL = "sql"
