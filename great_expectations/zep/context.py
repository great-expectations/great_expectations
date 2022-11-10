from __future__ import annotations

import logging
from pprint import pformat as pf
from typing import TYPE_CHECKING, ClassVar, Dict, Optional

from great_expectations.zep.sources import _SourceFactories

if TYPE_CHECKING:
    from great_expectations.zep.interfaces import Datasource

LOGGER = logging.getLogger(__name__)


class DataContext:
    """
    NOTE: this is just a scaffold for exploring and iterating on our ZEP prototype
    this will be formalized and tested prior to release.

    Use `great_expectations.get_context()` for a real DataContext.
    """

    _context: ClassVar[Optional[DataContext]] = None
    _datasources: Dict[str, Datasource]

    @classmethod
    def get_context(cls) -> DataContext:
        if not cls._context:
            cls._context = DataContext()

        return cls._context

    def __init__(self) -> None:
        self._sources: _SourceFactories = _SourceFactories(self)
        self._datasources: Dict[str, Datasource] = {}
        LOGGER.info(f"4a. Available Factories - {self._sources.factories}")
        LOGGER.info(f"4b. `type_lookup` mapping ->\n{pf(self._sources.type_lookup)}")

    @property
    def sources(self) -> _SourceFactories:
        return self._sources

    def _attach_datasource_to_context(self, datasource: Datasource) -> None:
        self._datasources[datasource.name] = datasource


def get_context() -> DataContext:
    """ZEP get_context placeholder function."""
    LOGGER.info("3. Getting context")
    context = DataContext.get_context()
    return context
