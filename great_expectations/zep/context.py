from __future__ import annotations

import logging
from pprint import pformat as pf
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Type

from great_expectations.util import camel_to_snake
from great_expectations.zep.type_lookup import TypeLookup

if TYPE_CHECKING:
    from great_expectations.zep.interfaces import DataAsset, Datasource

SourceFactoryFn = Callable[..., "Datasource"]

LOGGER = logging.getLogger(__name__)


def _remove_suffix(s: str, suffix: str) -> str:
    # NOTE: str.remove_suffix() added in python 3.9
    if s.endswith(suffix):
        s = s[: -len(suffix)]
    return s


def _get_simplified_name_from_type(
    t: type, suffix_to_remove: Optional[str] = None
) -> str:
    result = camel_to_snake(t.__name__)
    if suffix_to_remove:
        return _remove_suffix(result, suffix_to_remove)
    return result


class _SourceFactories:
    """
    Contains a collection of datasource factory methods in the format `.add_<TYPE_NAME>()`

    Contains a `.type_lookup` dict-like two way mapping between previously registered `Datasource`
    or `DataAsset` types and a simplified name for those types.
    """

    type_lookup = TypeLookup()
    __source_factories: Dict[str, SourceFactoryFn] = {}

    @classmethod
    def register_factory(
        cls,
        ds_type: type,
        fn: SourceFactoryFn,
        asset_types: List[Type[DataAsset]],
    ) -> None:
        """
        Add/Register a datasource factory function.

        Derives a SIMPLIFIED_NAME from the provided `Datasource` type.
        Attaches a method called `add_<SIMPLIFIED_NAME>()`.

        Also registers related `DataAsset` types.

        Example
        -------
        `class PandasDatasource` -> `add_pandas()`
        """
        simplified_name = _get_simplified_name_from_type(
            ds_type, suffix_to_remove="_datasource"
        )

        method_name = f"add_{simplified_name}"
        LOGGER.info(
            f"2a. Registering {ds_type.__name__} as {simplified_name} with {method_name}() factory"
        )

        pre_existing = cls.__source_factories.get(method_name)
        if pre_existing:
            raise ValueError(f"{simplified_name} factory already exists")

        # TODO: simplify or extract the following datasource & asset type registration logic
        asset_types = asset_types or []
        asset_type_names = [
            _get_simplified_name_from_type(t, suffix_to_remove="_asset")
            for t in asset_types
        ]

        # TODO: We should namespace the asset type to the datasource so different datasources can reuse asset types.
        already_registered_assets = set(asset_type_names).intersection(
            cls.type_lookup.keys()
        )
        if already_registered_assets:
            raise ValueError(
                f"The following names already have a registered type - {already_registered_assets} "
            )

        for type_, name in zip(asset_types, asset_type_names):
            cls.type_lookup[type_] = name
            LOGGER.debug(f"'{name}' added to `type_lookup`")

        cls.type_lookup[ds_type] = simplified_name
        LOGGER.debug(f"'{simplified_name}' added to `type_lookup`")
        cls.__source_factories[method_name] = fn  # type: ignore[assignment]

    @property
    def factories(self) -> List[str]:
        return list(self.__source_factories.keys())

    def __getattr__(self, name):
        try:
            return self.__source_factories[name]
        except KeyError:
            raise AttributeError(name)

    def __dir__(self) -> List[str]:
        """Preserves autocompletion for dynamic attributes."""
        return [*self.factories, *super().__dir__()]


class DataContext:
    """
    NOTE: this is just a scaffold for exploring and iterating on our ZEP prototype
    this will be formalized and tested prior to release.

    Use `great_expectations.get_context()` for a real DataContext.
    """

    _context = None

    @classmethod
    def get_context(cls) -> DataContext:
        if not cls._context:
            cls._context = DataContext()

        return cls._context

    def __init__(self) -> None:
        self._sources: _SourceFactories = _SourceFactories()
        LOGGER.info(f"4a. Available Factories - {self._sources.factories}")
        LOGGER.info(f"4b. `type_lookup` mapping ->\n{pf(self._sources.type_lookup)}")

    @property
    def sources(self) -> _SourceFactories:
        return self._sources


def get_context() -> DataContext:
    """ZEP get_context placeholder function."""
    LOGGER.info("3. Getting context")
    context = DataContext.get_context()
    return context
