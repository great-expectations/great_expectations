from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Type, Union

from typing_extensions import ClassVar

from great_expectations.util import camel_to_snake
from great_expectations.zep.type_lookup import TypeLookup

if TYPE_CHECKING:
    from great_expectations.data_context import DataContext as GXDataContext
    from great_expectations.execution_engine import ExecutionEngine
    from great_expectations.zep.context import DataContext
    from great_expectations.zep.interfaces import DataAsset, Datasource

SourceFactoryFn = Callable[..., "Datasource"]

LOGGER = logging.getLogger(__name__.lstrip("great_expectations."))


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

    # TODO (kilo59): split DataAsset & Datasource lookups
    type_lookup: ClassVar = TypeLookup()
    engine_lookup: ClassVar = TypeLookup()
    __source_factories: ClassVar[Dict[str, SourceFactoryFn]] = {}

    _data_context: Union[DataContext, GXDataContext]

    def __init__(self, data_context: Union[DataContext, GXDataContext]):
        self._data_context = data_context

    @classmethod
    def register_factory(
        cls,
        ds_type: Type[Datasource],
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

        # TODO: We should namespace the asset type to the datasource so different datasources can reuse asset types.
        cls._register_assets(ds_type)

        cls.type_lookup[ds_type] = simplified_name
        LOGGER.debug(f"'{simplified_name}' added to `type_lookup`")
        cls.__source_factories[method_name] = fn

        cls._register_engine(ds_type)

    @property
    def factories(self) -> List[str]:
        return list(self.__source_factories.keys())

    def __getattr__(self, name):
        try:
            fn = self.__source_factories[name]

            def wrapped(*args, **kwargs):
                datasource = fn(*args, **kwargs)
                # TODO (bdirks): _attach_datasource_to_context to the AbstractDataContext class
                self._data_context._attach_datasource_to_context(datasource)
                return datasource

            return wrapped
        except KeyError:
            raise AttributeError(f"No factory {name} in {self.factories}")

    def __dir__(self) -> List[str]:
        """Preserves autocompletion for dynamic attributes."""
        return [*self.factories, *super().__dir__()]

    @classmethod
    def _register_engine(cls, ds_type: Type[Datasource]):
        try:
            exec_engine_type: Type[ExecutionEngine] = ds_type.__fields__[
                "execution_engine"
            ].type_
        except (AttributeError, KeyError) as exc:
            LOGGER.warning(f"{exc.__class__.__name__}:{exc}")
            raise TypeError(
                f"No `execution_engine` found for {ds_type.__name__} unable to register `ExecutionEngine` type"
            ) from exc

        class_name: str = exec_engine_type.__name__
        engine_simple_name: str = _get_simplified_name_from_type(
            exec_engine_type, suffix_to_remove="_execution_engine"
        )

        LOGGER.info(
            f"2b. Registering engine type `{class_name}` as '{engine_simple_name}'"
        )
        cls.engine_lookup[engine_simple_name] = exec_engine_type
        LOGGER.info(list(cls.engine_lookup.keys()))

    @classmethod
    def _register_assets(cls, ds_type: Type[Datasource]):
        asset_types: List[Type[DataAsset]] = ds_type.asset_types
        asset_type_names: List[str] = [
            _get_simplified_name_from_type(t, suffix_to_remove="_asset")
            for t in asset_types
        ]

        cls.type_lookup.raise_if_contains([*asset_types, *asset_type_names])

        for type_, name in zip(asset_types, asset_type_names):
            cls.type_lookup[type_] = name
            LOGGER.debug(f"'{name}' added to `type_lookup`")
