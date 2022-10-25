"""
POC for dynamically bootstrapping context.sources with Datasource factory methods.
"""
from __future__ import annotations

import logging
from pprint import pformat as pf
from typing import Callable, Dict, List, Optional, Type, Union

from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.util import camel_to_snake
from great_expectations.zep.bi_directional_dict import BiDict
from great_expectations.zep.interfaces import DataAsset, Datasource

SourceFactoryFn = Callable[..., Datasource]

LOGGER = logging.getLogger(__name__)

if __name__ == "__main__":
    # don't setup the logger unless being run as a script
    # TODO: remove this before release
    logging.basicConfig(level=logging.INFO, format="%(message)s")


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

    type_lookup: BiDict[Union[str, type]] = BiDict()
    __source_factories: Dict[str, SourceFactoryFn] = {}

    @classmethod
    def register_factory(
        cls,
        ds_type: type,
        fn: SourceFactoryFn,
        asset_types: Optional[List[type]] = None,
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
        if not pre_existing:

            # TODO: simplify or extract the following datasource & asset type registration logic
            asset_types = asset_types or []
            asset_type_names = [
                _get_simplified_name_from_type(t, suffix_to_remove="_asset")
                for t in asset_types
            ]

            # NOTE: This check is a shortcut. What we need to protect against is different asset types
            # that share the same name. But we might want a Datasource to be able to use/register a previously
            # registered type ??
            already_registered_assets = set(asset_type_names).intersection(
                cls.type_lookup.keys()
            )
            if already_registered_assets:
                raise ValueError(
                    f"The following names already have a registered type - {already_registered_assets} "
                )

            for type_, name in zip(asset_types, asset_type_names):
                cls.type_lookup[type_] = name

            cls.type_lookup[ds_type] = simplified_name
            cls.__source_factories[method_name] = fn  # type: ignore[assignment]
        else:
            raise ValueError(f"{simplified_name} factory already exists")

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


class MetaDatasource(type):
    def __new__(
        meta_cls: Type[MetaDatasource], cls_name: str, bases: tuple[type], cls_dict
    ) -> MetaDatasource:
        """
        MetaDatasource hook that runs when a new `Datasource` is defined.
        This methods binds a factory method for the defined `Datasource` to `_SourceFactories` class which becomes
        available as part of the `DataContext`.

        Also binds asset adding methods according to the declared `asset_types`.
        """
        LOGGER.info(f"1a. {meta_cls.__name__}.__new__() for `{cls_name}`")

        # TODO: extract asset type details to build factory method signature etc. (pull args from __init__)

        asset_types: List[type] = cls_dict.get("asset_types")
        LOGGER.info(f"1b. Extracting Asset details - {asset_types}")
        if asset_types:
            meta_cls._inject_asset_methods(cls_dict, asset_types)

        # TODO: raise a TypeError here instead
        assert all(
            [isinstance(t, type) for t in asset_types]
        ), f"Datasource `asset_types` must be a iterable of classes/types got {asset_types}"

        def _datasource_factory(*args, **kwargs) -> Datasource:
            # TODO: update signature to match Datasource __init__ (ex update __signature__)
            LOGGER.info(f"5. Adding `{args[0] if args else ''}` {cls_name}")
            return cls(*args, **kwargs)

        cls = type(cls_name, bases, cls_dict)
        LOGGER.debug(f"  {cls_name} __dict__ ->\n{pf(cls.__dict__, depth=3)}")

        # TODO: TypeError & expose the missing details
        # assert isinstance(
        #     cls, Datasource
        # ), f"{cls.__name__} does not satisfy the {Datasource.__name__} protocol"

        sources = _SourceFactories()
        # TODO: generate schemas from `cls` if needed

        sources.register_factory(cls, _datasource_factory, asset_types=asset_types)

        return super().__new__(meta_cls, cls_name, bases, cls_dict)

    @classmethod
    def _inject_asset_methods(
        cls: Type[MetaDatasource],
        ds_cls_dict: Dict[str, Callable],
        asset_types: List[Type[DataAsset]],
    ) -> None:
        LOGGER.info(f"1c. Injecting `add_<ASSET_TYPE>` methods for {asset_types}")

        type_method_pairs: Dict[Type[DataAsset], str] = {
            t: f"add_{_get_simplified_name_from_type(t)}" for t in asset_types
        }

        # TODO: only inject 1 method per call to deal with issue of closures in a loop
        for asset_type, method_name in type_method_pairs.items():

            method_already_defined = ds_cls_dict.get(method_name)
            if method_already_defined:
                LOGGER.info(
                    f"  {asset_type.__name__} method `{method_name}()` already defined"
                )
                continue

            attr_annotations = asset_type.__dict__["__annotations__"]

            # TODO: update signature with `attr_annotations`
            def _add_asset(self: Datasource, name: str, *args, **kwargs):
                LOGGER.info(f"6. Creating `{asset_type.__name__}` '{name}' ...")
                data_asset = asset_type(name, *args, **kwargs)
                self.assets[name] = data_asset
                return data_asset

            ds_cls_dict[method_name] = _add_asset
            LOGGER.info(f"  {method_name}() - {attr_annotations} injected")


# class FileAsset:
#     file_path: str
#     delimiter: str
#     ...


# class MyOtherAsset:
#     foo: str
#     bar: List[int]


# class PandasDatasource(metaclass=MetaDatasource):

#     name: str
#     asset_types = [FileAsset, MyOtherAsset]

#     def __init__(self, name: str):
#         self.name = name

#     def __repr__(self):
#         return f"{self.__class__.__name__}(name='{self.name}')"


# class TableAsset:
#     pass


# class PostgresDatasource(metaclass=MetaDatasource):
#     asset_types = [TableAsset]

#     def __init__(self, name: str, connection_str: str):
#         self.name = name
#         self.connection_str = connection_str


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
    LOGGER.info("3. Getting context")
    context = DataContext.get_context()
    return context


if __name__ == "__main__":
    context = get_context()
    ds = context.sources.add_pandas("taxi")
    ds.add_my_other_asset(foo="bar")
    # context.sources.add_postgres("taxi2", connection_str="postgres://...")

    # # Demo the use of the `type_lookup` `BiDict`
    # # Alternatively use a Graph/Tree-like structure
    # sources = context.sources
    # print("\n  Datasource & DataAsset lookups ...")

    # s = "pandas"
    # pd_ds: PandasDatasource = sources.type_lookup[s]
    # print(f"\n'{s}' -> {pd_ds}")

    # pd_ds_assets = pd_ds.asset_types
    # print(f"\n{pd_ds} -> {pd_ds_assets}")

    # pd_ds_asset_names = [sources.type_lookup[t] for t in pd_ds_assets]
    # print(f"\n{pd_ds_assets} -> {pd_ds_asset_names}")

    # pd_ds_assets_from_names = [sources.type_lookup[name] for name in pd_ds_asset_names]
    # print(f"\n{pd_ds_asset_names} -> {pd_ds_assets_from_names}")
