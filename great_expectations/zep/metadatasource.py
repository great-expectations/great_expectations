"""
POC for dynamically bootstrapping context.sources with Datasource factory methods.
"""
from __future__ import annotations

import logging
from pprint import pformat as pf
from typing import TYPE_CHECKING, Callable, Dict, List, Type

from great_expectations.zep.context import _SourceFactories
from great_expectations.zep.util import _get_simplified_name_from_type

if TYPE_CHECKING:
    from great_expectations.zep.interfaces import DataAsset, Datasource

SourceFactoryFn = Callable[..., "Datasource"]

LOGGER = logging.getLogger(__name__)


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
            for t in asset_types:
                meta_cls._inject_asset_method(cls_dict, t)

            # TODO: raise a TypeError here instead
            assert all(
                [isinstance(t, type) for t in asset_types]
            ), f"Datasource `asset_types` must be a iterable of classes/types got {asset_types}"

        cls = type(cls_name, bases, cls_dict)
        LOGGER.debug(f"  {cls_name} __dict__ ->\n{pf(cls.__dict__, depth=3)}")

        def _datasource_factory(*args, **kwargs) -> Datasource:
            # TODO: update signature to match Datasource __init__ (ex update __signature__)
            LOGGER.info(f"5. Adding `{args[0] if args else ''}` {cls_name}")
            return cls(*args, **kwargs)

        # TODO: TypeError & expose the missing details
        # assert isinstance(
        #     cls, Datasource
        # ), f"{cls.__name__} does not satisfy the {Datasource.__name__} protocol"

        sources = _SourceFactories()
        # TODO: generate schemas from `cls` if needed

        sources.register_factory(cls, _datasource_factory, asset_types=asset_types)

        return super().__new__(meta_cls, cls_name, bases, cls_dict)

    @classmethod
    def _inject_asset_method(
        cls: Type[MetaDatasource],
        ds_cls_dict: Dict[str, Callable],
        asset_type: Type[DataAsset],
    ) -> None:
        LOGGER.info(f"1c. Injecting `add_<ASSET_TYPE>` method for {asset_type}")
        method_name = f"add_{_get_simplified_name_from_type(asset_type)}"

        method_already_defined = ds_cls_dict.get(method_name)
        if method_already_defined:
            LOGGER.info(
                f"  {asset_type.__name__} method `{method_name}()` already defined"
            )
        attr_annotations = asset_type.__dict__.get("__annotations__", "")

        # TODO: update signature with `attr_annotations`
        def _add_asset(self: Datasource, name: str, *args, **kwargs):
            LOGGER.info(f"6. Creating `{asset_type.__name__}` '{name}' ...")
            data_asset = asset_type(name, *args, **kwargs)
            self.assets[name] = data_asset
            return data_asset

        ds_cls_dict[method_name] = _add_asset
        LOGGER.info(f"  {method_name}({attr_annotations}) - injected")
