"""
POC for dynamically bootstrapping context.sources with Datasource factory methods.
"""
from __future__ import annotations

import logging
from pprint import pformat as pf
from typing import TYPE_CHECKING, List, Type

from great_expectations.zep.sources import _SourceFactories

if TYPE_CHECKING:
    from great_expectations.zep.interfaces import DataAsset, Datasource


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

        asset_types: List[Type[DataAsset]] = cls_dict.get("asset_types")
        LOGGER.info(f"1b. Extracting Asset details - {asset_types}")

        cls = super().__new__(meta_cls, cls_name, bases, cls_dict)
        LOGGER.debug(f"  {cls_name} __dict__ ->\n{pf(cls.__dict__, depth=3)}")

        if cls_name == "Datasource":
            # NOTE: the above check is brittle and must be kept in-line with the Datasource.__name__
            LOGGER.info("1c. Skip factory registration of base `Datasource`")
            return cls

        def _datasource_factory(*args, **kwargs) -> Datasource:
            # TODO: update signature to match Datasource __init__ (ex update __signature__)
            LOGGER.info(f"5. Adding `{args[0] if args else ''}` {cls_name}")
            return cls(*args, **kwargs)

        # TODO: generate schemas from `cls` if needed

        _SourceFactories.register_factory(
            cls, _datasource_factory, asset_types=asset_types
        )

        return cls
