"""
POC for dynamically bootstrapping context.sources with Datasource factory methods.
"""
from __future__ import annotations

import logging
from pprint import pformat as pf
from typing import TYPE_CHECKING, Callable, List, Type

from great_expectations.zep.context import _SourceFactories

if TYPE_CHECKING:
    from great_expectations.zep.interfaces import DataAsset, Datasource

SourceFactoryFn = Callable[..., "Datasource"]

LOGGER = logging.getLogger(__name__)

_CALLS = 0
_MAX_CALLS = 5


def _recursion_check(log_message: str):
    global _CALLS
    _CALLS += 1
    print(f"\n{_CALLS} {log_message}")
    if _CALLS >= _MAX_CALLS:
        raise RecursionError(f"Calls exceeded {_MAX_CALLS}")


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
        _recursion_check(f"{cls_name}\n  bases={bases}\n  meta_cls={meta_cls.__name__}")

        LOGGER.info(f"1a. {meta_cls.__name__}.__new__() for `{cls_name}`")

        # TODO: extract asset type details to build factory method signature etc. (pull args from __init__)

        asset_types: List[Type[DataAsset]] = cls_dict.get("asset_types")
        LOGGER.info(f"1b. Extracting Asset details - {asset_types}")

        # TODO: fix max recursion error that happens here
        # Bad solution - remove the base, extract & merge the __dict__ methods from the base
        # cls = type(cls_name, (), cls_dict)
        cls = super().__new__(meta_cls, cls_name, bases, cls_dict)
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

        return cls
