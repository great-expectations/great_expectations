"""
POC for dynamically bootstrapping context.sources/Datasource with all it's registered methods.
"""
from __future__ import annotations

import pathlib
from pprint import pformat as pf
from typing import Callable, Dict, List, NamedTuple, Optional, Union

from great_expectations.zep.interfaces import Datasource


class Models(NamedTuple):
    config: type
    runtime: type


class DatasourceDetails(NamedTuple):
    assets: List[Models]
    model: Models


SourceFactoryFn = Callable[..., Datasource]


def _camel_to_snake(s: str) -> str:
    # https://stackoverflow.com/a/44969381/6304433
    return "".join(["_" + c.lower() if c.isupper() else c for c in s]).lstrip("_")


class _SourceFactories:

    __source_factories: Dict[str, Callable] = {}
    __type_lookup: Dict[str, type] = {}
    __types: Dict[str, DatasourceDetails] = {}  # TODO: more work here

    @classmethod
    def register_factory(
        cls,
        type_name: str,
        fn: SourceFactoryFn,
        type_: type,
        asset_types: Optional[List[type]] = None,
    ) -> None:
        """
        Add/Register a datasource factory function.
        Attaches a method called `add_<TYPE_NAME>()`
        """
        method_name = f"add_{type_name}"
        print(f"2a. Register `{type_name}` `{method_name}()` factory")
        prexisting = cls.__type_lookup.get(type_name, None)
        if not prexisting:
            cls.__source_factories[method_name] = fn  # type: ignore[assignment]

            print(f"2b. Register type `{type_name}` models ...")
            # add Datasource type
            cls.__type_lookup[type_name] = type_

            asset_types = asset_types or []
            for t in asset_types:
                t_name = f"{_camel_to_snake(t.__name__).removesuffix('_asset')}"
                print(f"2c. Register type `{t_name}`")
                # TODO: need to check for name collisions here. And entire process should be atomic
                cls.__type_lookup[t_name] = t

            cls.__types[type_name] = DatasourceDetails(
                # TODO: separate runtime and config models??
                assets=[Models(runtime=t, config=t) for t in asset_types],
                model=Models(
                    runtime=type_,
                    config=type_,  # TODO: use generated model
                ),
            )
        else:
            raise ValueError(f"{type_name} factory already exists")

    @property
    def factories(self) -> List[str]:
        return list(self.__source_factories.keys())

    def __getattr__(self, name):
        try:
            return self.__source_factories[name]
        except KeyError:
            raise AttributeError(name)

    def types(self) -> Dict[str, type]:
        return self.__type_lookup

    # def __getitem__(self, key: str) -> _DatasourceDetails:
    #     return self.__types[key.lower()]

    # def __dir__(self) -> List[str]:
    #     # TODO: update to work for standard methods too
    #     # TODO: doesn't seem to work for jupyter-autocompletions
    #     return [k for k in self.__sources.keys()]


class MetaDatasouce(type):
    def __new__(meta_cls, cls_name, bases, cls_dict) -> MetaDatasouce:
        print(f"1. Hello {meta_cls.__name__} __new__() -> {cls_name}")

        cls = type(cls_name, bases, cls_dict)

        def _datasource_factory(*args, **kwargs) -> Datasource:
            print(f"5. Adding `{args[0] if args else ''}` {cls_name}")
            return cls(*args, **kwargs)

        sources = _SourceFactories()
        type_name = f"{_camel_to_snake(cls_name)}".removesuffix("_datasource")

        # TODO: generate schemas from `cls`

        # TODO: extract asset type details
        print(
            f"\n{cls_name}\n\n`cls_dict` details ->\n{pf(cls_dict)}\n\n`dir(cls)` details ->\n{pf(dir(cls))}\n"
        )
        asset_type_hint = cls_dict["__annotations__"]["asset_types"]
        print(f"{type(asset_type_hint)} {asset_type_hint=}")
        asset_types = cls_dict.get("asset_types", [])
        for t in asset_types:
            print(f"{type(t)} {t=}")

        sources.register_factory(
            type_name, _datasource_factory, cls, asset_types=asset_types
        )

        return super().__new__(meta_cls, cls_name, bases, cls_dict)


class MyAsset:
    pass


class FileAsset:
    base_directory: pathlib.Path
    sep: str = ","


class TableAsset:
    """Postgres TableAsset"""

    pass


class PandasDatasource(metaclass=MetaDatasouce):

    asset_types: Union[FileAsset, MyAsset]

    def __init__(self, name: str):
        self.name = name

    def __repr__(self):
        return f"{self.__class__.__name__}(name='{self.name}')"

    def add_csv(self, foo="foo", bar="bar", sep=","):
        """I'm a docstring!!"""
        # NOTE: should this return the datasource or the csv asset?
        return self


class PostgresDatasource(metaclass=MetaDatasouce):
    asset_types: List[type] = [TableAsset, MyAsset]

    def __init__(self, name: str, connection_str: str):
        self.name = name
        self.connection_str = connection_str


class DataContext:
    _context = None

    @classmethod
    def get_context(cls) -> DataContext:
        if not cls._context:
            cls._context = DataContext()

        return cls._context

    def __init__(self) -> None:
        self._sources: _SourceFactories = _SourceFactories()
        print(f"4. Available Factories - {self._sources.factories}")

    @property
    def sources(self) -> _SourceFactories:
        return self._sources


def get_context() -> DataContext:
    print("3. Getting context")
    context = DataContext.get_context()
    return context


if __name__ == "__main__":
    context = get_context()
    print(f"Registered types ->\n{pf(context.sources.types())}")
    context.sources.add_pandas("taxi")
    # context.sources.add_postgres()
