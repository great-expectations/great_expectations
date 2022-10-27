"""
POC for dynamically bootstrapping context.sources with Datasource factory methods.
"""
from __future__ import annotations

from typing import Callable, Dict, List

from great_expectations.util import camel_to_snake
from great_expectations.zep.interfaces import Datasource

SourceFactoryFn = Callable[..., Datasource]


def _remove_suffix(s: str, suffix: str) -> str:
    # NOTE: str.remove_suffix() added in python 3.9
    if s.endswith(suffix):
        s = s[: -len(suffix)]
    return s


class _SourceFactories:

    __source_factories: Dict[str, SourceFactoryFn] = {}

    @classmethod
    def register_factory(
        cls,
        ds_type: type,
        fn: SourceFactoryFn,
    ) -> None:
        """
        Add/Register a datasource factory function.

        Derives a SIMPLIFIED_NAME from the provided `Datasource` type.
        Attaches a method called `add_<SIMPLIFIED_NAME>()`.

        Example
        -------
        `class PandasDatasource` -> `add_pandas()`
        """
        simplified_name = _remove_suffix(
            camel_to_snake(ds_type.__name__), "_datasource"
        )
        method_name = f"add_{simplified_name}"
        print(
            f"2. Registering {ds_type.__name__} as {simplified_name} with {method_name}() factory"
        )

        pre_existing = cls.__source_factories.get(method_name)
        if not pre_existing:
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
        return ["register_factory", "factories", *self.factories]


class MetaDatasouce(type):
    def __new__(meta_cls, cls_name, bases, cls_dict) -> MetaDatasouce:
        print(f"1. {meta_cls.__name__}.__new__() for `{cls_name}`")

        cls = type(cls_name, bases, cls_dict)

        def _datasource_factory(*args, **kwargs) -> Datasource:
            # TODO: update signature to match Datasource __init__ (ex update __signature__)
            print(f"5. Adding `{args[0] if args else ''}` {cls_name}")
            return cls(*args, **kwargs)

        sources = _SourceFactories()
        # TODO: generate schemas from `cls` if needed

        # TODO: extract asset type details

        sources.register_factory(cls, _datasource_factory)

        return super().__new__(meta_cls, cls_name, bases, cls_dict)


# class FileAsset:
#     pass


# class PandasDatasource(metaclass=MetaDatasouce):

#     asset_types = [FileAsset]

#     def __init__(self, name: str):
#         self.name = name

#     def __repr__(self):
#         return f"{self.__class__.__name__}(name='{self.name}')"

#     def add_csv(self, foo="foo", bar="bar", sep=","):
#         """I'm a docstring!!"""
#         # NOTE: should this return the datasource or the csv asset?
#         return self


# class TableAsset:
#     pass


# class PostgresDatasource(metaclass=MetaDatasouce):
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
    # context.sources.add_pandas("taxi")
    # context.sources.add_postgres("taxi2", connection_str="postgres://...")
