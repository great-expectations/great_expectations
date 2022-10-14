"""
POC for dynamically bootstrapping context.sources/Datasource with all it's registered methods.
"""
from __future__ import annotations

from typing import Callable, List

from great_expectations.zep.core import Datasource, PandasDatasource


def _camel_to_snake(s: str) -> str:
    # https://stackoverflow.com/a/44969381/6304433
    return "".join(["_" + c.lower() if c.isupper() else c for c in s]).lstrip("_")


SourceFactoryFn = Callable[..., Datasource]


def _add_pandas(name: str) -> PandasDatasource:
    return PandasDatasource()


class _SourceFactories:

    __sources = {"add_pandas": _add_pandas}

    @classmethod
    def add_factory(cls, name: str, fn: SourceFactoryFn) -> None:
        """Add/Register a datasource factory function."""
        print(f"2. Adding {name} factory")
        prexisting = cls.__sources.get(name, None)
        if not prexisting:
            cls.__sources[name] = fn
        else:
            raise ValueError(f"{name} already exists")

    @property
    def factories(self) -> List[str]:
        return list(self.__sources.keys())

    def __getattr__(self, name):
        return self.__sources[name]

    def __dir__(self) -> List[str]:
        # TODO: update to work for standard methods too
        return [k for k in self.__sources.keys()]


class MetaDatasouce(type):
    def __new__(meta_cls, cls_name, bases, cls_dict) -> MetaDatasouce:
        print(f"1. Hello {meta_cls.__name__} __new__() -> {cls_name}")

        def _datasource_factory(name: str) -> Datasource:
            print(f"5. Adding `{name}` {cls_name}")
            return type(
                cls_name, bases, cls_dict
            )  # should this return `super().__new__()` instead?

        sources = _SourceFactories()
        factory_name = f"add_{_camel_to_snake(cls_name)}".removesuffix("_datasource")
        sources.add_factory(factory_name, _datasource_factory)

        return super().__new__(meta_cls, cls_name, bases, cls_dict)


class PostgresDatasource(metaclass=MetaDatasouce):
    pass


class DataContext:
    _context = None

    @classmethod
    def get_context(cls) -> "DataContext":
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
    context.sources.add_pandas("taxi")
    context.sources.add_postgres("taxi_pg")
