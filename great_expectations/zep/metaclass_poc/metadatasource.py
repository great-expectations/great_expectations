"""
POC for dynamically bootstrapping context.sources/Datasource with all it's registered methods.
"""
from __future__ import annotations

from typing import Callable, Dict, List

from great_expectations.zep.interfaces import Datasource

SourceFactoryFn = Callable[..., Datasource]


def _camel_to_snake(s: str) -> str:
    # https://stackoverflow.com/a/44969381/6304433
    return "".join(["_" + c.lower() if c.isupper() else c for c in s]).lstrip("_")


class _SourceFactories:

    __sources: Dict[str, Callable] = {}

    @classmethod
    def add_factory(cls, name: str, fn: SourceFactoryFn) -> None:
        """Add/Register a datasource factory function."""
        print(f"2. Adding {name} factory")
        prexisting = cls.__sources.get(name, None)
        if not prexisting:
            cls.__sources[name] = fn  # type: ignore[assignment]
        else:
            raise ValueError(f"{name} already exists")

    @property
    def factories(self) -> List[str]:
        return list(self.__sources.keys())

    def __getattr__(self, name):
        try:
            return self.__sources[name]
        except KeyError:
            raise AttributeError(name)

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
        factory_name = f"add_{_camel_to_snake(cls_name)}".removesuffix("_datasource")
        sources.add_factory(factory_name, _datasource_factory)

        return super().__new__(meta_cls, cls_name, bases, cls_dict)


class PandasDatasource(metaclass=MetaDatasouce):
    def __init__(self, name: str):
        self.name = name

    def __repr__(self):
        return f"{self.__class__.__name__}(name='{self.name}')"

    def add_csv(self, foo="foo", bar="bar", sep=","):
        """I'm a docstring!!"""
        return self


# class PostgresDatasource(metaclass=MetaDatasouce):
#     pass


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
    context.sources.add_pandas("taxi")
    # context.sources.add_postgres()
