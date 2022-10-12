"""
POC for dynamically bootstrapping context.sources/Datasource with all it's registered methods.
"""
from typing import List, Callable

from great_expectations.zep.core import PandasDatasource, Datasource


def dir_public(o) -> List[str]:
    return [x for x in dir(o) if not x.startswith("_")]


SourceFactoryFn = Callable[..., Datasource]


def _add_pandas(name: str) -> PandasDatasource:
    return PandasDatasource()


class _SourceFactories:

    __sources = {"add_pandas": _add_pandas}

    @classmethod
    def add_factory(cls, name: str, fn: SourceFactoryFn) -> None:
        """Add/Register a datasource factory function."""
        prexisting = cls.__sources.pop(name, None)
        if not prexisting:
            cls.__sources[name] = fn
        raise ValueError(f"{name} already exists")

    def __getattr__(self, name):
        return self.__sources[name]

    def __dir__(self) -> List[str]:
        # TODO: update to work for standard methods too
        return [k for k in self.__sources.keys()]


class DataContext:
    _context = None

    @classmethod
    def get_context(cls) -> "DataContext":
        if not cls._context:
            cls._context = DataContext()

        return cls._context

    def __init__(self) -> None:
        self._sources: _SourceFactories = _SourceFactories()

    @property
    def sources(self) -> _SourceFactories:
        return self._sources


def get_context() -> DataContext:
    context = DataContext.get_context()
    return context


if __name__ == "__main__":
    context = get_context()
    context.sources.add_pandas("taxi")
