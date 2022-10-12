"""
POC for dynamically bootstrapping context.sources/Datasource with all it's registered methods.
"""
from typing import Callable, List

from great_expectations.zep.core import PandasDatasource, SQLDatasource


def dir_public(o) -> List[str]:
    return [x for x in dir(o) if not x.startswith("_")]


class Datasources:
    def add_pandas(self, name: str) -> PandasDatasource:
        # add to self
        return PandasDatasource()


class DataContext:
    _context = None

    @classmethod
    def get_context(cls) -> "DataContext":
        if not cls._context:
            cls._context = DataContext()

        return cls._context

    def __init__(self) -> None:
        self._sources: Datasources = Datasources()

    @property
    def sources(self) -> Datasources:
        return self._sources


def bootstrap_sources():
    print(f"Before: {dir_public(Datasources)}")

    # these methods need to be collected in someway. Plugins, global, etc.
    # they will not be statically defined in this function.
    def add_sql(self, name: str) -> SQLDatasource:
        print(f"Added SQL - {name}")
        return SQLDatasource()

    registered_methods: List[Callable] = [add_sql]
    for method in registered_methods:
        setattr(Datasources, method.__name__, method)

    print(f"After: {dir_public(Datasources)}")


def get_context() -> DataContext:
    context = DataContext.get_context()
    return context


bootstrap_sources()


if __name__ == "__main__":
    context = get_context()
    context.sources.add_pandas("taxi")
    context.sources.add_sql("sql_taxi")
