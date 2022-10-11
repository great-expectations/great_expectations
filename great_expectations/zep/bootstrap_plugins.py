"""
POC for importing and bootstrapping context.sources/Datasource with all it's registered methods via plugins.
"""
import importlib
import pkgutil
from pprint import pformat as pf

from great_expectations.zep.core import PandasDatasource, SQLDatasource
from great_expectations.zep import gx_sql
from typing import Callable, List


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

    pkgs = [
        f"{name} - {ispkg}"
        for _, name, ispkg in pkgutil.iter_modules()
        # if name.startswith("gx_")
    ]
    print(f"packages: \n{pf(pkgs)}")

    # discovered_plugins = {
    #     name: importlib.import_module(name)
    #     for name in pkgs
    # }
    # print(f"plugins: \n{discovered_plugins}")

    print(f"After: {dir_public(Datasources)}")


def get_context() -> DataContext:
    context = DataContext.get_context()
    return context


bootstrap_sources()


if __name__ == "__main__":
    context = get_context()
    context.sources.add_pandas("taxi")
    context.sources.add_sql("sql_taxi")
