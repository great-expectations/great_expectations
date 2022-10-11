"""
POC for importing and bootstrapping context.sources/Datasource with all it's registered methods via plugins.
"""
import importlib
import inspect
import pathlib
import pkgutil
from pprint import pformat as pf
from types import ModuleType
from typing import List, Type

from great_expectations.zep.core import PandasDatasource


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


def find_plugins() -> List[str]:
    # NOTE: actual implementation would use `pkgutil.iter_modules` on sys.path to look for any installed package
    # that matches our plugin convention.
    parent_dir = pathlib.Path(__file__).parent

    found_plugins = []
    for _, name, is_package in pkgutil.iter_modules([str(parent_dir)]):
        # print(f"  {name} - {is_package}")
        if name.startswith("gx_"):
            found_plugins.append(name)
    return found_plugins


def set_methods_from_module(class_: Type, module: ModuleType) -> List:
    module_functions = inspect.getmembers(module, inspect.isfunction)

    added_methods = []
    for name, method in module_functions:
        if name.startswith("_"):
            print(f"skipping private method {name}")
            continue
        setattr(class_, name, method)
        added_methods.append(name)
    return added_methods


def bootstrap_sources():
    print(f"Before: {dir_public(Datasources)}")

    plugin_pkgs = find_plugins()
    imported_plugins = {name: importlib.import_module(name) for name in plugin_pkgs}
    print(f"\nplugins: \n{imported_plugins}\n")

    for module in imported_plugins.values():
        set_methods_from_module(Datasources, module)

    print(f"After: {dir_public(Datasources)}\n")


def get_context() -> DataContext:
    context = DataContext.get_context()
    return context


bootstrap_sources()


if __name__ == "__main__":
    context = get_context()
    context.sources.add_pandas("taxi")
    context.sources.add_sql("sql_taxi")
