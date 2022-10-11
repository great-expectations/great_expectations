import importlib
import pkgutil
import pathlib
from typing import List, Dict
from types import ModuleType
from great_expectations.zep.core import PandasDatasource


def add_pandas(name: str) -> PandasDatasource:
    # add to self
    return PandasDatasource()


def _public_globals() -> List[str]:
    return [x for x in globals().keys() if not x.startswith("_")]


print(f"Before loading plugins: {_public_globals()}\n")

# POC: register additional methods from plugins etc.
# With this method we don't need a complicated import hook we just look for plugins and load the wanted methods into this module's namespace
def _load_plugins() -> Dict[str, ModuleType]:
    parent_dir = pathlib.Path(__file__).parent

    imported_plugins = {
        name: importlib.import_module(name)
        for name in [
            name
            for _, name, _ in pkgutil.iter_modules([str(parent_dir)])
            if name.startswith("gx2_")
        ]
    }

    # https://stackoverflow.com/questions/43059267/how-to-do-from-module-import-using-importlib
    return imported_plugins


print(_load_plugins())


print(f"After loading plugins: {_public_globals()}\n")
