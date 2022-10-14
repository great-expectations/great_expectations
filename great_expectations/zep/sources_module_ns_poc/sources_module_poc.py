import importlib
import pathlib
import pkgutil
from types import ModuleType
from typing import Any, Dict, List

from great_expectations.zep.core import PandasDatasource


def add_pandas(name: str) -> PandasDatasource:
    return PandasDatasource()


def _public_globals() -> List[str]:
    return [x for x in globals().keys() if not x.startswith("_")]


print(f"Before loading plugins: {_public_globals()}\n")


def _get_module_names(mdl: ModuleType) -> Dict[str, Any]:
    # is there an __all__?  if so respect it``
    if "__all__" in mdl.__dict__:
        names = mdl.__dict__["__all__"]
    else:
        # otherwise we import all names that don't begin with _
        names = [x for x in mdl.__dict__ if not x.startswith("_")]
    return {k: getattr(mdl, k) for k in names}


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

    # https://stackoverflow.com/a/43059528/6304433
    # update the current module's global namespace
    for mdl in imported_plugins.values():
        # TODO: maybe don't overwrite existing names
        globals().update(_get_module_names(mdl))
    return imported_plugins


_load_plugins()


print(f"After loading plugins: {_public_globals()}\n")

print(dir())


def __dir__() -> List[str]:
    return [k for k in globals().keys()]


print(dir())
