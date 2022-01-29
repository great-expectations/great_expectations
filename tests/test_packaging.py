import importlib
import pkgutil
from types import ModuleType
from typing import Callable, Dict

import pytest
import requirements as rp

from great_expectations.data_context.util import file_relative_path


def test_requirements_files():
    """requirements.txt should be a subset of requirements-dev.txt"""

    with open(file_relative_path(__file__, "../requirements.txt")) as req:
        requirements = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(file_relative_path(__file__, "../requirements-dev.txt")) as req:
        requirements_dev = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(file_relative_path(__file__, "../requirements-dev-base.txt")) as req:
        requirements_dev_base = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(file_relative_path(__file__, "../requirements-dev-spark.txt")) as req:
        requirements_dev_spark = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    with open(
        file_relative_path(__file__, "../requirements-dev-sqlalchemy.txt")
    ) as req:
        requirements_dev_sqlalchemy = {
            f'{line.name}{"".join(line.specs[0])}' for line in rp.parse(req)
        }

    assert requirements <= requirements_dev

    assert requirements_dev_base.intersection(requirements_dev_spark) == set()
    assert requirements_dev_base.intersection(requirements_dev_sqlalchemy) == set()

    assert requirements_dev_spark.intersection(requirements_dev_sqlalchemy) == set()

    assert requirements_dev - (
        requirements
        | requirements_dev_base
        | requirements_dev_sqlalchemy
        | requirements_dev_spark
    ) <= {"numpy>=1.21.0", "scipy>=1.7.0"}


def test_all_imports_at_runtime():
    """Look for ImportErrors/ModuleNotFoundErrors when importing all GE modules"""

    # These are scripts that are triggered if investigated by `importlib`.
    # As we don't want to actually run the underlying code in our source files, let's only import files that define code.
    IGNORE = {
        "great_expectations.cli.checkpoint_script_template",
        "great_expectations.cli.v012.checkpoint_script_template",
    }

    def import_submodules(package_name: str) -> Dict[str, ModuleType]:
        """Import all submodules of a module, recursively, including subpackages"""

        package = importlib.import_module(package_name)
        results = {}
        for _, name, is_pkg in pkgutil.walk_packages(package.__path__):
            full_name = f"{package.__name__}.{name}"
            if full_name in IGNORE:
                continue

            results[full_name] = importlib.import_module(full_name)
            if is_pkg:
                results.update(import_submodules(full_name))

        return results

    import_dict = import_submodules("great_expectations")
