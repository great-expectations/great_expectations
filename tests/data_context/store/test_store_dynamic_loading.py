import importlib

import pytest

from great_expectations.util import verify_dynamic_loading_support


@pytest.mark.unit
def test_store_dynamic_loading_enablement() -> None:
    module = importlib.import_module("great_expectations.data_context.store")
    module_dict = vars(module)
    for module_name in module_dict:
        if module_name.endswith("_store") or module_name.endswith("_store_backend"):
            verify_dynamic_loading_support(
                module_name=f".{module_name}",
                package_name="great_expectations.data_context.store",
            )
