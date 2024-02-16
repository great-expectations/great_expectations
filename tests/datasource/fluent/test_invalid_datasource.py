from __future__ import annotations

import inspect
from pprint import pformat as pf
from typing import Any, Callable, Final, Literal

import pytest

from great_expectations.compatibility import pydantic
from great_expectations.datasource.fluent import Datasource, InvalidDatasource
from great_expectations.datasource.fluent.sources import _SourceFactories


@pytest.fixture
def invalid_datasource_factory() -> (
    Callable[[dict[Literal["name", "type", "assets"] | Any, Any]], InvalidDatasource]
):
    def _invalid_ds_fct(config: dict) -> InvalidDatasource:
        ds_type: Datasource = _SourceFactories.type_lookup[config["type"]]
        try:
            return ds_type(**config)
        except pydantic.ValidationError as config_error:
            return InvalidDatasource(**config, config_error=config_error)
        raise ValueError("The Datasource was valid")

    return _invalid_ds_fct


_EXCLUDE_METHODS: Final[set[str]] = {
    # we don't care about overriding these methods for InvalidDatasource
    "copy",
    "delete_asset",
    "delete_batch_config",
    "dict",
    "get_asset_names",
    "get_execution_engine",
    "get_assets_as_dict",
    "parse_order_by_sorters",
    "json",
    "yaml",
}
DATASOURCE_PUBLIC_METHODS: Final[list[str]] = [
    f[0]
    for f in inspect.getmembers(Datasource, predicate=inspect.isfunction)
    if not f[0].startswith("_") and f[0] not in _EXCLUDE_METHODS
]


@pytest.mark.unit
@pytest.mark.parametrize("base_ds_method_name", DATASOURCE_PUBLIC_METHODS)
def test_public_methods_are_overridden(base_ds_method_name: str):
    """
    Ensure that applicable Datasource public methods are overridden.
    Applicable public methods are those that would typically be called when a users is trying to run some action on a Datasource
    and would want to know if the Datasource is invalid.

    If a method is not overridden, it will be inherited from the base class and will not be present in the InvalidDatasource.__dict__.
    """
    print(f"InvalidDatasource.__dict__ attributes\n{pf(InvalidDatasource.__dict__)}")
    invalid_ds_public_attributes = [
        m for m in InvalidDatasource.__dict__.keys() if not m.startswith("_")
    ]
    assert base_ds_method_name in invalid_ds_public_attributes


if __name__ == "__main__":
    pytest.main(["-vv", __file__])
