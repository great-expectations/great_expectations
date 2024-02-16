from __future__ import annotations

import inspect
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


_EXCLUDE_METHODS: Final[set[str]] = {"dict", "yaml"}
DATASOURCE_PUBLIC_METHODS: Final[list[str]] = [
    f[0]
    for f in inspect.getmembers(Datasource, predicate=inspect.isfunction)
    if not f[0].startswith("_") or f[0] in _EXCLUDE_METHODS
]


@pytest.mark.unit
@pytest.mark.parametrize("method_name", DATASOURCE_PUBLIC_METHODS)
def test_public_methods_are_overridden(method_name: str):
    assert method_name in InvalidDatasource.__dict__


if __name__ == "__main__":
    pytest.main(["-vv", __file__])
