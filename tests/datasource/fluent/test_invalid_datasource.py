from __future__ import annotations

import inspect
from pprint import pformat as pf
from typing import Any, Callable, Final, Literal, Protocol

import pytest

from great_expectations.compatibility import pydantic
from great_expectations.datasource.fluent import (
    Datasource,
    InvalidDatasource,
    TestConnectionError,
)
from great_expectations.datasource.fluent.sources import _SourceFactories

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


class InvalidDSFn(Protocol):
    """
    Accept a datasource config and return an InvalidDatasource instance.
    Raises an error if the config was valid.
    """

    def __call__(
        self, config: dict[Literal["name", "type", "assets"] | Any, Any]
    ) -> InvalidDatasource:
        ...


@pytest.fixture
def invalid_datasource_factory() -> InvalidDSFn:
    def _invalid_ds_fct(config: dict) -> InvalidDatasource:
        ds_type: type[Datasource] = _SourceFactories.type_lookup[config["type"]]
        try:
            ds_type(**config)
        except pydantic.ValidationError as config_error:
            return InvalidDatasource(**config, config_error=config_error)
        raise ValueError("The Datasource was valid")

    return _invalid_ds_fct


@pytest.mark.unit
@pytest.mark.parametrize(
    "invalid_ds_cfg",
    [
        pytest.param(
            {
                "name": "my pg",
                "type": "postgres",
                "connection_string": "postmalone+psycopg2://postgres:@localhost/test_database",
            },
            id="invalid conn str",
        ),
        pytest.param(
            {
                "name": "my pg + asset",
                "type": "postgres",
                "connection_string": "postgresql+psycopg2://postgres:@localhost/test_database",
                "assets": [
                    {
                        "name": "my_table",
                        "type": "table",
                        "query": "table assets don't have a query",
                    }
                ],
            },
            id="invalid asset",
        ),
        pytest.param(
            {
                "name": "my snowflake",
                "type": "snowflake",
                "connection_string": "${MY_CONN_STR}",
                "user": "invalid_extra_field",
            },
            id="extra field",
        ),
    ],
)
class TestInvalidDatasource:
    def test_connection_raises_informative_error(
        self,
        invalid_ds_cfg: dict,
        invalid_datasource_factory: InvalidDSFn,
    ):
        invalid_ds = invalid_datasource_factory(invalid_ds_cfg)
        print(invalid_ds)

        with pytest.raises(TestConnectionError) as conn_err:
            invalid_ds.test_connection()
        print(f"{conn_err.value!r}\n >-- caused by -->\n{conn_err.value.__cause__!r}")
        assert invalid_ds.config_error == conn_err.value.__cause__

    def test_get_batch_list_raises_informative_error(
        self,
        invalid_ds_cfg: dict,
        invalid_datasource_factory: Callable[
            [dict[Literal["name", "type", "assets"] | Any, Any]], InvalidDatasource
        ],
    ):
        invalid_ds = invalid_datasource_factory(invalid_ds_cfg)
        with pytest.raises(TypeError) as err:
            invalid_ds.get_batch_list_from_batch_request({})
        assert invalid_ds.config_error == err.value.__cause__


if __name__ == "__main__":
    pytest.main(["-vv", __file__])
