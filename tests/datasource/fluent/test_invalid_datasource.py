from __future__ import annotations

import inspect
import random
from pprint import pformat as pf
from typing import Any, Callable, Final, Literal, Protocol

import pytest

from great_expectations.compatibility import pydantic
from great_expectations.datasource.fluent import (
    DataAsset,
    Datasource,
    GxInvalidDatasourceWarning,
    InvalidAsset,
    InvalidDatasource,
    TestConnectionError,
)
from great_expectations.datasource.fluent.sources import _SourceFactories

pytestmark = pytest.mark.unit

_EXCLUDE_METHODS: Final[set[str]] = {
    # we don't care about overriding these methods for InvalidDatasource
    "copy",
    "delete_asset",
    "delete_batch_config",
    "dict",
    "get_asset_names",
    "get_assets_as_dict",
    "get_batch_config",  # DataAsset
    "get_execution_engine",
    "json",
    "parse_order_by_sorters",
    "update_batch_config_field_set",  # DataAsset
    "yaml",
}
DATASOURCE_PUBLIC_METHODS: Final[list[str]] = [
    f[0]
    for f in inspect.getmembers(Datasource, predicate=inspect.isfunction)
    if not f[0].startswith("_") and f[0] not in _EXCLUDE_METHODS
]
DATA_ASSET_PUBLIC_METHODS: Final[list[str]] = [
    f[0]
    for f in inspect.getmembers(DataAsset, predicate=inspect.isfunction)
    if not f[0].startswith("_") and f[0] not in _EXCLUDE_METHODS
]


class TestPublicMethodsAreOverridden:
    """
    Ensure that applicable Datasource/DataAsset public methods are overridden.
    Applicable public methods are those that would typically be called when a users is trying to run some action on a Datasource
    and would want to know if the Datasource is invalid.

    If a method is not overridden, it will be inherited from the base class and will not be present in the InvalidDatasource.__dict__.
    """

    @pytest.mark.parametrize("base_ds_method_name", DATASOURCE_PUBLIC_METHODS)
    def test_datasource_methods(self, base_ds_method_name: str):
        """Ensure that InvalidDatasource overrides the applicable Datasource methods."""
        for base_ds_method_name in DATASOURCE_PUBLIC_METHODS:
            method = getattr(InvalidDatasource, base_ds_method_name, None)
            assert (
                method
            ), f"Expected {base_ds_method_name} to be defined on InvalidDatasource"
            with pytest.raises(TypeError):
                method()

    @pytest.mark.parametrize("base_ds_method_name", DATA_ASSET_PUBLIC_METHODS)
    def test_data_asset(self, base_ds_method_name: str):
        """Ensure that InvalidAsset overrides the applicable DataAsset methods."""
        for base_ds_method_name in DATA_ASSET_PUBLIC_METHODS:
            method = getattr(InvalidAsset, base_ds_method_name, None)
            assert (
                method
            ), f"Expected {base_ds_method_name} to be defined on InvalidAsset"
            with pytest.raises(TypeError):
                method()


@pytest.fixture(scope="module")
def datasource_fields() -> set[str]:
    """Return a set of all the fields in the base Datasource model."""
    return set(Datasource.__fields__.keys())


@pytest.fixture(scope="module")
def data_asset_fields() -> set[str]:
    """Return a set of all the fields in the base DataAsset model."""
    return set(DataAsset.__fields__.keys())


class InvalidDSFactory(Protocol):
    """
    Accept a datasource config and return an InvalidDatasource instance.
    Raises an error if the config was valid.
    """

    def __call__(
        self, config: dict[Literal["name", "type", "assets"] | Any, Any]
    ) -> InvalidDatasource:
        ...


@pytest.fixture
def invalid_datasource_factory() -> InvalidDSFactory:
    def _invalid_ds_fct(config: dict) -> InvalidDatasource:
        try:
            ds_type: type[Datasource] = _SourceFactories.type_lookup[config["type"]]
            ds_type(**config)
        except (pydantic.ValidationError, LookupError) as config_error:
            return InvalidDatasource(**config, config_error=config_error)
        raise ValueError("The Datasource was valid")

    return _invalid_ds_fct


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
                        "name": "my_bad_asset",
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
                "assets": [
                    {"name": "my_asset", "type": "table", "table_name": "foobar"}
                ],
            },
            id="extra field",
        ),
        pytest.param(
            {
                "name": "my pandas",
                "type": "pandas_filesystem",
                "assets": [
                    {"name": "my_asset", "type": "csv"},
                    {"name": "invalid_asset_type", "type": "whoops"},
                ],
            },
            id="pandas asset lookup error",
        ),
        pytest.param(
            {
                "name": "who knows",
                "type": "whoops",
            },
            id="datasource type lookup error",
        ),
    ],
)
class TestInvalidDatasource:
    def test_connection_raises_informative_error(
        self,
        invalid_ds_cfg: dict,
        invalid_datasource_factory: InvalidDSFactory,
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
            invalid_ds.get_batch_list_from_batch_request({})  # type: ignore[arg-type] # expect error
        assert invalid_ds.config_error == err.value.__cause__

    def test_random_attribute_access_raises_informative_error(
        self, invalid_ds_cfg: dict, invalid_datasource_factory: InvalidDSFactory
    ):
        invalid_ds = invalid_datasource_factory(invalid_ds_cfg)
        with pytest.raises(TypeError) as err:
            _ = invalid_ds.random_attribute
        assert invalid_ds.config_error == err.value.__cause__

    @pytest.mark.parametrize("attr_name", ["name", "id", "type", "assets"])
    def test_base_datasource_attribute_does_not_error(
        self,
        invalid_ds_cfg: dict,
        invalid_datasource_factory: InvalidDSFactory,
        attr_name: str,
    ):
        invalid_ds = invalid_datasource_factory(invalid_ds_cfg)
        attr_value = getattr(invalid_ds, attr_name)
        print(attr_name, attr_value)

    def test_get_asset_raises_warning(
        self,
        invalid_ds_cfg: dict,
        invalid_datasource_factory: Callable[
            [dict[Literal["name", "type", "assets"] | Any, Any]], InvalidDatasource
        ],
    ):
        invalid_ds = invalid_datasource_factory(invalid_ds_cfg)
        for asset in invalid_ds.assets:
            with pytest.warns(GxInvalidDatasourceWarning):
                invalid_asset = invalid_ds.get_asset(asset.name)
                assert invalid_asset, "No asset was returned"

            with pytest.raises(TestConnectionError):
                invalid_asset.test_connection()

    def test_extra_fields_are_ignored(
        self,
        datasource_fields: set[str],
        data_asset_fields: set[str],
        invalid_ds_cfg: dict,
        invalid_datasource_factory: Callable[
            [dict[Literal["name", "type", "assets"] | Any, Any]], InvalidDatasource
        ],
    ):
        """
        Ensure that extra fields are ignored when creating the InvalidDatasource instance.
        These fields could include secrets or other sensitive information that should not be included
        in the InvalidDatasource instance.

        Standard fields such as `type`, `name`, `id` etc. should be included in the InvalidDatasource instance and should
        never be sensitive.
        """
        print(f"Datasource config:\n{pf(invalid_ds_cfg)}")
        invalid_ds = invalid_datasource_factory(invalid_ds_cfg)

        ds_dict = invalid_ds._json_dict()
        print(f"\nInvalidDatasource dict:\n{pf(ds_dict)}")

        assert set(ds_dict.keys()) >= {
            "name",
            "type",
        }, "Expected standard fields to be present"

        extra_ds_fields = set(invalid_ds_cfg.keys()) - datasource_fields
        for field in extra_ds_fields:
            assert field not in ds_dict, f"Expected `{field}` to be ignored"

        for asset in invalid_ds.assets:
            asset_dict = asset.dict()
            extra_asset_fields = set(asset_dict.keys()) - data_asset_fields
            for field in extra_asset_fields:
                assert (
                    field not in asset_dict
                ), f"Expected asset `{field}` to be ignored"


@pytest.fixture
def rand_invalid_datasource_with_assets(
    invalid_datasource_factory: InvalidDSFactory,
) -> InvalidDatasource:
    random_ds_type = random.choice(
        [t for t in _SourceFactories.type_lookup.type_names()]
    )
    invalid_ds = invalid_datasource_factory(
        {
            "name": "my invalid ds",
            "type": random_ds_type,
            "connection_string": "postgresql+psycopg2://postgres:@localhost/test_database",
            "assets": [
                {"name": "definitely_invalid", "type": "NOT_A_VALID_TYPE"},
                {"name": "maybe_valid", "type": "table", "table_name": "my_table"},
                {"name": "maybe_valid_2", "type": "csv", "sep": "|"},
                {"name": "missing type"},
            ],
        }
    )
    return invalid_ds


class TestInvalidDataAsset:
    def test_connection_raises_informative_error(
        self, invalid_datasource_factory: InvalidDSFactory
    ):
        random_ds_type = random.choice(
            [t for t in _SourceFactories.type_lookup.type_names()]
        )
        print(f"{random_ds_type=}")
        invalid_datasource: InvalidDatasource = invalid_datasource_factory(
            {
                "name": "my invalid ds",
                "type": random_ds_type,
                "foo": "bar",  # regardless of the type this extra field should make the datasource invalid
                "assets": [
                    {"name": "definitely_invalid", "type": "NOT_A_VALID_TYPE"},
                    {"name": "maybe_valid", "type": "table", "table_name": "my_table"},
                    {"name": "maybe_valid_2", "type": "csv", "sep": "|"},
                    {"name": "missing type"},
                ],
            }
        )
        print(invalid_datasource)

        assert invalid_datasource.assets, "Expected assets to be present"
        for invalid_asset in invalid_datasource.assets:
            with pytest.raises(TestConnectionError) as conn_err:
                invalid_asset.test_connection()
                assert invalid_datasource.config_error == conn_err.value.__cause__

    @pytest.mark.parametrize("attr_name", ["name", "id", "type"])
    def test_base_data_asset_attribute_does_not_error(
        self, rand_invalid_datasource_with_assets: InvalidDatasource, attr_name: str
    ):
        assert (
            rand_invalid_datasource_with_assets.assets
        ), "Expected assets to be present"
        for asset in rand_invalid_datasource_with_assets.assets:
            value = getattr(asset, attr_name)
            print(attr_name, value)


if __name__ == "__main__":
    pytest.main(["-vv", __file__])
