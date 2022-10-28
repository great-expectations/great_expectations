from pprint import pformat as pf
from typing import MutableMapping

import pytest

from great_expectations.zep.context import _SourceFactories, get_context
from great_expectations.zep.interfaces import DataAsset, Datasource
from great_expectations.zep.metadatasource import MetaDatasource

param = pytest.param


@pytest.fixture(scope="function")
def context_sources_clean():
    """Return the sources object and clear and registered types/factories on teardown"""
    # setup
    sources = get_context().sources
    yield sources
    _SourceFactories._SourceFactories__source_factories.clear()
    _SourceFactories.type_lookup.clear()


class TestMetaDatasource:
    def test__new__registers_sources_factory_method(
        self, context_sources_clean: _SourceFactories
    ):
        expected_method_name = "add_my_test"

        ds_factory_method_initial = getattr(
            context_sources_clean, expected_method_name, None
        )
        assert ds_factory_method_initial is None, "Check test cleanup"

        class MyTestDatasource(metaclass=MetaDatasource):
            pass

        ds_factory_method_final = getattr(
            context_sources_clean, expected_method_name, None
        )

        assert (
            ds_factory_method_final
        ), f"{MetaDatasource.__name__}.__new__ failed to add `{expected_method_name}()` method"

    def test__new__updates_asset_type_lookup(
        self, context_sources_clean: _SourceFactories
    ):
        type_lookup = context_sources_clean.type_lookup

        class FooAsset(DataAsset):
            pass

        class BarAsset(DataAsset):
            pass

        class FooBarDatasource(metaclass=MetaDatasource):
            asset_types = [FooAsset, BarAsset]

        print(f" type_lookup ->\n{pf(type_lookup)}\n")

        asset_types = [FooAsset, BarAsset]

        registered_type_names = [type_lookup.get(t) for t in asset_types]
        for type_, name in zip(asset_types, registered_type_names):
            print(f"`{type_.__name__}` registered as '{name}'")
            assert name, f"{type.__name__} could not be retrieved"

        assert len(asset_types) == len(registered_type_names)


def test_minimal_ds_to_asset_flow(context_sources_clean):
    # 1. Define Datasource & Assets

    class RedAsset(DataAsset):
        pass

    class BlueAsset(DataAsset):
        pass

    class PurpleDatasource(metaclass=MetaDatasource):
        assets: MutableMapping[str, DataAsset]
        asset_types = [RedAsset, BlueAsset]

        def __init__(self, name: str) -> None:
            self.name = name
            self.assets = {}

        def get_asset(self, asset_name: str) -> DataAsset:
            return self.assets[asset_name]

        def add_red_asset(self, asset_name: str) -> RedAsset:
            asset = RedAsset(asset_name)
            self.assets[asset_name] = asset
            return asset

    # 2. Get context
    context = get_context()

    # 3. Add a datasource
    purple_ds: Datasource = context.sources.add_purple("my_ds_name")

    # 4. Add a DataAsset
    red_asset: DataAsset = purple_ds.add_red_asset("my_asset_name")
    assert isinstance(red_asset, RedAsset)

    # 5. Get an asset by name
    assert red_asset is purple_ds.get_asset("my_asset_name")


if __name__ == "__main__":
    pytest.main([__file__, "-vv", "--log-level=DEBUG"])
