from __future__ import annotations

import copy
import inspect
import logging
import pathlib
from pprint import pformat as pf
from typing import ClassVar, Dict, List, Optional, Type, Union, TYPE_CHECKING

import pytest
from pydantic import DirectoryPath, validate_arguments

from great_expectations.datasource.fluent.config import GxConfig
from great_expectations.datasource.fluent.interfaces import (
    BatchRequest,
    BatchRequestOptions,
    DataAsset,
    Datasource,
)
from great_expectations.datasource.fluent.metadatasource import MetaDatasource
from great_expectations.datasource.fluent.sources import (
    TypeRegistrationError,
    _SourceFactories,
)
from great_expectations.execution_engine import ExecutionEngine

if TYPE_CHECKING:
    from great_expectations.core.config_provider import _ConfigurationProvider


logger = logging.getLogger(__name__)


# Fake DataContext for testing
class DataContext:
    """
    This is just a scaffold for exploring and iterating on our fluent datasources.

    Use `great_expectations.get_context()` for a real DataContext.
    """

    _context: ClassVar[Optional[DataContext]] = None
    _config: ClassVar[Optional[GxConfig]] = None  # (kilo59) should this live  here?

    _datasources: Dict[str, Datasource]
    root_directory: Union[DirectoryPath, str, None]

    @classmethod
    def get_context(
        cls,
        context_root_dir: Optional[DirectoryPath] = None,
        _config_file: str = "config.yaml",  # for ease of use during POC
    ) -> DataContext:
        if not cls._context:
            cls._context = DataContext(context_root_dir=context_root_dir)

        assert cls._context
        if cls._context.root_directory:
            # load config and add/instantiate Datasources & Assets
            config_path = pathlib.Path(cls._context.root_directory) / _config_file
            cls._config = GxConfig.parse_yaml(config_path)
            for ds_name, datasource in cls._config.datasources.items():
                logger.info(f"Loaded '{ds_name}' from config")
                cls._context._attach_datasource_to_context(datasource)
                # TODO: add assets?

        return cls._context

    @validate_arguments
    def __init__(self, context_root_dir: Optional[DirectoryPath] = None) -> None:
        self.root_directory = context_root_dir
        self._sources: _SourceFactories = _SourceFactories(self)
        self._datasources: Dict[str, Datasource] = {}
        self.config_provider: _ConfigurationProvider | None = None
        logger.info(f"4a. Available Factories - {self._sources.factories}")
        logger.debug(f"4b. `type_lookup` mapping ->\n{pf(self._sources.type_lookup)}")

    @property
    def sources(self) -> _SourceFactories:
        return self._sources

    def _attach_datasource_to_context(self, datasource: Datasource) -> None:
        self._datasources[datasource.name] = datasource

    def get_datasource(self, datasource_name: str) -> Datasource:
        # NOTE: this same method exists on AbstractDataContext
        # TODO (kilo59): implement as __getitem__ ?
        try:
            return self._datasources[datasource_name]
        except KeyError as exc:
            raise LookupError(
                f"'{datasource_name}' not found. Available datasources are {list(self._datasources.keys())}"
            ) from exc


def get_context(
    context_root_dir: Optional[DirectoryPath] = None, **kwargs
) -> DataContext:
    """Experimental get_context placeholder function."""
    logger.info(f"3. Getting context {context_root_dir or ''}")
    context = DataContext.get_context(context_root_dir=context_root_dir, **kwargs)
    return context


class DummyDataAsset(DataAsset):
    """Minimal Concrete DataAsset Implementation"""

    def build_batch_request(
        self, options: Optional[BatchRequestOptions]
    ) -> BatchRequest:
        return BatchRequest("datasource_name", "data_asset_name", options or {})


@pytest.fixture(scope="function")
def context_sources_cleanup() -> _SourceFactories:
    """Return the sources object and reset types/factories on teardown"""
    try:
        # setup
        sources_copy = copy.deepcopy(
            _SourceFactories._SourceFactories__source_factories
        )
        type_lookup_copy = copy.deepcopy(_SourceFactories.type_lookup)
        sources = get_context().sources

        assert (
            "add_datasource" not in sources.factories
        ), "Datasource base class should not be registered as a source factory"

        yield sources
    finally:
        _SourceFactories._SourceFactories__source_factories = sources_copy
        _SourceFactories.type_lookup = type_lookup_copy


@pytest.fixture(scope="function")
def empty_sources(context_sources_cleanup) -> _SourceFactories:
    _SourceFactories._SourceFactories__source_factories.clear()
    _SourceFactories.type_lookup.clear()
    assert not _SourceFactories.type_lookup
    yield context_sources_cleanup


class DummyExecutionEngine(ExecutionEngine):
    def get_batch_data_and_markers(self, batch_spec):
        raise NotImplementedError


@pytest.mark.unit
class TestMetaDatasource:
    def test__new__only_registers_expected_number_of_datasources_factories_and_types(
        self, empty_sources: _SourceFactories
    ):
        assert len(empty_sources.factories) == 0
        assert len(empty_sources.type_lookup) == 0

        class MyTestDatasource(Datasource):
            asset_types: ClassVar[List[Type[DataAsset]]] = []
            type: str = "my_test"

            @property
            def execution_engine_type(self) -> Type[ExecutionEngine]:
                return DummyExecutionEngine

        expected_registrants = 1

        assert len(empty_sources.factories) == expected_registrants
        assert len(empty_sources.type_lookup) == 2 * expected_registrants

    def test__new__registers_sources_factory_method(
        self, context_sources_cleanup: _SourceFactories
    ):
        expected_method_name = "add_my_test"

        ds_factory_method_initial = getattr(
            context_sources_cleanup, expected_method_name, None
        )
        assert ds_factory_method_initial is None, "Check test cleanup"

        class MyTestDatasource(Datasource):
            asset_types: ClassVar[List[Type[DataAsset]]] = []
            type: str = "my_test"

            @property
            def execution_engine_type(self) -> Type[ExecutionEngine]:
                return DummyExecutionEngine

        ds_factory_method_final = getattr(
            context_sources_cleanup, expected_method_name, None
        )

        assert (
            ds_factory_method_final
        ), f"{MetaDatasource.__name__}.__new__ failed to add `{expected_method_name}()` method"

    def test_registered_sources_factory_method_has_correct_signature(
        self, context_sources_cleanup: _SourceFactories
    ):
        expected_method_name = "add_my_test"

        class MyTestDatasource(Datasource):
            asset_types: ClassVar[List[Type[DataAsset]]] = []
            type: str = "my_test"
            extra_field: str

            @property
            def execution_engine_type(self) -> Type[ExecutionEngine]:
                return DummyExecutionEngine

        ds_factory_method = getattr(context_sources_cleanup, expected_method_name)

        ds_factory_method_sig = inspect.signature(ds_factory_method)
        my_ds_init_method_sig = inspect.signature(MyTestDatasource)
        print(f"{MyTestDatasource.__name__}.__init__()\n  {my_ds_init_method_sig}\n")
        print(f"{expected_method_name}()\n  {ds_factory_method_sig}\n")

        for i, param_name in enumerate(my_ds_init_method_sig.parameters):
            print(f"{i} {param_name} ", end="")

            if param_name in ["type", "assets"]:
                assert (
                    param_name not in ds_factory_method_sig.parameters
                ), f"{param_name} should not be part of the `add_<DATASOURCE_TYPE>` method"
                print("⏩")
                continue

            assert param_name in ds_factory_method_sig.parameters
            print("✅")

    def test__new__updates_asset_type_lookup(
        self, context_sources_cleanup: _SourceFactories
    ):
        class FooAsset(DummyDataAsset):
            type: str = "foo"

        class BarAsset(DummyDataAsset):
            type: str = "bar"

        class FooBarDatasource(Datasource):
            asset_types: ClassVar[List[Type[DataAsset]]] = [FooAsset, BarAsset]
            type: str = "foo_bar"

            @property
            def execution_engine_type(self) -> Type[ExecutionEngine]:
                return DummyExecutionEngine

        print(f" type_lookup ->\n{pf(FooBarDatasource._type_lookup)}\n")
        asset_types = FooBarDatasource.asset_types
        assert asset_types, "No asset types have been declared"

        registered_type_names = [
            FooBarDatasource._type_lookup.get(t) for t in asset_types
        ]
        for type_, name in zip(asset_types, registered_type_names):
            print(f"`{type_.__name__}` registered as '{name}'")
            assert name, f"{type.__name__} could not be retrieved"

        assert len(asset_types) == len(registered_type_names)


@pytest.mark.unit
class TestMisconfiguredMetaDatasource:
    def test_ds_type_field_not_set(self, empty_sources: _SourceFactories):

        with pytest.raises(
            TypeRegistrationError,
            match=r"`MissingTypeDatasource` is missing a `type` attribute",
        ):

            class MissingTypeDatasource(Datasource):
                @property
                def execution_engine_type(self) -> Type[ExecutionEngine]:
                    return DummyExecutionEngine

                def test_connection(self) -> None:
                    ...

        # check that no types were registered
        assert len(empty_sources.type_lookup) < 1

    def test_ds_execution_engine_type_not_defined(
        self, empty_sources: _SourceFactories
    ):
        class MissingExecEngineTypeDatasource(Datasource):
            type: str = "valid"

            def test_connection(self) -> None:
                ...

        with pytest.raises(NotImplementedError):
            MissingExecEngineTypeDatasource(name="name").get_execution_engine()

    def test_ds_assets_type_field_not_set(self, empty_sources: _SourceFactories):

        with pytest.raises(
            TypeRegistrationError,
            match="No `type` field found for `BadAssetDatasource.asset_types` -> `MissingTypeAsset` unable to register asset type",
        ):

            class MissingTypeAsset(DataAsset):
                pass

            class BadAssetDatasource(Datasource):
                type: str = "valid"
                asset_types: ClassVar[List[Type[DataAsset]]] = [MissingTypeAsset]

                @property
                def execution_engine_type(self) -> Type[ExecutionEngine]:
                    return DummyExecutionEngine

                def test_connection(self) -> None:
                    ...

        # check that no types were registered
        assert len(empty_sources.type_lookup) < 1

    def test_ds_test_connection_not_defined(self, empty_sources: _SourceFactories):
        class MissingTestConnectionDatasource(Datasource):
            type: str = "valid"

            @property
            def execution_engine_type(self) -> Type[ExecutionEngine]:
                return DummyExecutionEngine

        with pytest.raises(NotImplementedError):
            MissingTestConnectionDatasource(name="name").test_connection()


def test_minimal_ds_to_asset_flow(context_sources_cleanup):
    # 1. Define Datasource & Assets

    class RedAsset(DataAsset):
        type = "red"

    class BlueAsset(DataAsset):
        type = "blue"

    class PurpleDatasource(Datasource):
        asset_types = [RedAsset, BlueAsset]
        type: str = "purple"

        @property
        def execution_engine_type(self) -> Type[ExecutionEngine]:
            return DummyExecutionEngine

        def test_connection(self):
            ...

        def add_red_asset(self, asset_name: str) -> RedAsset:
            asset = RedAsset(name=asset_name)
            self.assets[asset_name] = asset
            return asset

    # 2. Get context
    context = get_context()

    # 3. Add a datasource
    purple_ds: Datasource = context.sources.add_purple("my_ds_name")

    # 4. Add a DataAsset
    red_asset: DataAsset = purple_ds.add_red_asset("my_asset_name")
    assert isinstance(red_asset, RedAsset)

    # 5. Get an asset by name - (method defined in parent `Datasource`)
    assert red_asset is purple_ds.get_asset("my_asset_name")


if __name__ == "__main__":
    pytest.main([__file__, "-vv", "--log-level=DEBUG"])
