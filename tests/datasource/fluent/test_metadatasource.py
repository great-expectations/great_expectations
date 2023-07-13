from __future__ import annotations

import copy
import inspect
import logging
import pathlib
from pprint import pformat as pf
from typing import TYPE_CHECKING, ClassVar, Dict, List, Optional, Tuple, Type, Union

import pytest
from pydantic import DirectoryPath, validate_arguments

from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import AbstractDataContext, FileDataContext
from great_expectations.datasource.fluent.batch_request import (
    BatchRequest,
    BatchRequestOptions,
)
from great_expectations.datasource.fluent.config import GxConfig
from great_expectations.datasource.fluent.constants import (
    _FLUENT_DATASOURCES_KEY,
)
from great_expectations.datasource.fluent.interfaces import (
    DataAsset,
    Datasource,
)
from great_expectations.datasource.fluent.metadatasource import MetaDatasource
from great_expectations.datasource.fluent.sources import (
    TypeRegistrationError,
    _SourceFactories,
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.util import get_context as get_gx_context

yaml = YAMLHandler()

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
                cls._context._add_fluent_datasource(datasource)
                # TODO: add assets?

        return cls._context

    @validate_arguments
    def __init__(self, context_root_dir: Optional[DirectoryPath] = None) -> None:
        self.root_directory = context_root_dir
        self._sources: _SourceFactories = _SourceFactories(self)
        self._datasources: Dict[str, Datasource] = {}
        self.config_provider: _ConfigurationProvider | None = None
        logger.info(f"Available Factories - {self._sources.factories}")
        logger.debug(f"`type_lookup` mapping ->\n{pf(self._sources.type_lookup)}")

    @property
    def sources(self) -> _SourceFactories:
        return self._sources

    def _add_fluent_datasource(self, datasource: Datasource) -> None:
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

    def _save_project_config(self, _fs_datasource=None) -> None:
        ...


def get_context(
    context_root_dir: Optional[DirectoryPath] = None, **kwargs
) -> DataContext:
    """Experimental get_context placeholder function."""
    logger.info(f"Getting context {context_root_dir or ''}")
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
        sources_copy = copy.deepcopy(_SourceFactories._SourceFactories__crud_registry)
        type_lookup_copy = copy.deepcopy(_SourceFactories.type_lookup)
        sources = get_context().sources

        assert (
            "add_datasource" not in sources.factories
        ), "Datasource base class should not be registered as a source factory"

        yield sources
    finally:
        _SourceFactories._SourceFactories__crud_registry = sources_copy
        _SourceFactories.type_lookup = type_lookup_copy


@pytest.fixture(scope="function")
def empty_sources(context_sources_cleanup) -> _SourceFactories:
    _SourceFactories._SourceFactories__crud_registry.clear()
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

        # One for each crud method: add, update, add_or_update, delete
        assert len(empty_sources.factories) == 4
        # type_lookup maps the MyTestDatasource.type str to the class and vis versa
        assert len(empty_sources.type_lookup) == 2
        assert "my_test" in empty_sources.type_lookup

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

        def test_connection(self):
            ...

    class BlueAsset(DataAsset):
        type = "blue"

        def test_connection(self):
            ...

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
            self._add_asset(asset=asset)
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


# Testing crud methods
DEFAULT_CRUD_DATASOURCE_NAME = "pandas_datasource"


@pytest.fixture
def context_config_data(
    file_dc_config_dir_init: pathlib.Path,
) -> Tuple[AbstractDataContext, pathlib.Path, pathlib.Path]:
    config_file_path = file_dc_config_dir_init / FileDataContext.GX_YML
    assert config_file_path.exists() is True
    context = get_gx_context(context_root_dir=file_dc_config_dir_init)
    data_dir = file_dc_config_dir_init.parent / "data"
    data_dir.mkdir()
    return context, config_file_path, data_dir


def assert_fluent_datasource_content(
    config_file_path: pathlib.Path, fluent_datasource_config: dict
):
    config = yaml.load(config_file_path.read_text())
    assert _FLUENT_DATASOURCES_KEY in config
    assert config[_FLUENT_DATASOURCES_KEY] == fluent_datasource_config


@pytest.fixture
def context_with_fluent_datasource(
    context_config_data: Tuple[AbstractDataContext, pathlib.Path, pathlib.Path]
) -> Tuple[AbstractDataContext, pathlib.Path, pathlib.Path]:
    context, config_file_path, data_dir = context_config_data
    assert 0 == len(context.datasources)
    context.sources.add_pandas_filesystem(
        name=DEFAULT_CRUD_DATASOURCE_NAME,
        base_directory=data_dir,
        data_context_root_directory=config_file_path.parent,
    )
    assert 1 == len(context.datasources)
    assert_fluent_datasource_content(
        config_file_path=config_file_path,
        fluent_datasource_config={
            DEFAULT_CRUD_DATASOURCE_NAME: {
                "base_directory": str(data_dir),
                "data_context_root_directory": str(config_file_path.parent),
                "type": "pandas_filesystem",
            },
        },
    )
    return context, config_file_path, data_dir


@pytest.mark.unit
def test_add_datasource(context_with_fluent_datasource):
    pass


@pytest.mark.unit
@pytest.mark.parametrize("use_positional_arg", [True, False])
def test_add_datasource_with_datasource_object(
    context_with_fluent_datasource, use_positional_arg
):
    context, config_file_path, data_dir = context_with_fluent_datasource
    new_datasource = copy.deepcopy(context.get_datasource(DEFAULT_CRUD_DATASOURCE_NAME))
    new_datasource.name = "new_datasource"
    if use_positional_arg:
        context.sources.add_pandas_filesystem(new_datasource)
    else:
        context.sources.add_pandas_filesystem(datasource=new_datasource)
    assert len(context.datasources) == 2
    assert_fluent_datasource_content(
        config_file_path=config_file_path,
        fluent_datasource_config={
            "pandas_datasource": {
                "base_directory": str(data_dir),
                "data_context_root_directory": str(config_file_path.parent),
                "type": "pandas_filesystem",
            },
            "new_datasource": {
                "base_directory": str(data_dir),
                "data_context_root_directory": str(config_file_path.parent),
                "type": "pandas_filesystem",
            },
        },
    )


@pytest.mark.unit
@pytest.mark.parametrize("use_positional_arg", [True, False])
def test_update_datasource(context_with_fluent_datasource, use_positional_arg):
    context, config_file_path, data_dir = context_with_fluent_datasource
    data_dir_2 = data_dir.parent / "data2"
    data_dir_2.mkdir()
    if use_positional_arg:
        context.sources.update_pandas_filesystem(
            DEFAULT_CRUD_DATASOURCE_NAME,
            base_directory=data_dir_2,
            data_context_root_directory=config_file_path.parent,
        )
    else:
        context.sources.update_pandas_filesystem(
            name=DEFAULT_CRUD_DATASOURCE_NAME,
            base_directory=data_dir_2,
            data_context_root_directory=config_file_path.parent,
        )
    assert_fluent_datasource_content(
        config_file_path=config_file_path,
        fluent_datasource_config={
            DEFAULT_CRUD_DATASOURCE_NAME: {
                "base_directory": str(data_dir_2),
                "data_context_root_directory": str(config_file_path.parent),
                "type": "pandas_filesystem",
            },
        },
    )


@pytest.mark.unit
@pytest.mark.parametrize("use_positional_arg", [True, False])
def test_update_datasource_with_datasource_object(
    context_with_fluent_datasource, use_positional_arg
):
    context, config_file_path, data_dir = context_with_fluent_datasource
    datasource = context.get_datasource(DEFAULT_CRUD_DATASOURCE_NAME)
    assert_fluent_datasource_content(
        config_file_path=config_file_path,
        fluent_datasource_config={
            DEFAULT_CRUD_DATASOURCE_NAME: {
                "base_directory": str(data_dir),
                "data_context_root_directory": str(config_file_path.parent),
                "type": "pandas_filesystem",
            },
        },
    )

    # Add an asset and update datasource
    (data_dir / "1.csv").touch()
    if use_positional_arg:
        datasource.add_csv_asset("csv_asset", batching_regex=r"(?P<file_name>.*).csv")
    else:
        datasource.add_csv_asset(
            name="csv_asset", batching_regex=r"(?P<file_name>.*).csv"
        )

    context.sources.update_pandas_filesystem(datasource=datasource)
    assert_fluent_datasource_content(
        config_file_path=config_file_path,
        fluent_datasource_config={
            DEFAULT_CRUD_DATASOURCE_NAME: {
                "base_directory": str(data_dir),
                "data_context_root_directory": str(config_file_path.parent),
                "type": "pandas_filesystem",
                "assets": {
                    "csv_asset": {
                        "batching_regex": "(?P<file_name>.*).csv",
                        "type": "csv",
                    },
                },
            },
        },
    )


@pytest.mark.unit
@pytest.mark.parametrize("use_positional_arg", [True, False])
def test_add_or_update_datasource_using_add(
    context_with_fluent_datasource, use_positional_arg
):
    context, config_file_path, data_dir = context_with_fluent_datasource
    data_dir_2 = data_dir.parent / "data2"
    data_dir_2.mkdir()
    if use_positional_arg:
        context.sources.add_or_update_pandas_filesystem(
            f"{DEFAULT_CRUD_DATASOURCE_NAME}_2",
            base_directory=data_dir_2,
            data_context_root_directory=config_file_path.parent,
        )
    else:
        context.sources.add_or_update_pandas_filesystem(
            name=f"{DEFAULT_CRUD_DATASOURCE_NAME}_2",
            base_directory=data_dir_2,
            data_context_root_directory=config_file_path.parent,
        )
    assert_fluent_datasource_content(
        config_file_path=config_file_path,
        fluent_datasource_config={
            f"{DEFAULT_CRUD_DATASOURCE_NAME}_2": {
                "base_directory": str(data_dir_2),
                "data_context_root_directory": str(config_file_path.parent),
                "type": "pandas_filesystem",
            },
            DEFAULT_CRUD_DATASOURCE_NAME: {
                "base_directory": str(data_dir),
                "data_context_root_directory": str(config_file_path.parent),
                "type": "pandas_filesystem",
            },
        },
    )


@pytest.mark.unit
@pytest.mark.parametrize("use_positional_arg", [True, False])
def test_add_or_update_datasource_using_update(
    context_with_fluent_datasource, use_positional_arg
):
    context, config_file_path, data_dir = context_with_fluent_datasource
    data_dir_2 = data_dir.parent / "data2"
    data_dir_2.mkdir()
    if use_positional_arg:
        context.sources.add_or_update_pandas_filesystem(
            DEFAULT_CRUD_DATASOURCE_NAME,
            base_directory=data_dir_2,
            data_context_root_directory=config_file_path.parent,
        )
    else:
        context.sources.add_or_update_pandas_filesystem(
            name=DEFAULT_CRUD_DATASOURCE_NAME,
            base_directory=data_dir_2,
            data_context_root_directory=config_file_path.parent,
        )
    assert_fluent_datasource_content(
        config_file_path=config_file_path,
        fluent_datasource_config={
            DEFAULT_CRUD_DATASOURCE_NAME: {
                "base_directory": str(data_dir_2),
                "data_context_root_directory": str(config_file_path.parent),
                "type": "pandas_filesystem",
            },
        },
    )


@pytest.mark.xfail(
    reason="There is a bug in context._save_config where deletions don't get persisted",
    run=True,
    strict=True,
)
@pytest.mark.unit
def test_delete_datasource(context_with_fluent_datasource):
    context, config_file_path, data_dir = context_with_fluent_datasource
    context.sources.delete(name=DEFAULT_CRUD_DATASOURCE_NAME)
    assert_fluent_datasource_content(config_file_path, {})


@pytest.mark.unit
def test_legacy_delete_datasource_raises_deprecation_warning(
    context_with_fluent_datasource,
):
    context, _, _ = context_with_fluent_datasource
    with pytest.deprecated_call():
        context.sources.delete_pandas_filesystem(name=DEFAULT_CRUD_DATASOURCE_NAME)


if __name__ == "__main__":
    pytest.main([__file__, "-vv", "--log-level=DEBUG"])
