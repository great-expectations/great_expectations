from __future__ import annotations

import copy
import inspect
import logging
import pathlib
from pprint import pformat as pf
from typing import TYPE_CHECKING, ClassVar, Dict, Generator, List, Optional, Tuple, Type, Union

import pytest

from great_expectations.compatibility.pydantic import DirectoryPath, validate_arguments
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.partitioners import ColumnPartitioner
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import AbstractDataContext, FileDataContext
from great_expectations.data_context import get_context as get_gx_context
from great_expectations.datasource.fluent.batch_request import (
    BatchParameters,
    BatchRequest,
)
from great_expectations.datasource.fluent.config import GxConfig
from great_expectations.datasource.fluent.constants import (
    _FLUENT_DATASOURCES_KEY,
)
from great_expectations.datasource.fluent.data_connector.batch_filter import BatchSlice
from great_expectations.datasource.fluent.interfaces import (
    DataAsset,
    Datasource,
)
from great_expectations.datasource.fluent.metadatasource import MetaDatasource
from great_expectations.datasource.fluent.sources import (
    DataSourceManager,
    TypeRegistrationError,
)
from great_expectations.execution_engine import ExecutionEngine

yaml = YAMLHandler()

if TYPE_CHECKING:
    from great_expectations.core.config_provider import _ConfigurationProvider
    from great_expectations.datasource.datasource_dict import DatasourceDict
    from great_expectations.datasource.fluent.interfaces import Batch


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
    ):
        if not cls._context:
            cls._context = DataContext(context_root_dir=context_root_dir)

        assert cls._context
        return cls._context

    @validate_arguments
    def __init__(self, context_root_dir: Optional[DirectoryPath] = None) -> None:
        self.root_directory = context_root_dir
        self._data_sources: DataSourceManager = DataSourceManager(self)  # type: ignore[arg-type]
        self._datasources: Dict[str, Datasource] = {}
        self.config_provider: _ConfigurationProvider | None = None
        logger.info(f"Available Factories - {self._data_sources.factories}")
        logger.debug(f"`type_lookup` mapping ->\n{pf(self._data_sources.type_lookup)}")

    @property
    def data_sources(self) -> DataSourceManager:
        return self._data_sources

    @property
    def datasources(self) -> DatasourceDict:
        return self._datasources  # type: ignore[return-value]

    def _add_fluent_datasource(self, datasource: Datasource) -> Datasource:
        self._datasources[datasource.name] = datasource
        return datasource

    def _update_fluent_datasource(self, datasource: Datasource) -> Datasource:
        self._datasources[datasource.name] = datasource
        return datasource

    def _save_project_config(self) -> None: ...


def get_context(context_root_dir: Optional[DirectoryPath] = None, **kwargs):
    """Experimental get_context placeholder function."""
    logger.info(f"Getting context {context_root_dir or ''}")
    context = DataContext.get_context(context_root_dir=context_root_dir, **kwargs)
    return context


class DummyDataAsset(DataAsset):
    """Minimal Concrete DataAsset Implementation"""

    @override
    def build_batch_request(
        self,
        options: Optional[BatchParameters] = None,
        batch_slice: Optional[BatchSlice] = None,
        partitioner: Optional[ColumnPartitioner] = None,
    ) -> BatchRequest:
        return BatchRequest("datasource_name", "data_asset_name", options or {})

    @override
    def get_batch_identifiers_list(self, batch_request: BatchRequest) -> List[dict]:
        raise NotImplementedError

    @override
    def get_batch(self, batch_request: BatchRequest) -> Batch:
        raise NotImplementedError


@pytest.fixture(scope="function")
def context_sources_cleanup() -> Generator[DataSourceManager, None, None]:
    """Return the sources object and reset types/factories on teardown"""
    try:
        # setup
        sources_copy = copy.deepcopy(DataSourceManager._DataSourceManager__crud_registry)  # type: ignore[attr-defined]
        type_lookup_copy = copy.deepcopy(DataSourceManager.type_lookup)
        sources = get_context().data_sources

        assert (
            "add_datasource" not in sources.factories
        ), "Datasource base class should not be registered as a source factory"

        yield sources
    finally:
        DataSourceManager._DataSourceManager__crud_registry = sources_copy  # type: ignore[attr-defined]
        DataSourceManager.type_lookup = type_lookup_copy


@pytest.fixture(scope="function")
def empty_sources(context_sources_cleanup) -> Generator[DataSourceManager, None, None]:
    DataSourceManager._DataSourceManager__crud_registry.clear()  # type: ignore[attr-defined]
    DataSourceManager.type_lookup.clear()
    assert not DataSourceManager.type_lookup
    yield context_sources_cleanup


class DummyExecutionEngine(ExecutionEngine):
    def get_batch_data_and_markers(self, batch_spec):  # type: ignore[explicit-override] # FIXME
        raise NotImplementedError


@pytest.mark.unit
class TestMetaDatasource:
    def test__new__only_registers_expected_number_of_datasources_factories_and_types(
        self, empty_sources: DataSourceManager
    ):
        assert len(empty_sources.factories) == 0
        assert len(empty_sources.type_lookup) == 0

        class MyTestDatasource(Datasource):
            asset_types: ClassVar[List[Type[DataAsset]]] = []
            type: str = "my_test"

            @property
            @override
            def execution_engine_type(self) -> Type[ExecutionEngine]:
                return DummyExecutionEngine

        # One for each crud method: add, update, add_or_update, delete
        assert len(empty_sources.factories) == 4
        # type_lookup maps the MyTestDatasource.type str to the class and vis versa
        assert len(empty_sources.type_lookup) == 2
        assert "my_test" in empty_sources.type_lookup

    def test__new__registers_sources_factory_method(
        self, context_sources_cleanup: DataSourceManager
    ):
        expected_method_name = "add_my_test"

        ds_factory_method_initial = getattr(context_sources_cleanup, expected_method_name, None)
        assert ds_factory_method_initial is None, "Check test cleanup"

        class MyTestDatasource(Datasource):
            asset_types: ClassVar[List[Type[DataAsset]]] = []
            type: str = "my_test"

            @property
            @override
            def execution_engine_type(self) -> Type[ExecutionEngine]:
                return DummyExecutionEngine

        ds_factory_method_final = getattr(context_sources_cleanup, expected_method_name, None)

        assert (
            ds_factory_method_final
        ), f"{MetaDatasource.__name__}.__new__ failed to add `{expected_method_name}()` method"

    def test_registered_sources_factory_method_has_correct_signature(
        self, context_sources_cleanup: DataSourceManager
    ):
        expected_method_name = "add_my_test"

        class MyTestDatasource(Datasource):
            asset_types: ClassVar[List[Type[DataAsset]]] = []
            type: str = "my_test"
            extra_field: str

            @property
            @override
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

    def test__new__updates_asset_type_lookup(self, context_sources_cleanup: DataSourceManager):
        class FooAsset(DummyDataAsset):
            type: str = "foo"

        class BarAsset(DummyDataAsset):
            type: str = "bar"

        class FooBarDatasource(Datasource):
            asset_types: ClassVar[List[Type[DataAsset]]] = [FooAsset, BarAsset]
            type: str = "foo_bar"

            @property
            @override
            def execution_engine_type(self) -> Type[ExecutionEngine]:
                return DummyExecutionEngine

        print(f" type_lookup ->\n{pf(FooBarDatasource._type_lookup)}\n")
        asset_types = FooBarDatasource.asset_types
        assert asset_types, "No asset types have been declared"

        registered_type_names = [FooBarDatasource._type_lookup.get(t) for t in asset_types]
        for type_, name in zip(asset_types, registered_type_names):
            print(f"`{type_.__name__}` registered as '{name}'")
            assert name, f"{type.__name__} could not be retrieved"

        assert len(asset_types) == len(registered_type_names)


@pytest.mark.unit
class TestMisconfiguredMetaDatasource:
    def test_ds_type_field_not_set(self, empty_sources: DataSourceManager):
        with pytest.raises(
            TypeRegistrationError,
            match=r"`MissingTypeDatasource` is missing a `type` attribute",
        ):

            class MissingTypeDatasource(Datasource):
                @property
                @override
                def execution_engine_type(self) -> Type[ExecutionEngine]:
                    return DummyExecutionEngine

                @override
                def test_connection(self) -> None: ...  # type: ignore[override]

        # check that no types were registered
        assert len(empty_sources.type_lookup) < 1

    def test_ds_execution_engine_type_not_defined(self, empty_sources: DataSourceManager):
        class MissingExecEngineTypeDatasource(Datasource):
            type: str = "valid"

            @override
            def test_connection(self) -> None: ...  # type: ignore[override]

        with pytest.raises(NotImplementedError):
            MissingExecEngineTypeDatasource(name="name").get_execution_engine()

    def test_ds_assets_type_field_not_set(self, empty_sources: DataSourceManager):
        with pytest.raises(
            TypeRegistrationError,
            match="No `type` field found for `BadAssetDatasource.asset_types` -> `MissingTypeAsset` unable to register asset type",  # noqa: E501
        ):

            class MissingTypeAsset(DataAsset):
                @override
                def get_batch_identifiers_list(self, batch_request: BatchRequest) -> List[dict]:
                    raise NotImplementedError

                @override
                def get_batch(self, batch_request: BatchRequest) -> Batch:
                    raise NotImplementedError

            class BadAssetDatasource(Datasource):
                type: str = "valid"
                asset_types: ClassVar[List[Type[DataAsset]]] = [MissingTypeAsset]

                @property
                @override
                def execution_engine_type(self) -> Type[ExecutionEngine]:
                    return DummyExecutionEngine

                @override
                def test_connection(self) -> None: ...  # type: ignore[override]

        # check that no types were registered
        assert len(empty_sources.type_lookup) < 1

    def test_ds_test_connection_not_defined(self, empty_sources: DataSourceManager):
        class MissingTestConnectionDatasource(Datasource):
            type: str = "valid"

            @property
            @override
            def execution_engine_type(self) -> Type[ExecutionEngine]:
                return DummyExecutionEngine

        with pytest.raises(NotImplementedError):
            MissingTestConnectionDatasource(name="name").test_connection()


@pytest.mark.big
def test_minimal_ds_to_asset_flow(context_sources_cleanup):
    # 1. Define Datasource & Assets

    class SampleAsset(DataAsset):
        @override
        def get_batch_identifiers_list(self, batch_request: BatchRequest) -> List[dict]:
            raise NotImplementedError

        @override
        def get_batch(self, batch_request: BatchRequest) -> Batch:
            raise NotImplementedError

    class RedAsset(SampleAsset):
        type = "red"

        def test_connection(self): ...  # type: ignore[explicit-override] # FIXME

    class BlueAsset(SampleAsset):
        type = "blue"

        @override
        def test_connection(self): ...

    class PurpleDatasource(Datasource):
        asset_types = [RedAsset, BlueAsset]
        type: str = "purple"

        @property
        @override
        def execution_engine_type(self) -> Type[ExecutionEngine]:
            return DummyExecutionEngine

        def test_connection(self): ...  # type: ignore[explicit-override] # FIXME

        def add_red_asset(self, asset_name: str) -> RedAsset:
            asset = RedAsset(name=asset_name)  # type: ignore[call-arg] # ?
            self._add_asset(asset=asset)
            return asset

    # 2. Get context
    context = get_context()

    # 3. Add a datasource
    purple_ds: Datasource = context.data_sources.add_purple("my_ds_name")

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
    config_from_gx_yaml = config[_FLUENT_DATASOURCES_KEY]
    assert isinstance(config_from_gx_yaml, dict)
    config_from_gx_yaml_without_ids = _remove_ids(config_from_gx_yaml)
    assert config_from_gx_yaml_without_ids == fluent_datasource_config


def _remove_ids(config: dict) -> dict:
    for data_source in config.values():
        data_source.pop("id")
        for asset in data_source.get("assets", {}).values():
            asset.pop("id")
            for batch_definition in asset.get("batch_definitions", []):
                batch_definition.pop("id")

    return config


@pytest.fixture
def context_with_fluent_datasource(
    context_config_data: Tuple[AbstractDataContext, pathlib.Path, pathlib.Path],
) -> Tuple[AbstractDataContext, pathlib.Path, pathlib.Path]:
    context, config_file_path, data_dir = context_config_data
    assert len(context.data_sources.all()) == 0
    context.data_sources.add_pandas_filesystem(
        name=DEFAULT_CRUD_DATASOURCE_NAME,
        base_directory=data_dir,
        data_context_root_directory=config_file_path.parent,
    )
    assert len(context.data_sources.all()) == 1
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
def test_add_datasource_with_datasource_object(context_with_fluent_datasource, use_positional_arg):
    context, config_file_path, data_dir = context_with_fluent_datasource
    new_datasource = copy.deepcopy(context.data_sources.get(DEFAULT_CRUD_DATASOURCE_NAME))
    new_datasource.name = "new_datasource"
    if use_positional_arg:
        context.data_sources.add_pandas_filesystem(new_datasource)
    else:
        context.data_sources.add_pandas_filesystem(datasource=new_datasource)
    assert len(context.data_sources.all()) == 2
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
        context.data_sources.update_pandas_filesystem(
            DEFAULT_CRUD_DATASOURCE_NAME,
            base_directory=data_dir_2,
            data_context_root_directory=config_file_path.parent,
        )
    else:
        context.data_sources.update_pandas_filesystem(
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
    datasource = context.data_sources.get(DEFAULT_CRUD_DATASOURCE_NAME)
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
        datasource.add_csv_asset("csv_asset")
    else:
        datasource.add_csv_asset(name="csv_asset")

    context.data_sources.update_pandas_filesystem(datasource=datasource)
    assert_fluent_datasource_content(
        config_file_path=config_file_path,
        fluent_datasource_config={
            DEFAULT_CRUD_DATASOURCE_NAME: {
                "base_directory": str(data_dir),
                "data_context_root_directory": str(config_file_path.parent),
                "type": "pandas_filesystem",
                "assets": {
                    "csv_asset": {
                        "type": "csv",
                    },
                },
            },
        },
    )


@pytest.mark.unit
@pytest.mark.parametrize("use_positional_arg", [True, False])
def test_add_or_update_datasource_using_add(context_with_fluent_datasource, use_positional_arg):
    context, config_file_path, data_dir = context_with_fluent_datasource
    data_dir_2 = data_dir.parent / "data2"
    data_dir_2.mkdir()
    if use_positional_arg:
        context.data_sources.add_or_update_pandas_filesystem(
            f"{DEFAULT_CRUD_DATASOURCE_NAME}_2",
            base_directory=data_dir_2,
            data_context_root_directory=config_file_path.parent,
        )
    else:
        context.data_sources.add_or_update_pandas_filesystem(
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
def test_add_or_update_datasource_using_update(context_with_fluent_datasource, use_positional_arg):
    context, config_file_path, data_dir = context_with_fluent_datasource
    data_dir_2 = data_dir.parent / "data2"
    data_dir_2.mkdir()
    if use_positional_arg:
        context.data_sources.add_or_update_pandas_filesystem(
            DEFAULT_CRUD_DATASOURCE_NAME,
            base_directory=data_dir_2,
            data_context_root_directory=config_file_path.parent,
        )
    else:
        context.data_sources.add_or_update_pandas_filesystem(
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
    context, config_file_path, _data_dir = context_with_fluent_datasource
    context.data_sources.delete(name=DEFAULT_CRUD_DATASOURCE_NAME)
    assert_fluent_datasource_content(config_file_path, {})


@pytest.mark.unit
def test_legacy_delete_datasource_raises_deprecation_warning(
    context_with_fluent_datasource,
):
    context, _, _ = context_with_fluent_datasource
    with pytest.deprecated_call():
        context.data_sources.delete_pandas_filesystem(name=DEFAULT_CRUD_DATASOURCE_NAME)


if __name__ == "__main__":
    pytest.main([__file__, "-vv", "--log-level=DEBUG"])
