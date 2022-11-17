from __future__ import annotations

import pathlib
import shutil
from typing import TYPE_CHECKING, Dict, List, Optional, Type, Union

from pydantic import FilePath, ValidationError
from typing_extensions import ClassVar, Literal

from great_expectations.zep.config import GxConfig
from great_expectations.zep.fakes import sqlachemy_execution_engine_mock_cls

if TYPE_CHECKING:
    from great_expectations.zep.postgres_datasource import (
        PostgresDatasource,
        TableAsset,
    )


try:
    from devtools import debug as pp
except ImportError:
    from pprint import pprint as pp

GX_ROOT = pathlib.Path(__file__).parent.parent
SAMPLE_CONFIG_FILE = GX_ROOT.joinpath("..", "tests", "zep", "config.yaml").resolve()

TERM_WIDTH = shutil.get_terminal_size()[1]
SEPARATOR = "-" * TERM_WIDTH


if __name__ == "__main__":
    # don't setup the logger unless being run as a script
    # TODO: remove this before release
    from great_expectations.zep.logger import init_logger

    init_logger()


from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.zep.context import get_context
from great_expectations.zep.interfaces import (
    BatchRequest,
    BatchRequestOptions,
    DataAsset,
    Datasource,
)


class FileAsset(DataAsset):
    type: Literal["file"] = "file"

    file_path: FilePath
    delimiter: Literal[",", "|", "\t", " "] = ","

    def get_batch_request(self, options: Optional[BatchRequestOptions]) -> BatchRequest:
        return BatchRequest("datasource_name", "data_asset_name", options or {})


class OtherAsset(DataAsset):
    type: Literal["other"] = "other"

    food: Literal["pizza", "bad pizza", "good pizza"]

    def get_batch_request(self, options: Optional[BatchRequestOptions]) -> BatchRequest:
        return BatchRequest("datasource_name", "data_asset_name", options or {})


class PandasDatasource(Datasource):
    asset_types: ClassVar[List[Type[DataAsset]]] = [
        FileAsset,
        OtherAsset,
    ]

    type: Literal["pandas"] = "pandas"
    execution_engine: PandasExecutionEngine
    assets: Dict[str, Union[FileAsset, OtherAsset]]

    def __repr__(self):
        return f"{self.__class__.__name__}(name='{self.name}')"

    def get_batch_list_from_batch_request(self, batch_request):
        """TODO"""
        pass

    def add_my_other_asset(self, asset_name: str) -> OtherAsset:
        """Create `MyOtherAsset` add it to `self.assets` and return it."""
        print(f"Adding {OtherAsset.__name__} - {asset_name}")
        asset = OtherAsset(name=asset_name, food="bad pizza")
        self.assets[asset_name] = asset
        return asset

    def add_file_asset(self, name: str, **kwargs) -> FileAsset:
        """Create `FileAsset` add it to `self.assets` and return it."""
        print(f"Adding {FileAsset.__name__} - {name}")
        asset = FileAsset(name=name, **kwargs)
        self.assets[name] = asset
        return asset


def round_trip() -> None:
    """Demo Creating Datasource -> Adding Assets -> Retrieving asset by name"""
    print(f"\n  Adding and round tripping a toy DataAsset ...\n{SEPARATOR}")
    context = get_context()

    ds = context.sources.add_pandas("taxi")

    asset1 = ds.add_my_other_asset("bob")

    asset2 = ds.get_asset("bob")

    assert asset1 is asset2

    print("Successful Asset Roundtrip\n")


def type_lookup() -> None:
    """
    Demo the use of the `type_lookup` `BiDict`
    Alternatively use a Graph/Tree-like structure.
    """
    print(f"\n . Datasource & DataAsset lookups ...\n{SEPARATOR}")
    sources = get_context().sources

    s = "pandas"
    pd_ds: PandasDatasource = sources.type_lookup[s]
    print(f"\n'{s}' -> {pd_ds}")

    pd_ds_assets = pd_ds.asset_types
    print(f"\n{pd_ds} -> {pd_ds_assets}")

    pd_ds_asset_names = [sources.type_lookup[t] for t in pd_ds_assets]
    print(f"\n{pd_ds_assets} -> {pd_ds_asset_names}")

    pd_ds_assets_from_names = [sources.type_lookup[name] for name in pd_ds_asset_names]
    print(f"\n{pd_ds_asset_names} -> {pd_ds_assets_from_names}")


def add_real_asset() -> None:
    print(f"\n  Add a 'real' asset ...\n{SEPARATOR}")
    context = get_context()

    ds: PandasDatasource = context.sources.add_pandas("my_pandas_datasource")

    try:
        ds.add_file_asset("my_file_1", file_path="not_a_file")
    except ValidationError as exc:
        print(f"\n  Pydantic Validation catches problems\n{exc}\n")
        ds.add_file_asset("my_file_2", file_path=__file__)

    my_asset = ds.get_asset("my_file_2")
    pp(my_asset)


def from_yaml_config() -> None:
    print(f"\n  Load from a yaml config file\n{SEPARATOR}")
    root_dir = SAMPLE_CONFIG_FILE.parent
    context = get_context(context_root_dir=root_dir)
    print(f"\n  Context loaded from {root_dir}")

    my_ds: PostgresDatasource = context.get_datasource("my_pg_ds")  # type: ignore[assignment]
    print(f"\n  Retrieved '{my_ds.name}'->")
    pp(my_ds)
    assert my_ds

    my_asset: TableAsset = my_ds.get_asset("with_splitters")

    print(f"\n Retrieved '{my_asset.name}'->")
    pp(my_asset)
    assert my_asset.column_splitter.method_name  # type: ignore[union-attr] # could be none


def pg_ds_nested_within_asset() -> None:
    print(
        f"\n  Postgres datasource asset with nested datasource (from config)\n{SEPARATOR}"
    )

    config = GxConfig.parse_yaml(SAMPLE_CONFIG_FILE)

    my_ds = config.datasources["my_pg_ds"]
    pp(my_ds)

    my_asset = my_ds.get_asset("my_table_asset_wo_splitters")

    assert (
        my_ds.execution_engine is my_asset.datasource.execution_engine
    ), "Engine Failed Identity check"

    assert my_ds.connection_str == my_asset.datasource.connection_str  # type: ignore[attr-defined]

    assert my_ds == my_asset.datasource, "DS Failed Equality check"

    # assert my_ds is my_asset.datasource, "DS Failed Identity check"  # this fails


if __name__ == "__main__":
    # round_trip()
    # type_lookup()
    # add_real_asset()
    PostgresDatasource.execution_engine_override = sqlachemy_execution_engine_mock_cls(
        lambda x: None
    )
    from_yaml_config()
    pg_ds_nested_within_asset()
