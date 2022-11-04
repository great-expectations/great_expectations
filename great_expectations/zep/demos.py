import logging
import pathlib
import shutil
from typing import Dict, Literal

from pydantic import FilePath, ValidationError

TERM_WIDTH = shutil.get_terminal_size()[1]
SEPARATOR = "-" * TERM_WIDTH

if __name__ == "__main__":
    # don't setup the logger unless being run as a script
    # TODO: remove this before release
    logging.basicConfig(level=logging.INFO, format="%(message)s")


from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.zep.context import get_context
from great_expectations.zep.interfaces import DataAsset, Datasource


class FileAsset(DataAsset):
    type: Literal["file"] = "file"
    file_path: FilePath
    delimiter: str = ","
    ...


class MyOtherAsset(DataAsset):
    type: Literal["file"] = "file"
    ...


class PandasDatasource(Datasource):
    engine: Literal["pandas"] = "pandas"
    execution_engine = PandasExecutionEngine()
    asset_types = [FileAsset, MyOtherAsset]
    name: str
    assets: Dict[str, DataAsset]

    def __init__(self, name: str):
        self.name = name
        self.assets = {}
        # self.execution_engine = PandasExecutionEngine()

    def __repr__(self):
        return f"{self.__class__.__name__}(name='{self.name}')"

    def get_batch_list_from_batch_request(self, batch_request):
        """TODO"""
        pass

    def add_my_other_asset(self, asset_name: str) -> MyOtherAsset:
        """Create `MyOtherAsset` add it to `self.assets` and return it."""
        print(f"Adding {MyOtherAsset.__name__} - {asset_name}")
        asset = MyOtherAsset(name=asset_name)
        self.assets[asset_name] = asset
        return asset

    def add_file_asset(self, asset_name: str, **kwargs) -> FileAsset:
        """Create `FileAsset` add it to `self.assets` and return it."""
        print(f"Adding {FileAsset.__name__} - {asset_name}")
        asset = FileAsset(name=asset_name, **kwargs)
        self.assets[asset_name] = asset
        return asset


# class TableAsset:
#     pass


# class PostgresDatasource(metaclass=MetaDatasource):
#     asset_types = [TableAsset]

#     def __init__(self, name: str, connection_str: str):
#         self.name = name
#         self.connection_str = connection_str


def round_trip():
    """Demo Creating Datasource -> Adding Assets -> Retrieving asset by name"""
    print(f"\n  Adding and round tripping a toy DataAsset ...\n{SEPARATOR}")
    context = get_context()

    ds = context.sources.add_pandas("taxi")

    asset1 = ds.add_my_other_asset("bob")

    asset2 = ds.get_asset("bob")

    assert asset1 is asset2

    print("Successful Asset Roundtrip\n")


def type_lookup():
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


def add_real_asset():
    print(f"\n  Add a 'real' asset ...\n{SEPARATOR}")
    context = get_context()

    ds: PandasDatasource = context.sources.add_pandas("my_pandas_datasource")

    try:
        ds.add_file_asset("my_file_1", file_path="not_a_file")
    except ValidationError as exc:
        print(f"\n  Pydantic Validation catches problems\n{exc}\n")
        ds.add_file_asset("my_file_2", file_path=__file__)

    my_asset = ds.get_asset("my_file_2")
    print(my_asset)


def from_yaml_config():
    print(f"\n  Load from a yaml config file\n{SEPARATOR}")
    root_dir = pathlib.Path(__file__).parent
    context = get_context(context_root_dir=root_dir)
    print(f"\n  Context loaded from {root_dir}")

    my_ds = context.get_datasource("my_demo_datasource")
    print(f"\n  Retrieved '{my_ds.name}'->\n{repr(my_ds)}")
    assert my_ds

    my_asset = my_ds.assets["my_demo_file_asset"]

    print(f"\n Retrieved '{my_asset.name}'->\n{repr(my_asset)}")
    assert my_asset.file_path.exists()


if __name__ == "__main__":
    # round_trip()
    # type_lookup()
    # add_real_asset()
    from_yaml_config()
