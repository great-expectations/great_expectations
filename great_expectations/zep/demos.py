import logging
from typing import Dict, List

if __name__ == "__main__":
    # don't setup the logger unless being run as a script
    # TODO: remove this before release
    logging.basicConfig(level=logging.INFO, format="%(message)s")


from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.zep.context import get_context
from great_expectations.zep.interfaces import DataAsset, Datasource


class FileAsset(DataAsset):
    file_path: str
    delimiter: str
    ...


class MyOtherAsset(DataAsset):
    foo: str
    bar: List[int]


class PandasDatasource(Datasource):

    execution_engine = PandasExecutionEngine()
    asset_types = [FileAsset, MyOtherAsset]
    name: str
    assets: Dict[str, DataAsset]

    def __init__(self, name: str):
        self.name = name
        self.assets = {}

    def __repr__(self):
        return f"{self.__class__.__name__}(name='{self.name}')"

    def get_batch_list_from_batch_request(self, batch_request):
        """TODO"""
        pass

    def add_my_other_asset(self, asset_name: str) -> MyOtherAsset:
        """Create `MyOtherAsset` add it to `self.assets` and return it."""
        print(f"Adding {MyOtherAsset} - {asset_name}")
        asset = MyOtherAsset(asset_name)
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
    context = get_context()

    ds = context.sources.add_pandas("taxi")

    asset1 = ds.add_my_other_asset("bob")

    asset2 = ds.get_asset("bob")

    assert asset1 is asset2

    print("Successful Asset Roundtrip")


def type_lookup():
    """
    Demo the use of the `type_lookup` `BiDict`
    Alternatively use a Graph/Tree-like structure.
    """
    sources = get_context().sources
    print("\n  Datasource & DataAsset lookups ...")

    s = "pandas"
    pd_ds: PandasDatasource = sources.type_lookup[s]
    print(f"\n'{s}' -> {pd_ds}")

    pd_ds_assets = pd_ds.asset_types
    print(f"\n{pd_ds} -> {pd_ds_assets}")

    pd_ds_asset_names = [sources.type_lookup[t] for t in pd_ds_assets]
    print(f"\n{pd_ds_assets} -> {pd_ds_asset_names}")

    pd_ds_assets_from_names = [sources.type_lookup[name] for name in pd_ds_asset_names]
    print(f"\n{pd_ds_asset_names} -> {pd_ds_assets_from_names}")


if __name__ == "__main__":
    round_trip()
    type_lookup()
