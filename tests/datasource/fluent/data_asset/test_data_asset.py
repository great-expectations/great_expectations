import pytest

from great_expectations.core.data_context_key import DataContextVariableKey
from great_expectations.core.serializer import DictConfigSerializer
from great_expectations.data_context.store.datasource_store import DatasourceStore
from great_expectations.data_context.types.base import datasourceConfigSchema
from great_expectations.datasource.fluent.interfaces import DataAsset, Datasource
from great_expectations.datasource.fluent.pandas_datasource import PandasDatasource


@pytest.fixture
def datasource_name() -> str:
    return "my datasource for batch configs"


@pytest.fixture
def data_asset_name() -> str:
    return "my data asset for batch configs"


@pytest.fixture
def store(datasource_name: str) -> DatasourceStore:
    """Datasource store on datasource that has 2 assets. one of the assets has a batch config."""
    store = DatasourceStore(
        store_name="datasources",
        serializer=DictConfigSerializer(schema=datasourceConfigSchema),
    )
    datasource = PandasDatasource(name=datasource_name)

    key = DataContextVariableKey(resource_name=datasource_name)
    store.set(key=key, value=datasource)
    return store


@pytest.fixture
def empty_data_asset(
    store: DatasourceStore,
    datasource_name: str,
    data_asset_name: str,
) -> DataAsset:
    key = DataContextVariableKey(
        resource_name=datasource_name,
    )
    datasource = store.get(key)
    assert isinstance(datasource, Datasource)
    return datasource.add_csv_asset(data_asset_name, "taxi.csv")


def test_add_batch_config__success(empty_data_asset: DataAsset):
    name = "my batch config"
    batch_config = empty_data_asset.add_batch_config(name)

    assert batch_config.name == name
    assert batch_config.data_asset == empty_data_asset


def test_add_batch_config__persists(empty_data_asset: DataAsset):
    name = "my batch config"
    batch_config = empty_data_asset.add_batch_config(name)

    assert False, "Actually do the assertion!"

    assert batch_config.name == name
    assert batch_config.data_asset == empty_data_asset


def test_add_batch_config__multiple(empty_data_asset: DataAsset):
    empty_data_asset.add_batch_config("foo")
    empty_data_asset.add_batch_config("bar")

    assert len(empty_data_asset.batch_configs) == 2


def test_add_batch_config__duplicate_key(empty_data_asset: DataAsset):
    name = "my batch config"
    empty_data_asset.add_batch_config(name)

    with pytest.raises(ValueError) as e:
        empty_data_asset.add_batch_config(name)

    assert "already exists" in str(e.value)
