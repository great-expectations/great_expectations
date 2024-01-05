import pytest

from great_expectations.core.batch_config import BatchConfig
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.datasource.fluent.interfaces import DataAsset, Datasource
from great_expectations.datasource.fluent.pandas_datasource import PandasDatasource


@pytest.fixture
def datasource_name() -> str:
    return "my datasource for batch configs"


@pytest.fixture
def empty_data_asset_name() -> str:
    return "my data asset for batch configs"


@pytest.fixture
def data_asset_with_batch_config_name() -> str:
    return "I have batch configs"


@pytest.fixture
def batch_config_name() -> str:
    return "my batch config"


@pytest.fixture
def context(empty_data_context: AbstractDataContext) -> AbstractDataContext:
    return empty_data_context


@pytest.fixture
def context_with_asset(
    context: AbstractDataContext,
    datasource_name: str,
    empty_data_asset_name: str,
    data_asset_with_batch_config_name: str,
    batch_config_name: str,
) -> AbstractDataContext:
    """Context with a datasource that has 2 assets. one of the assets has a batch config."""
    datasource = context.sources.add_pandas(datasource_name)
    datasource.add_csv_asset(empty_data_asset_name, "taxi.csv")  # type: ignore [arg-type]
    datasource.add_csv_asset(
        data_asset_with_batch_config_name,
        "taxi.csv",  # type: ignore [arg-type]
    ).add_batch_config(batch_config_name)

    return context


@pytest.fixture
def datasource(
    context_with_asset: AbstractDataContext, datasource_name: str
) -> PandasDatasource:
    output = context_with_asset.get_datasource(datasource_name)
    assert isinstance(output, PandasDatasource)
    return output


@pytest.fixture
def empty_data_asset(empty_data_asset_name: str, datasource: Datasource) -> DataAsset:
    return datasource.get_asset(empty_data_asset_name)


@pytest.fixture
def data_asset_with_batch_config(
    data_asset_with_batch_config_name: str, datasource: Datasource
) -> DataAsset:
    return datasource.get_asset(data_asset_with_batch_config_name)


@pytest.fixture
def persisted_batch_config(
    data_asset_with_batch_config: DataAsset,
) -> BatchConfig:
    return data_asset_with_batch_config.batch_configs[0]


@pytest.mark.unit
def test_add_batch_config__success(empty_data_asset: DataAsset):
    name = "my batch config"
    batch_config = empty_data_asset.add_batch_config(name)

    assert batch_config.name == name
    assert batch_config.data_asset == empty_data_asset
    assert empty_data_asset.batch_configs == [batch_config]


@pytest.mark.unit
def test_add_batch_config__persists(
    context: AbstractDataContext,
    empty_data_asset: DataAsset,
    datasource_name: str,
    empty_data_asset_name: str,
):
    name = "my batch config"
    batch_config = empty_data_asset.add_batch_config(name)

    loaded_datasource = context.get_datasource(datasource_name)
    assert isinstance(loaded_datasource, Datasource)
    loaded_asset = loaded_datasource.get_asset(empty_data_asset_name)

    assert loaded_asset.batch_configs == [batch_config]


@pytest.mark.unit
def test_add_batch_config__multiple(empty_data_asset: DataAsset):
    empty_data_asset.add_batch_config("foo")
    empty_data_asset.add_batch_config("bar")

    assert len(empty_data_asset.batch_configs) == 2


@pytest.mark.unit
def test_add_batch_config__duplicate_key(empty_data_asset: DataAsset):
    name = "my batch config"
    empty_data_asset.add_batch_config(name)

    with pytest.raises(ValueError, match="already exists"):
        empty_data_asset.add_batch_config(name)


@pytest.mark.unit
def test_add_batch_config__file_data__does_not_clobber_other_assets(
    context_with_asset: AbstractDataContext,
    datasource_name: str,
):
    ds1 = context_with_asset.get_datasource(datasource_name)
    ds2 = context_with_asset.get_datasource(datasource_name)
    assert isinstance(ds1, PandasDatasource)
    assert isinstance(ds2, PandasDatasource)

    my_asset = ds1.add_csv_asset("my asset", "taxi.csv")  # type: ignore [arg-type]
    your_asset = ds2.add_csv_asset("your asset", "taxi.csv")  # type: ignore [arg-type]

    my_batch_config = my_asset.add_batch_config("my batch config")
    your_batch_config = your_asset.add_batch_config("your batch config")

    loaded_datasource = context_with_asset.get_datasource(datasource_name)
    assert isinstance(loaded_datasource, Datasource)
    assert loaded_datasource.get_asset(my_asset.name).batch_configs == [my_batch_config]
    assert loaded_datasource.get_asset(your_asset.name).batch_configs == [
        your_batch_config
    ]


@pytest.mark.unit
def test_add_batch_config__file_data__does_not_clobber_other_batch_configs(
    context_with_asset: AbstractDataContext,
    datasource_name: str,
    empty_data_asset_name: str,
):
    ds1 = context_with_asset.get_datasource(datasource_name)
    ds2 = context_with_asset.get_datasource(datasource_name)
    assert isinstance(ds1, PandasDatasource)
    assert isinstance(ds2, PandasDatasource)

    asset_1 = ds1.get_asset(empty_data_asset_name)
    asset_2 = ds2.get_asset(empty_data_asset_name)

    my_batch_config = asset_1.add_batch_config("my batch config")
    your_batch_config = asset_2.add_batch_config("your batch config")

    loaded_datasource = context_with_asset.get_datasource(datasource_name)
    assert isinstance(loaded_datasource, Datasource)
    assert loaded_datasource.get_asset(empty_data_asset_name).batch_configs == [
        my_batch_config,
        your_batch_config,
    ]


@pytest.mark.unit
def test_delete_batch_config__success(
    data_asset_with_batch_config: DataAsset,
    persisted_batch_config: BatchConfig,
):
    assert persisted_batch_config in data_asset_with_batch_config.batch_configs

    data_asset_with_batch_config.delete_batch_config(persisted_batch_config)

    assert data_asset_with_batch_config.batch_configs == []


@pytest.mark.unit
def test_delete_batch_config__persists(
    context_with_asset: AbstractDataContext,
    datasource_name: str,
    empty_data_asset_name: str,
    data_asset_with_batch_config: DataAsset,
    persisted_batch_config: BatchConfig,
):
    data_asset_with_batch_config.delete_batch_config(persisted_batch_config)

    loaded_datasource = context_with_asset.get_datasource(datasource_name)
    assert isinstance(loaded_datasource, Datasource)
    loaded_asset = loaded_datasource.get_asset(empty_data_asset_name)

    assert loaded_asset.batch_configs == []


@pytest.mark.unit
def test_delete_batch_config__unsaved_batch_config(empty_data_asset: DataAsset):
    batch_config = BatchConfig(name="uh oh")

    with pytest.raises(ValueError, match="does not exist"):
        empty_data_asset.delete_batch_config(batch_config)


@pytest.mark.unit
def test_fields_set(empty_data_asset: DataAsset):
    """We mess with pydantic's internal __fields_set__ to determine
    if certain fields (batch_configs in this case) should get serialized.

    This test is essentially a proxy for whether we serialize that field
    """
    asset = empty_data_asset

    # when we don't have batch configs, it shouldn't be in the set
    assert "batch_configs" not in asset.__fields_set__

    # add some batch configs and ensure we have it in the set
    batch_config_a = asset.add_batch_config("a")
    batch_config_b = asset.add_batch_config("b")
    assert "batch_configs" in asset.__fields_set__

    # delete one of the batch configs and ensure we still have it in the set
    asset.delete_batch_config(batch_config_a)
    assert "batch_configs" in asset.__fields_set__

    # delete the remaining batch config and ensure we don't have it in the set
    asset.delete_batch_config(batch_config_b)
    assert "batch_configs" not in asset.__fields_set__
