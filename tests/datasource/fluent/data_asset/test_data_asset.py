from typing import List

import pytest

from great_expectations.core.batch_config import BatchConfig
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.datasource.fluent.interfaces import DataAsset, Datasource
from great_expectations.datasource.fluent.pandas_datasource import PandasDatasource
from great_expectations.datasource.fluent.sql_datasource import PartitionerYear

DATASOURCE_NAME = "my datasource for batch configs"
EMPTY_DATA_ASSET_NAME = "my data asset for batch configs"
DATA_ASSET_WITH_BATCH_CONFIG_NAME = "I have batch configs"
ANOTHER_DATA_ASSET_WITH_BATCH_CONFIG_NAME = "I have batch configs too"
BATCH_CONFIG_NAME = "my batch config"


@pytest.fixture
def file_context(empty_data_context: AbstractDataContext) -> AbstractDataContext:
    return empty_data_context


@pytest.fixture
def file_context_with_assets(file_context: AbstractDataContext) -> AbstractDataContext:
    """Context with a datasource that has 2 assets. one of the assets has a batch config."""
    datasource = file_context.sources.add_pandas(DATASOURCE_NAME)
    datasource.add_csv_asset(EMPTY_DATA_ASSET_NAME, "taxi.csv")  # type: ignore [arg-type]
    datasource.add_csv_asset(
        DATA_ASSET_WITH_BATCH_CONFIG_NAME,
        "taxi.csv",  # type: ignore [arg-type]
    ).add_batch_config(BATCH_CONFIG_NAME)
    datasource.add_csv_asset(
        ANOTHER_DATA_ASSET_WITH_BATCH_CONFIG_NAME,
        "taxi.csv",  # type: ignore [arg-type]
    ).add_batch_config(BATCH_CONFIG_NAME)

    return file_context


@pytest.fixture
def cloud_context(empty_cloud_context_fluent: CloudDataContext) -> AbstractDataContext:
    datasource = empty_cloud_context_fluent.sources.add_pandas(DATASOURCE_NAME)
    datasource.add_csv_asset(EMPTY_DATA_ASSET_NAME, "taxi.csv")  # type: ignore [arg-type]
    datasource.add_csv_asset(
        DATA_ASSET_WITH_BATCH_CONFIG_NAME,
        "taxi.csv",  # type: ignore [arg-type]
    ).add_batch_config(BATCH_CONFIG_NAME)
    datasource.add_csv_asset(
        ANOTHER_DATA_ASSET_WITH_BATCH_CONFIG_NAME,
        "taxi.csv",  # type: ignore [arg-type]
    ).add_batch_config(BATCH_CONFIG_NAME)
    return empty_cloud_context_fluent


@pytest.fixture
def datasource(file_context_with_assets: AbstractDataContext) -> PandasDatasource:
    output = file_context_with_assets.get_datasource(DATASOURCE_NAME)
    assert isinstance(output, PandasDatasource)
    return output


@pytest.fixture
def empty_data_asset(datasource: Datasource) -> DataAsset:
    return datasource.get_asset(EMPTY_DATA_ASSET_NAME)


@pytest.fixture
def data_asset_with_batch_config(datasource: Datasource) -> DataAsset:
    return datasource.get_asset(DATA_ASSET_WITH_BATCH_CONFIG_NAME)


@pytest.fixture
def persisted_batch_config(data_asset_with_batch_config: DataAsset) -> BatchConfig:
    return data_asset_with_batch_config.batch_configs[0]


@pytest.mark.unit
def test_add_batch_config__success(empty_data_asset: DataAsset):
    name = "my batch config"
    batch_config = empty_data_asset.add_batch_config(name)

    assert batch_config.name == name
    assert batch_config.data_asset == empty_data_asset
    assert empty_data_asset.batch_configs == [batch_config]


@pytest.mark.unit
def test_add_batch_config_with_partitioner__success(empty_data_asset: DataAsset):
    name = "my batch config"
    partitioner = PartitionerYear(column_name="test-column")
    batch_config = empty_data_asset.add_batch_config(name, partitioner=partitioner)

    assert batch_config.partitioner == partitioner


@pytest.mark.unit
def test_add_batch_config__persists(
    file_context: AbstractDataContext, empty_data_asset: DataAsset
):
    name = "my batch config"
    partitioner = PartitionerYear(column_name="test-column")
    batch_config = empty_data_asset.add_batch_config(name, partitioner=partitioner)

    loaded_datasource = file_context.get_datasource(DATASOURCE_NAME)
    assert isinstance(loaded_datasource, Datasource)
    loaded_asset = loaded_datasource.get_asset(EMPTY_DATA_ASSET_NAME)

    assert loaded_asset.batch_configs == [batch_config]


@pytest.mark.unit
def test_add_batch_config_with_partitioner__persists(
    file_context: AbstractDataContext, empty_data_asset: DataAsset
):
    name = "my batch config"
    partitioner = PartitionerYear(column_name="test-column")
    empty_data_asset.add_batch_config(name, partitioner=partitioner)

    loaded_datasource = file_context.get_datasource(DATASOURCE_NAME)
    assert isinstance(loaded_datasource, Datasource)
    loaded_asset = loaded_datasource.get_asset(EMPTY_DATA_ASSET_NAME)

    loaded_batch_config = loaded_asset.batch_configs[0]
    assert loaded_batch_config.partitioner == partitioner


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
    file_context_with_assets: AbstractDataContext,
):
    _test_add_batch_config__does_not_clobber_other_assets(
        context=file_context_with_assets,
        datasource_name=DATASOURCE_NAME,
    )


@pytest.mark.unit
def test_add_batch_config__cloud_data__does_not_clobber_other_assets(
    cloud_context: AbstractDataContext,
):
    _test_add_batch_config__does_not_clobber_other_assets(
        context=cloud_context,
        datasource_name=DATASOURCE_NAME,
    )


def _test_add_batch_config__does_not_clobber_other_assets(
    context: AbstractDataContext,
    datasource_name: str,
):
    ds1 = context.get_datasource(datasource_name)
    assert isinstance(ds1, PandasDatasource)
    my_asset = ds1.add_csv_asset("my asset", "taxi.csv")  # type: ignore [arg-type]

    # adding an asset currently clobbers the datasource, so for now we
    # need to reload the datasource AFTER adding the asset
    # TODO: Move fetching ds2 up with ds1 after V1-124
    ds2 = context.get_datasource(datasource_name)
    assert isinstance(ds2, PandasDatasource)
    your_asset = ds2.add_csv_asset("your asset", "taxi.csv")  # type: ignore [arg-type]

    my_batch_config = my_asset.add_batch_config("my batch config")
    your_batch_config = your_asset.add_batch_config("your batch config")

    loaded_datasource = context.get_datasource(datasource_name)
    assert isinstance(loaded_datasource, Datasource)
    assert loaded_datasource.get_asset(my_asset.name).batch_configs == [my_batch_config]
    assert loaded_datasource.get_asset(your_asset.name).batch_configs == [
        your_batch_config
    ]


@pytest.mark.unit
def test_add_batch_config__file_data__does_not_clobber_other_batch_configs(
    file_context_with_assets: AbstractDataContext,
):
    _test_add_batch_config__does_not_clobber_other_batch_configs(
        context=file_context_with_assets,
        datasource_name=DATASOURCE_NAME,
        asset_name=EMPTY_DATA_ASSET_NAME,
    )


@pytest.mark.unit
def test_add_batch_config__cloud_data__does_not_clobber_other_batch_configs(
    cloud_context: AbstractDataContext,
):
    _test_add_batch_config__does_not_clobber_other_batch_configs(
        context=cloud_context,
        datasource_name=DATASOURCE_NAME,
        asset_name=EMPTY_DATA_ASSET_NAME,
    )


def _test_add_batch_config__does_not_clobber_other_batch_configs(
    context: AbstractDataContext,
    datasource_name: str,
    asset_name: str,
):
    ds1 = context.get_datasource(datasource_name)
    ds2 = context.get_datasource(datasource_name)
    assert isinstance(ds1, PandasDatasource)
    assert isinstance(ds2, PandasDatasource)

    asset_1 = ds1.get_asset(asset_name)
    asset_2 = ds2.get_asset(asset_name)

    my_batch_config = asset_1.add_batch_config("my batch config")
    your_batch_config = asset_2.add_batch_config("your batch config")

    loaded_datasource = context.get_datasource(datasource_name)
    assert isinstance(loaded_datasource, Datasource)
    assert loaded_datasource.get_asset(asset_name).batch_configs == [
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
    file_context_with_assets: AbstractDataContext,
    data_asset_with_batch_config: DataAsset,
    persisted_batch_config: BatchConfig,
):
    data_asset_with_batch_config.delete_batch_config(persisted_batch_config)

    loaded_datasource = file_context_with_assets.get_datasource(DATASOURCE_NAME)
    assert isinstance(loaded_datasource, Datasource)
    loaded_asset = loaded_datasource.get_asset(EMPTY_DATA_ASSET_NAME)

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


@pytest.mark.unit
def test_delete_batch_config__file_data__does_not_clobber_other_assets(
    file_context_with_assets: AbstractDataContext,
):
    _test_delete_batch_config__does_not_clobber_other_assets(
        context=file_context_with_assets,
        datasource_name=DATASOURCE_NAME,
        asset_names=[
            DATA_ASSET_WITH_BATCH_CONFIG_NAME,
            ANOTHER_DATA_ASSET_WITH_BATCH_CONFIG_NAME,
        ],
    )


@pytest.mark.unit
def test_delete_batch_config__cloud_data__does_not_clobber_other_assets(
    cloud_context: AbstractDataContext,
):
    _test_delete_batch_config__does_not_clobber_other_assets(
        context=cloud_context,
        datasource_name=DATASOURCE_NAME,
        asset_names=[
            DATA_ASSET_WITH_BATCH_CONFIG_NAME,
            ANOTHER_DATA_ASSET_WITH_BATCH_CONFIG_NAME,
        ],
    )


def _test_delete_batch_config__does_not_clobber_other_assets(
    context: AbstractDataContext, datasource_name: str, asset_names: List[str]
):
    # each asset has one batch config; delete it
    for asset_name in asset_names:
        datasource = context.get_datasource(datasource_name)
        assert isinstance(datasource, Datasource)
        asset = datasource.get_asset(asset_name)
        asset.delete_batch_config(asset.batch_configs[0])

    loaded_datasource = context.get_datasource(datasource_name)
    assert isinstance(loaded_datasource, Datasource)

    # ensure neither call to delete_batch_config() didn't clobber each other
    for asset_name in asset_names:
        assert loaded_datasource.get_asset(asset_name).batch_configs == []
