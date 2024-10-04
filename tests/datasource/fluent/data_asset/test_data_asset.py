from typing import List

import pytest

from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.partitioners import ColumnPartitionerYearly
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.datasource.fluent.fluent_base_model import FluentBaseModel
from great_expectations.datasource.fluent.interfaces import Batch, DataAsset, Datasource
from great_expectations.datasource.fluent.pandas_datasource import PandasDatasource

DATASOURCE_NAME = "my datasource for batch configs"
EMPTY_DATA_ASSET_NAME = "my data asset for batch configs"
DATA_ASSET_WITH_BATCH_DEFINITION_NAME = "I have batch configs"
ANOTHER_DATA_ASSET_WITH_BATCH_DEFINITION_NAME = "I have batch configs too"
BATCH_DEFINITION_NAME = "my batch config"


@pytest.fixture
def file_context(empty_data_context: AbstractDataContext) -> AbstractDataContext:
    return empty_data_context


@pytest.fixture
def file_context_with_assets(file_context: AbstractDataContext) -> AbstractDataContext:
    """Context with a datasource that has 2 assets. one of the assets has a batch config."""
    datasource = file_context.data_sources.add_pandas(DATASOURCE_NAME)
    datasource.add_csv_asset(EMPTY_DATA_ASSET_NAME, "taxi.csv")  # type: ignore [arg-type]
    datasource.add_csv_asset(
        DATA_ASSET_WITH_BATCH_DEFINITION_NAME,
        "taxi.csv",  # type: ignore [arg-type]
    ).add_batch_definition(BATCH_DEFINITION_NAME)
    datasource.add_csv_asset(
        ANOTHER_DATA_ASSET_WITH_BATCH_DEFINITION_NAME,
        "taxi.csv",  # type: ignore [arg-type]
    ).add_batch_definition(BATCH_DEFINITION_NAME)

    return file_context


@pytest.fixture
def cloud_context(empty_cloud_context_fluent: CloudDataContext) -> AbstractDataContext:
    datasource = empty_cloud_context_fluent.data_sources.add_pandas(DATASOURCE_NAME)
    datasource.add_csv_asset(EMPTY_DATA_ASSET_NAME, "taxi.csv")  # type: ignore [arg-type]
    datasource.add_csv_asset(
        DATA_ASSET_WITH_BATCH_DEFINITION_NAME,
        "taxi.csv",  # type: ignore [arg-type]
    ).add_batch_definition(BATCH_DEFINITION_NAME)
    datasource.add_csv_asset(
        ANOTHER_DATA_ASSET_WITH_BATCH_DEFINITION_NAME,
        "taxi.csv",  # type: ignore [arg-type]
    ).add_batch_definition(BATCH_DEFINITION_NAME)
    return empty_cloud_context_fluent


@pytest.fixture
def datasource(file_context_with_assets: AbstractDataContext) -> PandasDatasource:
    output = file_context_with_assets.data_sources.get(DATASOURCE_NAME)
    assert isinstance(output, PandasDatasource)
    return output


@pytest.fixture
def empty_data_asset(datasource: Datasource) -> DataAsset:
    return datasource.get_asset(EMPTY_DATA_ASSET_NAME)


@pytest.fixture
def data_asset_with_batch_definition(datasource: Datasource) -> DataAsset:
    return datasource.get_asset(DATA_ASSET_WITH_BATCH_DEFINITION_NAME)


@pytest.fixture
def persisted_batch_definition(data_asset_with_batch_definition: DataAsset) -> BatchDefinition:
    return data_asset_with_batch_definition.batch_definitions[0]


@pytest.mark.unit
def test_add_batch_definition__success(empty_data_asset: DataAsset):
    name = "my batch config"
    batch_definition = empty_data_asset.add_batch_definition(name)

    assert batch_definition.name == name
    assert batch_definition.data_asset == empty_data_asset
    assert empty_data_asset.batch_definitions == [batch_definition]


@pytest.mark.unit
def test_add_batch_definition_with_partitioner__success(empty_data_asset: DataAsset):
    name = "my batch config"
    partitioner = ColumnPartitionerYearly(column_name="test-column")
    batch_definition = empty_data_asset.add_batch_definition(name, partitioner=partitioner)

    assert batch_definition.partitioner == partitioner


@pytest.mark.unit
def test_add_batch_definition__persists(
    file_context: AbstractDataContext, empty_data_asset: DataAsset
):
    name = "my batch config"
    partitioner = ColumnPartitionerYearly(column_name="test-column")
    batch_definition = empty_data_asset.add_batch_definition(name, partitioner=partitioner)

    loaded_datasource = file_context.data_sources.get(DATASOURCE_NAME)
    assert isinstance(loaded_datasource, Datasource)
    loaded_asset = loaded_datasource.get_asset(EMPTY_DATA_ASSET_NAME)

    assert loaded_asset.batch_definitions == [batch_definition]


@pytest.mark.unit
def test_add_batch_definition_with_partitioner__persists(
    file_context: AbstractDataContext, empty_data_asset: DataAsset
):
    name = "my batch config"
    partitioner = ColumnPartitionerYearly(column_name="test-column")
    empty_data_asset.add_batch_definition(name, partitioner=partitioner)

    loaded_datasource = file_context.data_sources.get(DATASOURCE_NAME)
    assert isinstance(loaded_datasource, Datasource)
    loaded_asset = loaded_datasource.get_asset(EMPTY_DATA_ASSET_NAME)

    loaded_batch_definition = loaded_asset.batch_definitions[0]
    assert loaded_batch_definition.partitioner == partitioner


@pytest.mark.unit
def test_add_batch_definition__multiple(empty_data_asset: DataAsset):
    empty_data_asset.add_batch_definition("foo")
    empty_data_asset.add_batch_definition("bar")

    assert len(empty_data_asset.batch_definitions) == 2


@pytest.mark.unit
def test_add_batch_definition__duplicate_key(empty_data_asset: DataAsset):
    name = "my batch config"
    empty_data_asset.add_batch_definition(name)

    with pytest.raises(ValueError, match="already exists"):
        empty_data_asset.add_batch_definition(name)


@pytest.mark.unit
def test_add_batch_definition__file_data__does_not_clobber_other_assets(
    file_context_with_assets: AbstractDataContext,
):
    _test_add_batch_definition__does_not_clobber_other_assets(
        context=file_context_with_assets,
        datasource_name=DATASOURCE_NAME,
    )


@pytest.mark.unit
def test_add_batch_definition__cloud_data__does_not_clobber_other_assets(
    cloud_context: AbstractDataContext,
):
    _test_add_batch_definition__does_not_clobber_other_assets(
        context=cloud_context,
        datasource_name=DATASOURCE_NAME,
    )


def _test_add_batch_definition__does_not_clobber_other_assets(
    context: AbstractDataContext,
    datasource_name: str,
):
    ds1 = context.data_sources.get(datasource_name)
    assert isinstance(ds1, PandasDatasource)
    my_asset = ds1.add_csv_asset("my asset", "taxi.csv")  # type: ignore [arg-type]

    # adding an asset currently clobbers the datasource, so for now we
    # need to reload the datasource AFTER adding the asset
    # TODO: Move fetching ds2 up with ds1 after V1-124
    ds2 = context.data_sources.get(datasource_name)
    assert isinstance(ds2, PandasDatasource)
    your_asset = ds2.add_csv_asset("your asset", "taxi.csv")  # type: ignore [arg-type]

    my_batch_definition = my_asset.add_batch_definition("my batch config")
    your_batch_definition = your_asset.add_batch_definition("your batch config")

    loaded_datasource = context.data_sources.get(datasource_name)
    assert isinstance(loaded_datasource, Datasource)
    assert loaded_datasource.get_asset(my_asset.name).batch_definitions == [my_batch_definition]
    assert loaded_datasource.get_asset(your_asset.name).batch_definitions == [your_batch_definition]


@pytest.mark.unit
def test_add_batch_definition__file_data__does_not_clobber_other_batch_definitions(
    file_context_with_assets: AbstractDataContext,
):
    _test_add_batch_definition__does_not_clobber_other_batch_definitions(
        context=file_context_with_assets,
        datasource_name=DATASOURCE_NAME,
        asset_name=EMPTY_DATA_ASSET_NAME,
    )


@pytest.mark.unit
def test_add_batch_definition__cloud_data__does_not_clobber_other_batch_definitions(
    cloud_context: AbstractDataContext,
):
    _test_add_batch_definition__does_not_clobber_other_batch_definitions(
        context=cloud_context,
        datasource_name=DATASOURCE_NAME,
        asset_name=EMPTY_DATA_ASSET_NAME,
    )


def _test_add_batch_definition__does_not_clobber_other_batch_definitions(
    context: AbstractDataContext,
    datasource_name: str,
    asset_name: str,
):
    ds1 = context.data_sources.get(datasource_name)
    ds2 = context.data_sources.get(datasource_name)
    assert isinstance(ds1, PandasDatasource)
    assert isinstance(ds2, PandasDatasource)

    asset_1 = ds1.get_asset(asset_name)
    asset_2 = ds2.get_asset(asset_name)

    my_batch_definition = asset_1.add_batch_definition("my batch config")
    your_batch_definition = asset_2.add_batch_definition("your batch config")

    loaded_datasource = context.data_sources.get(datasource_name)
    assert isinstance(loaded_datasource, Datasource)
    assert loaded_datasource.get_asset(asset_name).batch_definitions == [
        my_batch_definition,
        your_batch_definition,
    ]


@pytest.mark.unit
def test_delete_batch_definition__success(
    data_asset_with_batch_definition: DataAsset,
    persisted_batch_definition: BatchDefinition,
):
    assert persisted_batch_definition in data_asset_with_batch_definition.batch_definitions

    data_asset_with_batch_definition.delete_batch_definition(persisted_batch_definition.name)

    assert data_asset_with_batch_definition.batch_definitions == []


@pytest.mark.unit
def test_delete_batch_definition__persists(
    file_context_with_assets: AbstractDataContext,
    data_asset_with_batch_definition: DataAsset,
    persisted_batch_definition: BatchDefinition,
):
    data_asset_with_batch_definition.delete_batch_definition(persisted_batch_definition.name)

    loaded_datasource = file_context_with_assets.data_sources.get(DATASOURCE_NAME)
    assert isinstance(loaded_datasource, Datasource)
    loaded_asset = loaded_datasource.get_asset(EMPTY_DATA_ASSET_NAME)

    assert loaded_asset.batch_definitions == []


@pytest.mark.unit
def test_delete_batch_definition__unsaved_batch_definition(empty_data_asset: DataAsset):
    batch_definition = BatchDefinition[None](name="uh oh")

    with pytest.raises(ValueError, match="does not exist"):
        empty_data_asset.delete_batch_definition(batch_definition.name)


@pytest.mark.unit
def test_fields_set(empty_data_asset: DataAsset):
    """We mess with pydantic's internal __fields_set__ to determine
    if certain fields (batch_definitions in this case) should get serialized.

    This test is essentially a proxy for whether we serialize that field
    """
    asset = empty_data_asset

    # when we don't have batch configs, it shouldn't be in the set
    assert "batch_definitions" not in asset.__fields_set__

    # add some batch configs and ensure we have it in the set
    batch_definition_a = asset.add_batch_definition("a")
    batch_definition_b = asset.add_batch_definition("b")
    assert "batch_definitions" in asset.__fields_set__

    # delete one of the batch configs and ensure we still have it in the set
    asset.delete_batch_definition(batch_definition_a.name)
    assert "batch_definitions" in asset.__fields_set__

    # delete the remaining batch config and ensure we don't have it in the set
    asset.delete_batch_definition(batch_definition_b.name)
    assert "batch_definitions" not in asset.__fields_set__


@pytest.mark.unit
def test_delete_batch_definition__file_data__does_not_clobber_other_assets(
    file_context_with_assets: AbstractDataContext,
):
    _test_delete_batch_definition__does_not_clobber_other_assets(
        context=file_context_with_assets,
        datasource_name=DATASOURCE_NAME,
        asset_names=[
            DATA_ASSET_WITH_BATCH_DEFINITION_NAME,
            ANOTHER_DATA_ASSET_WITH_BATCH_DEFINITION_NAME,
        ],
    )


@pytest.mark.unit
def test_delete_batch_definition__cloud_data__does_not_clobber_other_assets(
    cloud_context: AbstractDataContext,
):
    _test_delete_batch_definition__does_not_clobber_other_assets(
        context=cloud_context,
        datasource_name=DATASOURCE_NAME,
        asset_names=[
            DATA_ASSET_WITH_BATCH_DEFINITION_NAME,
            ANOTHER_DATA_ASSET_WITH_BATCH_DEFINITION_NAME,
        ],
    )


def _test_delete_batch_definition__does_not_clobber_other_assets(
    context: AbstractDataContext, datasource_name: str, asset_names: List[str]
):
    # each asset has one batch config; delete it
    for asset_name in asset_names:
        datasource = context.data_sources.get(datasource_name)
        assert isinstance(datasource, Datasource)
        asset = datasource.get_asset(asset_name)
        asset.delete_batch_definition(asset.batch_definitions[0].name)

    loaded_datasource = context.data_sources.get(datasource_name)
    assert isinstance(loaded_datasource, Datasource)

    # ensure neither call to delete_batch_definition() didn't clobber each other
    for asset_name in asset_names:
        assert loaded_datasource.get_asset(asset_name).batch_definitions == []


class _MyPartitioner(FluentBaseModel):
    """Partitioner that adhere's to the expected protocol."""

    sort_ascending: bool = True

    @property
    def param_names(self) -> List[str]:
        return ["a", "b"]


@pytest.fixture
def metadata_1_1(mocker):
    return mocker.MagicMock(spec=Batch, metadata={"a": 1, "b": 1})


@pytest.fixture
def metadata_1_2(mocker):
    return mocker.MagicMock(spec=Batch, metadata={"a": 1, "b": 2})


@pytest.fixture
def metadata_2_1(mocker):
    return mocker.MagicMock(spec=Batch, metadata={"a": 2, "b": 1})


@pytest.fixture
def metadata_2_2(mocker):
    return mocker.MagicMock(spec=Batch, metadata={"a": 2, "b": 2})


@pytest.fixture
def metadata_2_none(mocker):
    return mocker.MagicMock(spec=Batch, metadata={"a": 2, "b": None})


@pytest.fixture
def metadata_none_2(mocker):
    return mocker.MagicMock(spec=Batch, metadata={"a": None, "b": 2})


@pytest.fixture
def metadata_none_none(mocker):
    return mocker.MagicMock(spec=Batch, metadata={"a": None, "b": None})


@pytest.mark.unit
def test_sort_batches__ascending(
    empty_data_asset,
    metadata_1_1,
    metadata_1_2,
    metadata_2_1,
    metadata_2_2,
    metadata_none_2,
    metadata_2_none,
    metadata_none_none,
):
    partitioner = _MyPartitioner(sort_ascending=True)
    batches = [
        metadata_1_1,
        metadata_1_2,
        metadata_2_1,
        metadata_2_2,
        metadata_2_none,
        metadata_none_2,
        metadata_none_none,
    ]

    batches = empty_data_asset.sort_batches(batches, partitioner)

    assert batches == [
        metadata_none_none,
        metadata_none_2,
        metadata_1_1,
        metadata_1_2,
        metadata_2_none,
        metadata_2_1,
        metadata_2_2,
    ]


@pytest.mark.unit
def test_sort_batches__descending(
    empty_data_asset,
    metadata_1_1,
    metadata_1_2,
    metadata_2_1,
    metadata_2_2,
    metadata_none_2,
    metadata_2_none,
    metadata_none_none,
):
    partitioner = _MyPartitioner(sort_ascending=False)
    batches = [
        metadata_1_1,
        metadata_1_2,
        metadata_2_1,
        metadata_2_2,
        metadata_2_none,
        metadata_none_2,
        metadata_none_none,
    ]

    batches = empty_data_asset.sort_batches(batches, partitioner)

    assert batches == [
        metadata_2_2,
        metadata_2_1,
        metadata_2_none,
        metadata_1_2,
        metadata_1_1,
        metadata_none_2,
        metadata_none_none,
    ]


@pytest.mark.unit
def test_sort_batches__requires_keys(empty_data_asset, mocker):
    partitioner = _MyPartitioner()

    wheres_my_b = mocker.MagicMock(spec=Batch, metadata={"a": 1})
    i_have_a_b = mocker.MagicMock(spec=Batch, metadata={"a": 1, "b": 2})

    expected_error = "Trying to sort my data asset for batch configs's batches on key b"

    with pytest.raises(KeyError, match=expected_error):
        empty_data_asset.sort_batches([wheres_my_b, i_have_a_b], partitioner)
