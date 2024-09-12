from __future__ import annotations

import re
import uuid
from typing import TYPE_CHECKING, Optional

import pytest

from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.partitioners import FileNamePartitionerYearly
from great_expectations.datasource.fluent.batch_request import BatchParameters
from great_expectations.datasource.fluent.interfaces import Batch, DataAsset
from great_expectations.exceptions import (
    BatchDefinitionNotAddedError,
    BatchDefinitionNotFreshError,
    ResourceFreshnessAggregateError,
)
from great_expectations.exceptions.exceptions import (
    BatchDefinitionNotFoundError,
    DataAssetNotFoundError,
    DatasourceNotFoundError,
)

if TYPE_CHECKING:
    from typing import List

    import pytest_mock

    from great_expectations.datasource.fluent.batch_request import BatchRequest


class DataAssetForTests(DataAsset):
    @override
    def get_batch_identifiers_list(self, batch_request: BatchRequest) -> List[dict]:
        raise NotImplementedError

    @override
    def get_batch(self, batch_request: BatchRequest) -> Batch:
        raise NotImplementedError


@pytest.fixture
def mock_data_asset(monkeypatch, mocker: pytest_mock.MockerFixture) -> DataAsset:
    monkeypatch.setattr(DataAsset, "build_batch_request", mocker.Mock())
    data_asset: DataAsset = DataAssetForTests(name="my_data_asset", type="table")

    return data_asset


@pytest.mark.parametrize(
    "batch_parameters",
    [
        (None,),
        ({"foo": "bar"},),
    ],
)
@pytest.mark.unit
def test_build_batch_request(
    batch_parameters: Optional[BatchParameters],
    mock_data_asset: DataAsset,
    mocker: pytest_mock.MockerFixture,
):
    batching_regex = re.compile(r"data_(?P<year>\d{4})-(?P<month>\d{2}).csv")
    partitioner = FileNamePartitionerYearly(regex=batching_regex)
    batch_definition = BatchDefinition(
        name="test_batch_definition",
        partitioner=partitioner,
    )
    batch_definition.set_data_asset(mock_data_asset)

    batch_definition.build_batch_request(batch_parameters=batch_parameters)

    mock_build_batch_request = batch_definition.data_asset.build_batch_request
    assert isinstance(mock_build_batch_request, mocker.Mock)
    mock_build_batch_request.assert_called_once_with(
        options=batch_parameters,
        partitioner=partitioner,
    )


@pytest.mark.unit
def test_get_batch_retrieves_only_batch(mocker: pytest_mock.MockFixture):
    # Arrange
    batch_definition = BatchDefinition[None](name="test_batch_definition")
    mock_asset = mocker.Mock(spec=DataAsset)
    batch_definition.set_data_asset(mock_asset)

    mock_batch = mocker.Mock(spec=Batch)
    mock_asset.get_batch.return_value = mock_batch

    # Act
    batch = batch_definition.get_batch()

    # Assert
    assert batch == mock_batch
    mock_asset.get_batch.assert_called_once_with(batch_definition.build_batch_request())


@pytest.mark.unit
def test_get_batch_identifiers_list(mocker: pytest_mock.MockFixture):
    # Arrange
    batch_definition = BatchDefinition[None](name="test_batch_definition")
    mock_asset = mocker.Mock(spec=DataAsset)
    batch_definition.set_data_asset(mock_asset)

    mock_batch_identifiers_list = [{"foo": "bar"}, {"baz": "qux"}]
    mock_asset.get_batch_identifiers_list.return_value = mock_batch_identifiers_list

    # Act
    batch_identifiers_list = batch_definition.get_batch_identifiers_list()

    # Assert
    assert batch_identifiers_list == mock_batch_identifiers_list
    mock_asset.get_batch_identifiers_list.assert_called_once_with(
        batch_definition.build_batch_request()
    )


@pytest.mark.unit
def test_get_batch_identifiers_list_with_batch_parameters(mocker: pytest_mock.MockFixture):
    # Arrange
    batch_definition = BatchDefinition[None](name="test_batch_definition")
    mock_asset = mocker.Mock(spec=DataAsset)
    batch_definition.set_data_asset(mock_asset)

    mock_batch_identifiers_list = [{"foo": "bar"}, {"baz": "qux"}]
    mock_asset.get_batch_identifiers_list.return_value = mock_batch_identifiers_list

    # Act
    batch_parameters: BatchParameters = {"path": "my_path"}
    batch_identifiers_list = batch_definition.get_batch_identifiers_list(batch_parameters)

    # Assert
    assert batch_identifiers_list == mock_batch_identifiers_list
    mock_asset.get_batch_identifiers_list.assert_called_once_with(
        batch_definition.build_batch_request(batch_parameters)
    )


@pytest.mark.unit
def test_identifier_bundle_success(in_memory_runtime_context):
    context = in_memory_runtime_context
    ds = context.data_sources.add_pandas("pandas_datasource")
    asset = ds.add_csv_asset("my_asset", "data.csv")
    batch_definition = asset.add_batch_definition("my_batch_definition")

    result = batch_definition.identifier_bundle()
    assert result.datasource.name == "pandas_datasource" and result.datasource.id is not None
    assert result.asset.name == "my_asset" and result.asset.id is not None
    assert (
        result.batch_definition.name == "my_batch_definition"
        and result.batch_definition.id is not None
    )


@pytest.mark.unit
def test_identifier_bundle_no_id_raises_error(in_memory_runtime_context):
    context = in_memory_runtime_context
    ds = context.data_sources.add_pandas("pandas_datasource")
    asset = ds.add_csv_asset("my_asset", "data.csv")
    batch_definition = asset.add_batch_definition("my_batch_definition")

    batch_definition.id = None

    with pytest.raises(ResourceFreshnessAggregateError) as e:
        batch_definition.identifier_bundle()

    assert len(e.value.errors) == 1
    assert isinstance(e.value.errors[0], BatchDefinitionNotAddedError)


@pytest.mark.parametrize(
    "id,is_fresh,num_errors",
    [
        pytest.param(str(uuid.uuid4()), True, 0, id="added"),
        pytest.param(None, False, 1, id="not_added"),
    ],
)
@pytest.mark.unit
def test_is_fresh_is_added(
    in_memory_runtime_context, id: str | None, is_fresh: bool, num_errors: int
):
    context = in_memory_runtime_context
    batch_definition = (
        context.data_sources.add_pandas(name="my_pandas_ds")
        .add_csv_asset(name="my_csv_asset", filepath_or_buffer="data.csv")
        .add_batch_definition(name="my_batch_def")
    )
    batch_definition.id = id  # Fluent API will add an ID but manually overriding for test
    diagnostics = batch_definition.is_fresh()

    assert diagnostics.success is is_fresh
    assert len(diagnostics.errors) == num_errors
    assert all(isinstance(err, BatchDefinitionNotAddedError) for err in diagnostics.errors)


@pytest.mark.cloud
def test_is_fresh_freshness(empty_cloud_context_fluent):
    # Ephemeral/file use a cacheable datasource dict so freshness
    # with batch definitions is a Cloud-only concern
    context = empty_cloud_context_fluent
    batch_definition = (
        context.data_sources.add_pandas(name="my_pandas_ds")
        .add_csv_asset(name="my_csv_asset", filepath_or_buffer="data.csv")
        .add_batch_definition(name="my_batch_def")
    )

    batching_regex = re.compile(r"data_(?P<year>\d{4})-(?P<month>\d{2}).csv")
    partitioner = FileNamePartitionerYearly(regex=batching_regex)
    batch_definition.partitioner = partitioner

    diagnostics = batch_definition.is_fresh()
    assert diagnostics.success is False
    assert len(diagnostics.errors) == 1
    assert isinstance(diagnostics.errors[0], BatchDefinitionNotFreshError)


@pytest.mark.unit
def test_is_fresh_fails_on_datasource_retrieval(in_memory_runtime_context):
    context = in_memory_runtime_context
    datasource = context.data_sources.add_pandas(name="my_pandas_ds")
    asset = datasource.add_csv_asset(name="my_csv_asset", filepath_or_buffer="data.csv")
    batch_definition = asset.add_batch_definition(name="my_batch_def")

    context.delete_datasource("my_pandas_ds")

    diagnostics = batch_definition.is_fresh()
    assert diagnostics.success is False
    assert len(diagnostics.errors) == 1
    assert isinstance(diagnostics.errors[0], DatasourceNotFoundError)


@pytest.mark.unit
def test_is_fresh_fails_on_asset_retrieval(in_memory_runtime_context):
    context = in_memory_runtime_context
    datasource = context.data_sources.add_pandas(name="my_pandas_ds")
    asset = datasource.add_csv_asset(name="my_csv_asset", filepath_or_buffer="data.csv")
    batch_definition = asset.add_batch_definition(name="my_batch_def")

    datasource.delete_asset("my_csv_asset")

    diagnostics = batch_definition.is_fresh()
    assert diagnostics.success is False
    assert len(diagnostics.errors) == 1
    assert isinstance(diagnostics.errors[0], DataAssetNotFoundError)


@pytest.mark.unit
def test_is_fresh_fails_on_batch_definition_retrieval(in_memory_runtime_context):
    context = in_memory_runtime_context
    datasource = context.data_sources.add_pandas(name="my_pandas_ds")
    asset = datasource.add_csv_asset(name="my_csv_asset", filepath_or_buffer="data.csv")
    batch_definition = asset.add_batch_definition(name="my_batch_def")

    asset.delete_batch_definition("my_batch_def")

    diagnostics = batch_definition.is_fresh()
    assert diagnostics.success is False
    assert len(diagnostics.errors) == 1
    assert isinstance(diagnostics.errors[0], BatchDefinitionNotFoundError)
