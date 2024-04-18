from __future__ import annotations

import re
from typing import TYPE_CHECKING, Optional
from unittest.mock import Mock  # noqa: TID251

import pytest

from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.partitioners import PartitionerYear
from great_expectations.core.serdes import _EncodedValidationData, _IdentifierBundle
from great_expectations.datasource.fluent.batch_request import BatchParameters
from great_expectations.datasource.fluent.interfaces import Batch, DataAsset
from great_expectations.datasource.fluent.pandas_datasource import PandasDatasource

if TYPE_CHECKING:
    import pytest_mock


@pytest.fixture
def mock_data_asset(monkeypatch) -> DataAsset:
    monkeypatch.setattr(DataAsset, "build_batch_request", Mock())
    data_asset: DataAsset = DataAsset(name="my_data_asset", type="table")
    data_asset._save_batch_definition = Mock()

    return data_asset


@pytest.mark.unit
def test_save(mock_data_asset):
    batch_definition = BatchDefinition(name="test_batch_definition")
    batch_definition.set_data_asset(mock_data_asset)

    batch_definition.save()

    mock_data_asset._save_batch_definition.assert_called_once_with(batch_definition)


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
):
    partitioner = PartitionerYear(column_name="foo")
    batching_regex = re.compile(r"data_(?P<year>\d{4})-(?P<month>\d{2}).csv")
    batch_definition = BatchDefinition(
        name="test_batch_definition", partitioner=partitioner, batching_regex=batching_regex
    )
    batch_definition.set_data_asset(mock_data_asset)

    batch_definition.build_batch_request(batch_parameters=batch_parameters)

    mock_build_batch_request = batch_definition.data_asset.build_batch_request
    assert isinstance(mock_build_batch_request, Mock)
    mock_build_batch_request.assert_called_once_with(
        options=batch_parameters,
        partitioner=partitioner,
        batching_regex=batching_regex,
    )


@pytest.mark.unit
def test_get_batch_retrieves_only_batch(mocker: pytest_mock.MockFixture):
    # Arrange
    batch_definition = BatchDefinition(name="test_batch_definition")
    mock_asset = mocker.Mock(spec=DataAsset)
    batch_definition.set_data_asset(mock_asset)

    mock_get_batch_list_from_batch_request = mock_asset.get_batch_list_from_batch_request

    mock_batch = mocker.Mock(spec=Batch)
    mock_get_batch_list_from_batch_request.return_value = [mock_batch]

    # Act
    batch = batch_definition.get_batch()

    # Assert
    assert batch == mock_batch
    mock_get_batch_list_from_batch_request.assert_called_once_with(
        batch_definition.build_batch_request()
    )


@pytest.mark.unit
def test_get_batch_retrieves_last_batch(mocker: pytest_mock.MockFixture):
    # Arrange
    batch_definition = BatchDefinition(name="test_batch_definition")
    mock_asset = mocker.Mock(spec=DataAsset)
    batch_definition.set_data_asset(mock_asset)

    mock_get_batch_list_from_batch_request = mock_asset.get_batch_list_from_batch_request

    batch_a = mocker.Mock(spec=Batch)
    batch_b = mocker.Mock(spec=Batch)
    batch_list = [batch_a, batch_b]
    mock_get_batch_list_from_batch_request.return_value = batch_list

    # Act
    batch = batch_definition.get_batch()

    # Assert
    assert batch == batch_b
    mock_get_batch_list_from_batch_request.assert_called_once_with(
        batch_definition.build_batch_request()
    )


@pytest.mark.unit
def test_get_batch_raises_error_with_empty_batch_list(mocker: pytest_mock.MockFixture):
    # Arrange
    batch_definition = BatchDefinition(name="test_batch_definition")
    mock_asset = mocker.Mock(spec=DataAsset)
    batch_definition.set_data_asset(mock_asset)

    mock_get_batch_list_from_batch_request = mock_asset.get_batch_list_from_batch_request

    mock_get_batch_list_from_batch_request.return_value = []

    # Act
    with pytest.raises(ValueError):
        batch_definition.get_batch()

    # Assert
    mock_get_batch_list_from_batch_request.assert_called_once_with(
        batch_definition.build_batch_request()
    )


@pytest.mark.unit
def test_identifier_bundle():
    ds = PandasDatasource(
        name="pandas_datasource",
    )
    asset = ds.add_csv_asset("my_asset", "data.csv")
    batch_definition = asset.add_batch_definition("my_batch_definition")

    assert batch_definition.identifier_bundle() == _EncodedValidationData(
        datasource=_IdentifierBundle(name="pandas_datasource", id=None),
        asset=_IdentifierBundle(name="my_asset", id=None),
        batch_definition=_IdentifierBundle(name="my_batch_definition", id=None),
    )
