from __future__ import annotations

import pathlib
import re
from typing import TYPE_CHECKING, Optional
from unittest.mock import Mock  # noqa: TID251

import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.expectation_suite import ExpectationSuite
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
    batch_definition = BatchDefinition[None](name="test_batch_definition")
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
    batch_definition = BatchDefinition[None](name="test_batch_definition")
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
    batch_definition = BatchDefinition[None](name="test_batch_definition")
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


@pytest.fixture
def pandas_batch_definition(csv_path: pathlib.Path) -> BatchDefinition:
    context = gx.get_context(mode="ephemeral")
    source = context.sources.add_pandas("my_pandas")
    filepath = (
        csv_path
        / "ten_trips_from_each_month"
        / "yellow_tripdata_sample_10_trips_from_each_month.csv"
    )
    asset = source.add_csv_asset("my_csv", filepath_or_buffer=filepath)
    batch_definition = asset.add_batch_definition("my_batch_definition")
    return batch_definition


@pytest.mark.filesystem
def test_batch_validate_expectation(pandas_batch_definition: BatchDefinition):
    # Make Expectation
    expectation = gxe.ExpectColumnValuesToNotBeNull(column="vendor_id", mostly=0.95)
    # Validate
    result = pandas_batch_definition.run(expectation)
    # Asserts on result
    assert result.success is True


@pytest.mark.filesystem
def test_batch_validate_expectation_suite(pandas_batch_definition: BatchDefinition):
    suite = ExpectationSuite("my_suite")
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="vendor_id", mostly=0.95))

    # Validate
    result = pandas_batch_definition.run(suite)

    # Asserts on result
    assert result.success is True
