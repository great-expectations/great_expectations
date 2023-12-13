from __future__ import annotations

from typing import Optional
from unittest.mock import Mock

import pytest

from great_expectations.core.batch_config import BatchConfig
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.datasource.fluent.batch_request import BatchRequestOptions
from great_expectations.datasource.fluent.interfaces import DataAsset
from great_expectations.datasource.fluent.pandas_datasource import CSVAsset, TableAsset


@pytest.fixture
def mock_data_asset() -> DataAsset:
    data_asset: DataAsset = CSVAsset(
        name="my_data_asset", type="csv", filepath_or_buffer="taxi.csv"
    )
    data_asset._save_batch_config = Mock()

    # data_asset = Mock(spec=DataAsset)

    return data_asset


@pytest.fixture
def data_asset(
    fds_data_context: AbstractDataContext,
) -> DataAsset:
    return fds_data_context.sources.add_pandas("my_datasource").add_csv_asset(
        "taxi town", "taxi.csv"
    )


@pytest.mark.unit
def test_save(mock_data_asset):
    batch_config = BatchConfig(
        name="test_batch_config",
        data_asset=mock_data_asset,
    )

    batch_config.save()

    mock_data_asset._save_batch_config.assert_called_once_with(batch_config)


@pytest.mark.unit
def test_data_asset(
    mock_data_asset: DataAsset,
):
    batch_config = BatchConfig(name="foo", data_asset=mock_data_asset)
    mock_data_asset.name = "bad name"

    assert batch_config.data_asset == mock_data_asset


@pytest.mark.parametrize(
    "batch_request_options",
    [
        (None,),
        ({"foo": "bar"},),
    ],
)
@pytest.mark.unit
def test_build_batch_request(
    batch_request_options: Optional[BatchRequestOptions],
    mock_data_asset: DataAsset,
):
    batch_config = BatchConfig(
        name="test_batch_config",
        data_asset=mock_data_asset,
    )

    batch_config.build_batch_request(batch_request_options=batch_request_options)

    mock_build_batch_request = batch_config.data_asset.build_batch_request
    assert isinstance(mock_build_batch_request, Mock)
    mock_build_batch_request.assert_called_once_with(options=batch_request_options)
