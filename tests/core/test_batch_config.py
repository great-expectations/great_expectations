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
from great_expectations.datasource.fluent.sql_datasource import TableAsset
from great_expectations.datasource.fluent.sqlite_datasource import SqliteDatasource


@pytest.fixture
def mock_asset_name() -> str:
    return "foo"


@pytest.fixture
def mock_asset(
    mock_asset_name: str,
) -> TableAsset:
    mock_data_asset = Mock(spec=TableAsset)
    mock_data_asset.name = mock_asset_name
    return mock_data_asset


@pytest.fixture
def data_context(
    fds_data_context: AbstractDataContext,
    fds_data_context_datasource_name: str,
    mock_asset: TableAsset,
) -> AbstractDataContext:
    datasource = fds_data_context.get_datasource(fds_data_context_datasource_name)
    assert isinstance(datasource, SqliteDatasource)
    datasource.assets.append(mock_asset)
    return fds_data_context

@pytest.mark.unit
def test_data_asset(
    data_context: AbstractDataContext,
    fds_data_context_datasource_name: str,
    mock_asset_name: str,
    mock_asset: DataAsset,
):
    batch_config = BatchConfig(
        context=data_context,
        datasource_name=fds_data_context_datasource_name,
        data_asset_name=mock_asset_name,
    )
    assert batch_config.data_asset == mock_asset

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
    data_context: AbstractDataContext,
    fds_data_context_datasource_name: str,
    mock_asset_name: str,
):
    batch_config = BatchConfig(
        context=data_context,
        datasource_name=fds_data_context_datasource_name,
        data_asset_name=mock_asset_name,
    )

    batch_config.build_batch_request(batch_request_options=batch_request_options)

    mock_build_batch_request = batch_config.data_asset.build_batch_request
    assert isinstance(mock_build_batch_request, Mock)
    mock_build_batch_request.assert_called_once_with(options=batch_request_options)
