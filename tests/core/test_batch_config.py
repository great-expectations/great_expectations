from __future__ import annotations

from typing import Optional
from unittest.mock import Mock

import pytest

from great_expectations.core.batch_config import BatchConfig
from great_expectations.datasource.fluent.batch_request import BatchRequestOptions
from great_expectations.datasource.fluent.interfaces import DataAsset


@pytest.mark.parametrize(
    "batch_request_options",
    [
        (None,),
        ({"foo": "bar"},),
    ],
)
@pytest.mark.unit
def test_build_batch_request(batch_request_options: Optional[BatchRequestOptions]):
    batch_config = BatchConfig(data_asset=Mock(spec=DataAsset))

    batch_config.build_batch_request(batch_request_options=batch_request_options)

    mock_build_batch_request = batch_config.data_asset.build_batch_request
    assert isinstance(mock_build_batch_request, Mock)

    mock_build_batch_request.assert_called_once_with(options=batch_request_options)
