from unittest import mock

import pytest

from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)


@pytest.mark.integration
@pytest.mark.cloud
def test_view_validation_result(
    empty_cloud_data_context: CloudDataContext,
    checkpoint_result: CheckpointResult,
):
    context = empty_cloud_data_context

    with mock.patch("webbrowser.open") as mock_open:
        context.view_validation_result(checkpoint_result)

    mock_open.assert_called_once_with("https://my.cloud.app/validation-result/123")
