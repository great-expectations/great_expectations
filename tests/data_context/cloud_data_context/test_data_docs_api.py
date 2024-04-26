from unittest import mock

import pytest
import pytest_mock

from great_expectations.checkpoint.checkpoint import CheckpointResult
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)


@pytest.mark.cloud
def test_view_validation_result(
    empty_cloud_data_context: CloudDataContext,
    mocker: pytest_mock.MockFixture,
):
    context = empty_cloud_data_context
    result_url = "https://my.cloud.app/validation-result/123"
    checkpoint_result = mocker.Mock(spec=CheckpointResult, result_url=result_url)

    with mock.patch("webbrowser.open") as mock_open:
        context.view_validation_result(checkpoint_result)

    mock_open.assert_called_once_with(result_url)
