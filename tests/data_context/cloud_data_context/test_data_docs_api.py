from unittest import mock

import pytest
import pytest_mock

from great_expectations.checkpoint.checkpoint import CheckpointResult
from great_expectations.core.expectation_validation_result import ExpectationSuiteValidationResult
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)


@pytest.mark.cloud
def test_view_validation_result(
    empty_cloud_data_context: CloudDataContext,
    mocker: pytest_mock.MockFixture,
):
    context = empty_cloud_data_context

    result_url_1 = "https://my.cloud.app/validation-result/123"
    result_url_2 = "https://my.cloud.app/validation-result/123"
    validation_result_1 = mocker.Mock(
        spec=ExpectationSuiteValidationResult, result_url=result_url_1
    )
    validation_result_2 = mocker.Mock(
        spec=ExpectationSuiteValidationResult, result_url=result_url_2
    )
    checkpoint_result = mocker.Mock(
        spec=CheckpointResult,
        run_results={
            "validation_result_1": validation_result_1,
            "validation_result_2": validation_result_2,
        },
    )

    with mock.patch("webbrowser.open") as mock_open:
        context.view_validation_result(checkpoint_result)

    assert mock_open.call_count == 2
    mock_open.assert_any_call(result_url_1)
    mock_open.assert_any_call(result_url_2)
