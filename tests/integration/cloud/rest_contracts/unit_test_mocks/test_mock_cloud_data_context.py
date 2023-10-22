import pytest

from great_expectations.data_context import CloudDataContext


@pytest.mark.cloud
def test_mock_cloud_data_context(mock_cloud_data_context: CloudDataContext):
    assert isinstance(mock_cloud_data_context, CloudDataContext)
    assert mock_cloud_data_context.variables.include_rendered_content.globally is True
    assert (
        mock_cloud_data_context.variables.include_rendered_content.expectation_suite
        is True
    )
    assert (
        mock_cloud_data_context.variables.include_rendered_content.expectation_validation_result
        is True
    )
