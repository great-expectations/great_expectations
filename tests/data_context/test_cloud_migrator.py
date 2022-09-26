"""TODO: Add docstring"""
from unittest import mock

import pytest

import great_expectations as gx
from great_expectations import DataContext


def test_cloud_migrator_test_migrate_true(empty_data_context: DataContext):
    """TODO: Test is a placeholder."""

    with pytest.raises(NotImplementedError):
        gx.CloudMigrator.migrate(test_migrate=True, context=empty_data_context)


@pytest.mark.unit
@pytest.mark.cloud
def test__send_configuration_bundle_sends_valid_http_request(
    serialized_configuration_bundle: dict,
):
    # These values aren't actual creds but resemble values used in production
    ge_cloud_base_url = "https://app.test.greatexpectations.io"
    ge_cloud_organization_id = "229616e2-1bbc-4849-8161-4be89b79bd36"
    ge_cloud_access_token = "d7asdh2efads9afah2e0fadf8eh20da8"

    # Mock any external dependencies so we're only testing HTTP request logic
    mock_context = mock.MagicMock()
    configuration_bundle = mock.MagicMock()
    serializer = mock.MagicMock()
    serializer.serialize.return_value = serialized_configuration_bundle

    migrator = gx.CloudMigrator(
        context=mock_context,
        ge_cloud_base_url=ge_cloud_base_url,
        ge_cloud_organization_id=ge_cloud_organization_id,
        ge_cloud_access_token=ge_cloud_access_token,
    )

    with mock.patch("requests.Session.post", autospec=True) as mock_post:
        migrator._send_configuration_bundle(
            configuration_bundle=configuration_bundle, serializer=serializer
        )

    mock_post.assert_called_once_with(
        mock.ANY,  # requests.Session object
        f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/migration",
        json={
            "data": {
                "type": "migration",
                "attributes": {
                    "organization_id": ge_cloud_organization_id,
                    "bundle": serialized_configuration_bundle,
                },
            }
        },
    )
