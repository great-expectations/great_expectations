"""TODO: Add docstring"""
from unittest import mock

import pytest

import great_expectations as gx
from great_expectations import DataContext
from great_expectations.data_context.migrator.cloud_migrator import CloudMigrator


@pytest.fixture
def mock_context_with_disabled_usage_stats() -> mock.MagicMock:
    context = mock.MagicMock()
    variables = context.variables
    usage_stats = variables.anonymous_usage_statistics
    usage_stats.enabled = False
    return context


@pytest.fixture
def migrator_with_mock_context(
    mock_context_with_disabled_usage_stats: mock.MagicMock,
) -> CloudMigrator:
    ge_cloud_base_url = ""
    ge_cloud_access_token = ""
    ge_cloud_organization_id = ""

    return CloudMigrator(
        context=mock_context_with_disabled_usage_stats,
        ge_cloud_base_url=ge_cloud_base_url,
        ge_cloud_access_token=ge_cloud_access_token,
        ge_cloud_organization_id=ge_cloud_organization_id,
    )


def test_cloud_migrator_test_migrate_true(empty_data_context: DataContext):
    """TODO: Test is a placeholder."""

    with pytest.raises(NotImplementedError):
        gx.CloudMigrator.migrate(test_migrate=True, context=empty_data_context)


@pytest.mark.unit
@pytest.mark.cloud
def test_cloud_migrator_test_migrate_true_emits_warnings(
    migrator_with_mock_context: CloudMigrator,
):
    migrator = migrator_with_mock_context

    with pytest.warns() as record:
        migrator._migrate_to_cloud(test_migrate=True)

    assert len(record) == 2
    assert "Please pass `test_migrate=False` to begin the actual migration" in str(
        record[0].message
    )
    assert (
        "Please note that by migrating your context to GX Cloud your new Cloud Data Context will emit usage statistics"
        in str(record[1].message)
    )


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
