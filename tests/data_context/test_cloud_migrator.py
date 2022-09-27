"""TODO: Add docstring"""
from typing import Union, Callable
from unittest import mock

import pytest
from great_expectations.core.usage_statistics.events import UsageStatsEvents

import great_expectations as gx
from great_expectations import DataContext, CloudMigrator


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


@pytest.fixture
def mock_successful_migration(ge_cloud_organization_id: str) -> Callable:

    def _build_mock_migrate(
        test_migrate: bool
    ) -> Union[mock.MagicMock, mock.AsyncMock]:
        context = mock.MagicMock()

        with mock.patch.object(
                CloudMigrator,
                "_migrate_to_cloud",
                return_value=None,
        ), mock.patch(
            "great_expectations.data_context.cloud_migrator.send_usage_message",
            autospec=True,
        ) as mock_send_usage_message:
            CloudMigrator.migrate(context=context, test_migrate=test_migrate, ge_cloud_organization_id=ge_cloud_organization_id)

        return mock_send_usage_message

    return _build_mock_migrate


@pytest.fixture
def mock_failed_migration(ge_cloud_organization_id: str) -> Callable:

    def _build_mock_migrate(
        test_migrate: bool
    ) -> Union[mock.MagicMock, mock.AsyncMock]:
        context = mock.MagicMock()

        with mock.patch.object(
                CloudMigrator,
                "_migrate_to_cloud",
                return_value=None,
                side_effect=Exception,
        ), mock.patch(
            "great_expectations.data_context.cloud_migrator.send_usage_message",
            autospec=True,
        ) as mock_send_usage_message:
            with pytest.raises(Exception):
                CloudMigrator.migrate(context=context, test_migrate=test_migrate, ge_cloud_organization_id=ge_cloud_organization_id)

        return mock_send_usage_message

    return _build_mock_migrate





@pytest.mark.cloud
@pytest.mark.unit
class TestUsageStats:
    def test_migrate_successful_event(self, ge_cloud_organization_id: str, mock_successful_migration: Callable):
        """Test that send_usage_message is called with the right params."""

        mock_send_usage_message = mock_successful_migration(test_migrate=False)

        mock_send_usage_message.assert_called_once_with(
            data_context=mock.ANY,
            event=UsageStatsEvents.CLOUD_MIGRATE.value,
            event_payload={"organization_id": ge_cloud_organization_id},
            success=True,
        )

    def test_migrate_failed_event(self, ge_cloud_organization_id: str, mock_failed_migration: Callable):
        """Test that send_usage_message is called with the right params."""

        mock_send_usage_message = mock_failed_migration(test_migrate=False)

        mock_send_usage_message.assert_called_once_with(
            data_context=mock.ANY,
            event=UsageStatsEvents.CLOUD_MIGRATE.value,
            event_payload={"organization_id": ge_cloud_organization_id},
            success=False,
        )

    def test_no_event_sent_for_migrate_dry_run(self, mock_successful_migration: Callable):
        """No event should be sent for a successful test run."""

        mock_send_usage_message = mock_successful_migration(test_migrate=True)

        mock_send_usage_message.assert_not_called()

    def test_no_event_sent_for_migrate_dry_run_failure(self, mock_failed_migration: Callable):
        """No event should be sent for a failed test run."""

        mock_send_usage_message = mock_failed_migration(test_migrate=True)

        mock_send_usage_message.assert_not_called()
