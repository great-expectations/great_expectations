"""These tests ensure that CloudMigrator works as intended."""
from typing import Callable
from unittest import mock

import pytest

import great_expectations as gx
import great_expectations.exceptions as ge_exceptions
from great_expectations import CloudMigrator
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.data_context.store.ge_cloud_store_backend import (
    GeCloudRESTResource,
)


@pytest.mark.unit
@pytest.mark.cloud
def test__send_configuration_bundle_sends_valid_http_request(
    serialized_configuration_bundle: dict,
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    ge_cloud_access_token: str,
):
    migrator = gx.CloudMigrator(
        context=mock.MagicMock(),
        ge_cloud_base_url=ge_cloud_base_url,
        ge_cloud_organization_id=ge_cloud_organization_id,
        ge_cloud_access_token=ge_cloud_access_token,
    )

    with mock.patch("requests.Session.post", autospec=True) as mock_post:
        migrator._send_configuration_bundle(
            serialized_bundle=serialized_configuration_bundle,
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


@pytest.mark.unit
@pytest.mark.cloud
def test__send_validation_results_sends_valid_http_request(
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    ge_cloud_access_token: str,
):
    # These values aren't actual creds but resemble values used in production
    ge_cloud_base_url = "https://app.test.greatexpectations.io"
    ge_cloud_organization_id = "229616e2-1bbc-4849-8161-4be89b79bd36"
    ge_cloud_access_token = "d7asdh2efads9afah2e0fadf8eh20da8"

    migrator = gx.CloudMigrator(
        context=mock.MagicMock(),
        ge_cloud_base_url=ge_cloud_base_url,
        ge_cloud_organization_id=ge_cloud_organization_id,
        ge_cloud_access_token=ge_cloud_access_token,
    )

    validation_results = [
        {
            "evaluation_parameters": {},
            "meta": {},
            "results": [],
            "statistics": {},
            "success": True,
        }
        for _ in range(5)
    ]

    with mock.patch("requests.Session.post", autospec=True) as mock_post:
        migrator._send_validation_results(
            serialized_validation_results=validation_results
        )

    mock_post.assert_called_with(
        mock.ANY,  # requests.Session object
        f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/expectation-validation-results",
        json={
            "data": {
                "type": GeCloudRESTResource.EXPECTATION_VALIDATION_RESULT,
                "attributes": {
                    "organization_id": ge_cloud_organization_id,
                    "result": validation_results[0],
                },
            }
        },
    )
    assert mock_post.call_count == 5


@pytest.fixture
def mock_successful_migration(
    ge_cloud_base_url: str, ge_cloud_access_token: str, ge_cloud_organization_id: str
) -> Callable:
    def _build_mock_migrate(
        test_migrate: bool,
    ) -> mock.MagicMock:
        context = mock.MagicMock()

        with mock.patch.object(
            CloudMigrator,
            "_migrate_to_cloud",
            return_value=None,
        ), mock.patch(
            f"{CloudMigrator.__module__}.send_usage_message",
            autospec=True,
        ) as mock_send_usage_message:
            CloudMigrator.migrate(
                context=context,
                test_migrate=test_migrate,
                ge_cloud_base_url=ge_cloud_base_url,
                ge_cloud_access_token=ge_cloud_access_token,
                ge_cloud_organization_id=ge_cloud_organization_id,
            )

        return mock_send_usage_message

    return _build_mock_migrate


@pytest.fixture
def mock_failed_migration(
    ge_cloud_base_url: str, ge_cloud_access_token: str, ge_cloud_organization_id: str
) -> Callable:
    def _build_mock_migrate(
        test_migrate: bool,
    ) -> mock.MagicMock:
        context = mock.MagicMock()

        with mock.patch.object(
            CloudMigrator,
            "_migrate_to_cloud",
            return_value=None,
            side_effect=ge_exceptions.MigrationError,
        ), mock.patch(
            f"{CloudMigrator.__module__}.send_usage_message",
            autospec=True,
        ) as mock_send_usage_message:
            with pytest.raises(ge_exceptions.MigrationError):
                CloudMigrator.migrate(
                    context=context,
                    test_migrate=test_migrate,
                    ge_cloud_base_url=ge_cloud_base_url,
                    ge_cloud_access_token=ge_cloud_access_token,
                    ge_cloud_organization_id=ge_cloud_organization_id,
                )

        return mock_send_usage_message

    return _build_mock_migrate


@pytest.mark.cloud
@pytest.mark.unit
class TestUsageStats:
    def test_migrate_successful_event(
        self, ge_cloud_organization_id: str, mock_successful_migration: Callable
    ):
        """Test that send_usage_message is called with the right params."""

        mock_send_usage_message = mock_successful_migration(test_migrate=False)

        mock_send_usage_message.assert_called_once_with(
            data_context=mock.ANY,
            event=UsageStatsEvents.CLOUD_MIGRATE.value,
            event_payload={"organization_id": ge_cloud_organization_id},
            success=True,
        )

    def test_migrate_failed_event(
        self, ge_cloud_organization_id: str, mock_failed_migration: Callable
    ):
        """Test that send_usage_message is called with the right params."""

        mock_send_usage_message = mock_failed_migration(test_migrate=False)

        mock_send_usage_message.assert_called_once_with(
            data_context=mock.ANY,
            event=UsageStatsEvents.CLOUD_MIGRATE.value,
            event_payload={"organization_id": ge_cloud_organization_id},
            success=False,
        )

    def test_no_event_sent_for_migrate_test_run(
        self, mock_successful_migration: Callable
    ):
        """No event should be sent for a successful test run."""

        mock_send_usage_message = mock_successful_migration(test_migrate=True)

        mock_send_usage_message.assert_not_called()

    def test_no_event_sent_for_migrate_test_run_failure(
        self, mock_failed_migration: Callable
    ):
        """No event should be sent for a failed test run."""

        mock_send_usage_message = mock_failed_migration(test_migrate=True)

        mock_send_usage_message.assert_not_called()
