"""These tests ensure that CloudMigrator works as intended."""
import sys
from typing import Callable, List
from unittest import mock

import pytest

import great_expectations as gx
import great_expectations.exceptions as ge_exceptions
from great_expectations import CloudMigrator
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.data_context.store.ge_cloud_store_backend import (
    GeCloudRESTResource,
)
from tests.data_context.migrator.conftest import StubBaseDataContext


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
            serialized_bundle=serialized_configuration_bundle, test_migrate=False
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
    migrator = gx.CloudMigrator(
        context=mock.MagicMock(),
        ge_cloud_base_url=ge_cloud_base_url,
        ge_cloud_organization_id=ge_cloud_organization_id,
        ge_cloud_access_token=ge_cloud_access_token,
    )

    keys = [f"key_{i}" for i in range(5)]
    validation_results = {
        key: {
            "evaluation_parameters": {},
            "meta": {},
            "results": [],
            "statistics": {},
            "success": True,
        }
        for key in keys
    }

    with mock.patch("requests.Session.post", autospec=True) as mock_post:
        migrator._send_validation_results(
            serialized_validation_results=validation_results, test_migrate=False
        )

    mock_post.assert_called_with(
        mock.ANY,  # requests.Session object
        f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/expectation-validation-results",
        json={
            "data": {
                "type": GeCloudRESTResource.EXPECTATION_VALIDATION_RESULT,
                "attributes": {
                    "organization_id": ge_cloud_organization_id,
                    "result": validation_results[keys[0]],
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


@pytest.mark.unit
@pytest.mark.cloud
@pytest.mark.parametrize(
    "test_migrate,expected_logs",
    [
        pytest.param(
            True,
            [
                "This is a test run! Please pass `test_migrate=False` to begin the actual migration",
                "Since your existing context includes one or more datasources, please note that if your credentials are included in the datasource config, they will be sent to the GX Cloud backend",
            ],
        ),
        pytest.param(
            False,
            [
                "Since your existing context includes one or more datasources, please note that if your credentials are included in the datasource config, they will be sent to the GX Cloud backend",
            ],
        ),
    ],
)
def test__migrate_to_cloud_happy_path_outputs_logs_and_warnings(
    stub_base_data_context: StubBaseDataContext,
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    ge_cloud_access_token: str,
    test_migrate: bool,
    expected_logs: List[str],
    caplog,
    capsys,
):
    migrator = gx.CloudMigrator(
        context=stub_base_data_context,
        ge_cloud_base_url=ge_cloud_base_url,
        ge_cloud_organization_id=ge_cloud_organization_id,
        ge_cloud_access_token=ge_cloud_access_token,
    )

    with mock.patch("requests.Session.post", autospec=True) as mock_post:
        migrator._migrate_to_cloud(test_migrate=test_migrate)

    actual_logs = [rec.message for rec in caplog.records]
    assert len(actual_logs) == len(expected_logs)
    for expected, actual in zip(expected_logs, actual_logs):
        assert expected in actual

    # Regardless of `test_migrate` being True/False, the following message should appear in stdout
    expected_stdout = [
        "Thank you for using Great Expectations!",
        "We will now begin the migration process to GX Cloud.",
        "[Step 1/4: Bundling context configuration]",
        "Bundled 1 Datasource(s):",
        "my_datasource",
        "Bundled 1 Checkpoint(s):",
        "my_checkpoint",
        "Bundled 1 Expectation Suite(s):",
        "my_suite",
        "Bundled 1 Profiler(s):",
        "my_profiler",
        "[Step 2/4: Preparing validation results]",
        "[Step 3/4: Sending context configuration]",
        "[Step 4/4: Sending validation results]",
        "Success!",
        "If you continue to use your existing Data Context your configurations could become out of sync.",
    ]
    actual_stdout = capsys.readouterr().out

    # Each string in expected_stdout should be present in the listed order
    last_position = -1
    for expected in expected_stdout:
        position = actual_stdout.find(expected)  # No match results in -1
        assert position > last_position
        last_position = position
