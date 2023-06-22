"""These tests ensure that CloudMigrator works as intended."""
import logging
from typing import Any, Callable, List
from unittest import mock

import pytest

import great_expectations as gx
import great_expectations.exceptions as gx_exceptions
from great_expectations import CloudMigrator
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.migrator.cloud_migrator import MigrationResponse
from great_expectations.data_context.types.base import AnonymizedUsageStatisticsConfig
from tests.data_context.migrator.conftest import StubBaseDataContext


@pytest.fixture
def migrator_factory(
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    ge_cloud_access_token: str,
) -> Callable:
    def _create_migrator(context: Any) -> CloudMigrator:
        return gx.CloudMigrator(
            context=context,
            cloud_base_url=ge_cloud_base_url,
            cloud_organization_id=ge_cloud_organization_id,
            cloud_access_token=ge_cloud_access_token,
        )

    return _create_migrator


@pytest.fixture
def migrator_with_mock_context(
    migrator_factory: Callable,
) -> CloudMigrator:
    return migrator_factory(context=mock.MagicMock)


@pytest.fixture
def migrator_with_stub_base_data_context(
    migrator_factory: Callable,
    stub_base_data_context: StubBaseDataContext,
) -> CloudMigrator:
    return migrator_factory(context=stub_base_data_context)


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
                cloud_base_url=ge_cloud_base_url,
                cloud_access_token=ge_cloud_access_token,
                cloud_organization_id=ge_cloud_organization_id,
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
            side_effect=gx_exceptions.MigrationError,
        ), mock.patch(
            f"{CloudMigrator.__module__}.send_usage_message",
            autospec=True,
        ) as mock_send_usage_message:
            with pytest.raises(gx_exceptions.MigrationError):
                CloudMigrator.migrate(
                    context=context,
                    test_migrate=test_migrate,
                    cloud_base_url=ge_cloud_base_url,
                    cloud_access_token=ge_cloud_access_token,
                    cloud_organization_id=ge_cloud_organization_id,
                )

        return mock_send_usage_message

    return _build_mock_migrate


def assert_stdout_is_accurate_and_properly_ordered(
    stdout: str, statements: List[str]
) -> None:
    last_position = -1
    for statement in statements:
        position = stdout.find(statement)
        assert position != -1, f"Could not find '{statement}' in stdout"
        assert (
            position > last_position
        ), f"Statement '{statement}' occurred in the wrong order"


@pytest.mark.unit
@pytest.mark.cloud
def test__send_configuration_bundle_sends_valid_http_request(
    serialized_configuration_bundle: dict,
    migrator_with_mock_context: CloudMigrator,
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
):
    migrator = migrator_with_mock_context
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
    migrator_with_mock_context: CloudMigrator,
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
):
    migrator = migrator_with_mock_context

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
                "type": GXCloudRESTResource.EXPECTATION_VALIDATION_RESULT,
                "attributes": {
                    "organization_id": ge_cloud_organization_id,
                    "result": validation_results[keys[0]],
                },
            }
        },
    )
    assert mock_post.call_count == 5


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
            event=UsageStatsEvents.CLOUD_MIGRATE,
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
            event=UsageStatsEvents.CLOUD_MIGRATE,
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
@pytest.mark.parametrize("test_migrate", [True, False])
@pytest.mark.parametrize("include_datasources", [True, False])
@pytest.mark.parametrize("enable_usage_stats", [True, False])
def test__migrate_to_cloud_outputs_warnings(
    migrator_factory: Callable,
    test_migrate: bool,
    include_datasources: bool,
    enable_usage_stats: bool,
    caplog,
):
    anonymized_usage_statistics_config = AnonymizedUsageStatisticsConfig(
        enabled=enable_usage_stats
    )
    datasource_names = ("my_datasource",) if include_datasources else tuple()

    context = StubBaseDataContext(
        anonymized_usage_statistics_config=anonymized_usage_statistics_config,
        datasource_names=datasource_names,
    )

    migrator = migrator_factory(context=context)

    caplog.set_level(logging.INFO)
    with mock.patch("requests.Session.post", autospec=True):
        migrator._migrate_to_cloud(test_migrate=test_migrate)

    actual_logs = [rec.message for rec in caplog.records]
    aggregated_log_output = "\n".join(log for log in actual_logs)

    expected_log_count = 0
    if test_migrate:
        expected_log_count += 1
        assert (
            "This is a test run! Please pass `test_migrate=False` to begin the actual migration"
            in aggregated_log_output
        )
    if not enable_usage_stats:
        expected_log_count += 1
        assert (
            "Please note that by migrating your context to GX Cloud your new Cloud Data Context will emit usage statistics."
            in aggregated_log_output
        )
    if include_datasources:
        expected_log_count += 1
        assert (
            "Since your existing context includes one or more datasources, please note that if your credentials are included"
            in aggregated_log_output
        )

    assert len(actual_logs) == expected_log_count


@pytest.mark.unit
@pytest.mark.cloud
@pytest.mark.parametrize(
    "test_migrate,expected_statements",
    [
        pytest.param(
            True,
            [
                "Thank you for using Great Expectations!",
                "We will now begin the migration process to GX Cloud.",
                "First we will bundle your existing context configuration and send it to the Cloud backend.",
                "Then we will send each of your validation results.",
                "[Step 1/4]: Bundling context configuration",
                "Bundled 1 Datasource(s):",
                "Bundled 1 Checkpoint(s):",
                "Bundled 1 Expectation Suite(s):",
                "Bundled 1 Profiler(s):",
                "[Step 2/4]: Preparing validation results",
                "[Step 3/4]: Sending context configuration",
                "[Step 4/4]: Sending validation results",
                "Test run completed!",
            ],
        ),
        pytest.param(
            False,
            [
                "Thank you for using Great Expectations!",
                "We will now begin the migration process to GX Cloud.",
                "First we will bundle your existing context configuration and send it to the Cloud backend.",
                "Then we will send each of your validation results.",
                "[Step 1/4]: Bundling context configuration",
                "Bundled 1 Datasource(s):",
                "Bundled 1 Checkpoint(s):",
                "Bundled 1 Expectation Suite(s):",
                "Bundled 1 Profiler(s):",
                "[Step 2/4]: Preparing validation results",
                "[Step 3/4]: Sending context configuration",
                "[Step 4/4]: Sending validation results",
                "Success!",
                "Now that you have migrated your Data Context to GX Cloud, you should use your Cloud Data Context from now on to interact with Great Expectations.",
                "If you continue to use your existing Data Context your configurations could become out of sync.",
            ],
        ),
    ],
)
def test__migrate_to_cloud_happy_path_prints_to_stdout(
    test_migrate: bool,
    expected_statements: List[str],
    migrator_with_stub_base_data_context: CloudMigrator,
    capsys,
):
    migrator = migrator_with_stub_base_data_context

    with mock.patch("requests.Session.post", autospec=True):
        migrator._migrate_to_cloud(test_migrate=test_migrate)

    stdout, _ = capsys.readouterr()
    assert_stdout_is_accurate_and_properly_ordered(
        stdout=stdout, statements=expected_statements
    )


@pytest.mark.unit
@pytest.mark.cloud
def test__migrate_to_cloud_bad_bundle_request_prints_to_stdout(
    migrator_with_stub_base_data_context: CloudMigrator,
    capsys,
):
    migrator = migrator_with_stub_base_data_context

    with mock.patch(
        "great_expectations.data_context.migrator.cloud_migrator.CloudMigrator._post_to_cloud_backend",
        autospec=True,
    ) as mock_post:
        mock_post.return_value = MigrationResponse(
            message="Bad request!", status_code=400, success=False
        )
        migrator._migrate_to_cloud(test_migrate=False)

    stdout, _ = capsys.readouterr()
    expected_statements = [
        "Thank you for using Great Expectations!",
        "We will now begin the migration process to GX Cloud.",
        "First we will bundle your existing context configuration and send it to the Cloud backend.",
        "Then we will send each of your validation results.",
        "[Step 1/4]: Bundling context configuration",
        "Bundled 1 Datasource(s):",
        "Bundled 1 Checkpoint(s):",
        "Bundled 1 Expectation Suite(s):",
        "Bundled 1 Profiler(s):",
        "[Step 2/4]: Preparing validation results",
        "[Step 3/4]: Sending context configuration",
        "There was an error sending your configuration to GX Cloud!",
        "We have reverted your GX Cloud configuration to the state before the migration.",
        "The server returned the following error:",
        "Code : 400",
        "Error: Bad request!",
    ]

    assert_stdout_is_accurate_and_properly_ordered(
        stdout=stdout, statements=expected_statements
    )


@pytest.mark.unit
@pytest.mark.cloud
def test__migrate_to_cloud_bad_validations_request_prints_to_stdout(
    migrator_with_stub_base_data_context: CloudMigrator,
    capsys,
):
    migrator = migrator_with_stub_base_data_context

    good_response = MigrationResponse(
        message="Good request!", status_code=200, success=True
    )
    bad_response = MigrationResponse(
        message="Bad request!", status_code=400, success=False
    )

    with mock.patch(
        "great_expectations.data_context.migrator.cloud_migrator.CloudMigrator._post_to_cloud_backend",
        autospec=True,
    ) as mock_post:
        # Ensure that the first call, which is the bundle request, goes through successfully
        mock_post.side_effect = [good_response, bad_response]
        migrator._migrate_to_cloud(test_migrate=False)

    stdout, _ = capsys.readouterr()
    expected_statements = [
        "Thank you for using Great Expectations!",
        "We will now begin the migration process to GX Cloud.",
        "First we will bundle your existing context configuration and send it to the Cloud backend.",
        "Then we will send each of your validation results.",
        "[Step 1/4]: Bundling context configuration",
        "Bundled 1 Datasource(s):",
        "Bundled 1 Checkpoint(s):",
        "Bundled 1 Expectation Suite(s):",
        "Bundled 1 Profiler(s):",
        "[Step 2/4]: Preparing validation results",
        "[Step 3/4]: Sending context configuration",
        "[Step 4/4]: Sending validation results",
        "Error sending validation result 'some_key' (1/1)",
        "Partial success!",
        "Now that you have migrated your Data Context to GX Cloud, you should use your Cloud Data Context from now on to interact with Great Expectations.",
        "If you continue to use your existing Data Context your configurations could become out of sync.",
        "Please note that there were 1 validation result(s) that were not successfully migrated",
        "To retry uploading these validation results, you can use the following code snippet:",
        "migrator.retry_migrate_validation_results()",
    ]

    assert_stdout_is_accurate_and_properly_ordered(
        stdout=stdout, statements=expected_statements
    )
