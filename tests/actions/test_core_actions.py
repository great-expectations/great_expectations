from __future__ import annotations

import logging
import smtplib
from contextlib import contextmanager
from datetime import datetime, timezone
from types import ModuleType
from typing import TYPE_CHECKING, Iterator, Literal
from unittest import mock

import pytest
import requests
from requests import Session

import great_expectations as gx
from great_expectations.checkpoint.actions import (
    ActionContext,
    APINotificationAction,
    EmailAction,
    MicrosoftTeamsNotificationAction,
    NotifyOn,
    OpsgenieAlertAction,
    PagerdutyAlertAction,
    SlackNotificationAction,
    SNSNotificationAction,
    UpdateDataDocsAction,
    ValidationAction,
    should_notify,
)
from great_expectations.checkpoint.checkpoint import Checkpoint, CheckpointResult
from great_expectations.core.batch import IDDict, LegacyBatchDefinition
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.data_context.cloud_data_context import CloudDataContext
from great_expectations.data_context.data_context.context_factory import (
    project_manager,
    set_context,
)
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    GXCloudIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.exceptions.exceptions import ValidationActionAlreadyRegisteredError
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration
from great_expectations.expectations.metadata_types import FailureSeverity
from great_expectations.util import is_library_loadable

if TYPE_CHECKING:
    from pytest_mock import MockerFixture
    from typing_extensions import Never

logger = logging.getLogger(__name__)


# Global constants to be referenced in both tests and fixtures
SUITE_A: str = "suite_a"
SUITE_B: str = "suite_b"
BATCH_ID_A: str = "my_datasource-my_first_asset"
BATCH_ID_B: str = "my_datasource-my_second_asset"
utc_datetime = datetime.fromisoformat("2024-04-01T20:51:18.077262").replace(tzinfo=timezone.utc)


@pytest.fixture
def checkpoint_result(mocker: MockerFixture):
    utc_datetime = datetime.fromisoformat("2024-04-01T20:51:18.077262").replace(tzinfo=timezone.utc)
    return CheckpointResult(
        run_id=RunIdentifier(run_time=utc_datetime),
        run_results={
            ValidationResultIdentifier(
                expectation_suite_identifier=ExpectationSuiteIdentifier(
                    name=SUITE_A,
                ),
                run_id=RunIdentifier(run_name="prod_20240401"),
                batch_identifier=BATCH_ID_A,
            ): ExpectationSuiteValidationResult(
                success=True,
                statistics={"successful_expectations": 3, "evaluated_expectations": 3},
                results=[],
                suite_name=SUITE_A,
            ),
            ValidationResultIdentifier(
                expectation_suite_identifier=ExpectationSuiteIdentifier(
                    name=SUITE_B,
                ),
                run_id=RunIdentifier(run_name="prod_20240402"),
                batch_identifier=BATCH_ID_B,
            ): ExpectationSuiteValidationResult(
                success=True,
                statistics={"successful_expectations": 2, "evaluated_expectations": 2},
                results=[],
                suite_name=SUITE_B,
            ),
        },
        checkpoint_config=Checkpoint(
            name="test-checkpoint",
            validation_definitions=[
                mocker.MagicMock(spec=ValidationDefinition),
                mocker.MagicMock(spec=ValidationDefinition),
            ],
        ),
    )


@pytest.fixture
def checkpoint_result_with_failure(mocker: MockerFixture):
    """Create a checkpoint result with failed validations (warning-level severity)."""
    utc_datetime = datetime.fromisoformat("2024-04-01T20:51:18.077262").replace(tzinfo=timezone.utc)

    # Create a real failed expectation validation result
    failed_evr = ExpectationValidationResult(
        success=False,
        expectation_config=ExpectationConfiguration(
            "expect_column_values_to_be_between",
            kwargs={"column": "test_column", "min_value": 0, "max_value": 100},
            severity=FailureSeverity.WARNING,
        ),
        result={},
        exception_info=None,
        meta={},
    )

    return CheckpointResult(
        run_id=RunIdentifier(run_time=utc_datetime),
        run_results={
            ValidationResultIdentifier(
                expectation_suite_identifier=ExpectationSuiteIdentifier(
                    name=SUITE_A,
                ),
                run_id=RunIdentifier(run_name="prod_20240401"),
                batch_identifier=BATCH_ID_A,
            ): ExpectationSuiteValidationResult(
                success=False,  # Failed validation
                statistics={"successful_expectations": 1, "evaluated_expectations": 3},
                results=[failed_evr],
                suite_name=SUITE_A,
            ),
        },
        checkpoint_config=Checkpoint(
            name="test-checkpoint-failure",
            validation_definitions=[mocker.MagicMock(spec=ValidationDefinition)],
        ),
    )


@pytest.fixture
def checkpoint_result_with_assets(mocker: MockerFixture):
    utc_datetime = datetime.fromisoformat("2024-04-01T20:51:18.077262").replace(tzinfo=timezone.utc)
    return CheckpointResult(
        run_id=RunIdentifier(run_time=utc_datetime),
        run_results={
            ValidationResultIdentifier(
                expectation_suite_identifier=ExpectationSuiteIdentifier(
                    name=SUITE_A,
                ),
                run_id=RunIdentifier(run_name="prod_20240401"),
                batch_identifier=BATCH_ID_A,
            ): ExpectationSuiteValidationResult(
                success=True,
                statistics={"successful_expectations": 3, "evaluated_expectations": 3},
                results=[],
                suite_name=SUITE_A,
                meta={
                    "active_batch_definition": LegacyBatchDefinition(
                        datasource_name="test_environment",
                        data_connector_name="general_azure_data_connector",
                        data_asset_name="asset_1",
                        batch_identifiers=IDDict(
                            {"name": "alex", "timestamp": "20200809", "price": "1000"}
                        ),
                    )
                },
                result_url="www.testing",
            ),
            ValidationResultIdentifier(
                expectation_suite_identifier=ExpectationSuiteIdentifier(
                    name=SUITE_B,
                ),
                run_id=RunIdentifier(run_name="prod_20240402"),
                batch_identifier=BATCH_ID_B,
            ): ExpectationSuiteValidationResult(
                success=True,
                statistics={"successful_expectations": 2, "evaluated_expectations": 2},
                results=[],
                suite_name=SUITE_B,
                meta={
                    "active_batch_definition": LegacyBatchDefinition(
                        datasource_name="test_environment",
                        data_connector_name="general_azure_data_connector",
                        data_asset_name="asset_2_two_wow_whoa_vroom",
                        batch_identifiers=IDDict(
                            {"name": "alex", "timestamp": "20200809", "price": "1000"}
                        ),
                    ),
                },
            ),
        },
        checkpoint_config=mocker.Mock(spec=Checkpoint, name="my_checkpoint"),
    )


@contextmanager
def mock_not_imported_module(
    parent_module: ModuleType, target_name: str, mocker: MockerFixture
) -> Iterator[Never]:
    original = getattr(parent_module, target_name)
    try:
        setattr(parent_module, target_name, mocker.Mock())
        yield getattr(parent_module, target_name)
    finally:
        setattr(parent_module, target_name, original)


class TestAPINotificationAction:
    @pytest.mark.unit
    def test_create_payload(self, mock_context):
        mock_validation_results = []
        expected_payload = {
            "test_suite_name": "my_suite",
            "data_asset_name": "my_schema.my_table",
            "validation_results": [],
        }
        api_notification_action = APINotificationAction(
            name="my_api_notification", url="http://www.example.com"
        )
        payload = api_notification_action.create_payload(
            "my_schema.my_table", "my_suite", mock_validation_results
        )
        assert payload == expected_payload

    @pytest.mark.unit
    def test_run(self, checkpoint_result: CheckpointResult):
        url = "http://www.example.com"
        action = APINotificationAction(name="my_action", url=url)

        with mock.patch.object(requests, "post") as mock_post:
            action.run(checkpoint_result=checkpoint_result)

        mock_post.assert_called_once_with(
            url,
            headers={"Content-Type": "application/json"},
            data=[
                {
                    "data_asset_name": BATCH_ID_A,
                    "test_suite_name": SUITE_A,
                    "validation_results": [],
                },
                {
                    "data_asset_name": BATCH_ID_B,
                    "test_suite_name": SUITE_B,
                    "validation_results": [],
                },
            ],
        )


class TestEmailAction:
    @pytest.mark.unit
    def test_equality(self):
        """I know, this one seems silly. But this was a bug."""
        a = EmailAction(
            name="my_action",
            smtp_address="test",
            smtp_port="587",
            receiver_emails="test@gmail.com",
        )
        b = EmailAction(
            name="my_action",
            smtp_address="test",
            smtp_port="587",
            receiver_emails="test@gmail.com",
        )

        assert a == b

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "emails, expected_email_list",
        [
            pytest.param("test1@gmail.com", ["test1@gmail.com"], id="single_email"),
            pytest.param(
                "test1@gmail.com, test2@hotmail.com",
                ["test1@gmail.com", "test2@hotmail.com"],
                id="multiple_emails",
            ),
            pytest.param(
                "test1@gmail.com,test2@hotmail.com",
                ["test1@gmail.com", "test2@hotmail.com"],
                id="multiple_emails_no_space",
            ),
        ],
    )
    def test_run(
        self,
        checkpoint_result: CheckpointResult,
        emails: str,
        expected_email_list: list[str],
    ):
        action = EmailAction(
            name="my_action",
            smtp_address="test",
            smtp_port="587",
            receiver_emails=emails,
        )

        with mock.patch.object(smtplib, "SMTP") as mock_server:
            out = action.run(checkpoint_result=checkpoint_result)

        mock_send_email = mock_server().sendmail

        # Should contain success/failure in title
        assert (
            f"Subject: {checkpoint_result.checkpoint_config.name}: True"
            in mock_send_email.call_args.args[-1]
        )

        mock_send_email.assert_called_once_with(
            None,
            expected_email_list,
            mock.ANY,
        )
        assert out == {"email_result": "success"}

    @pytest.mark.unit
    def test_run_smptp_address_substitution(self, checkpoint_result: CheckpointResult):
        config_provider = project_manager.get_config_provider()
        assert isinstance(config_provider, mock.Mock)  # noqa: TID251 # just using for the instance compare

        SMPT_ADDRESS_KEY = "${smtp_address}"
        SMPT_PORT_KEY = "${smtp_port}"
        SENDER_LOGIN_KEY = "${sender_login}"
        SENDER_ALIAS_KEY = "${sender_alias_login}"
        SENDER_PASSWORD_KEY = "${sender_password_login}"
        RECEIVER_EMAILS_KEY = "${receiver_emails}"

        action = EmailAction(
            name="my_action",
            smtp_address=SMPT_ADDRESS_KEY,
            smtp_port=SMPT_PORT_KEY,
            sender_login=SENDER_LOGIN_KEY,
            sender_alias=SENDER_ALIAS_KEY,
            sender_password=SENDER_PASSWORD_KEY,
            receiver_emails=RECEIVER_EMAILS_KEY,
        )

        config_from_uncommitted_config = {
            SMPT_ADDRESS_KEY: "something.com",
            SMPT_PORT_KEY: "123",
            SENDER_LOGIN_KEY: "sender@greatexpectations.io",
            SENDER_ALIAS_KEY: "alias@greatexpectations.io",
            SENDER_PASSWORD_KEY: "sender_password_login",
            RECEIVER_EMAILS_KEY: "foo@greatexpectations.io, bar@great_expectations.io",
        }

        config_provider.substitute_config.side_effect = lambda key: config_from_uncommitted_config[
            key
        ]
        with mock.patch.object(smtplib, "SMTP") as mock_server:
            action.run(checkpoint_result=checkpoint_result)

        mock_server().sendmail.assert_called_once_with(
            config_from_uncommitted_config[SENDER_ALIAS_KEY],
            [
                email.strip()
                for email in config_from_uncommitted_config[RECEIVER_EMAILS_KEY].split(",")
            ],
            mock.ANY,
        )


class TestMicrosoftTeamsNotificationAction:
    @pytest.mark.unit
    def test_run(self, checkpoint_result: CheckpointResult):
        action = MicrosoftTeamsNotificationAction(name="my_action", teams_webhook="test")

        with mock.patch.object(Session, "post") as mock_post:
            action.run(checkpoint_result=checkpoint_result)

        mock_post.assert_called_once()

        body = mock_post.call_args.kwargs["json"]["attachments"][0]["content"]["body"]

        assert len(body) == 5

        # Assert header
        assert "Success" in body[0]["columns"][1]["items"][0]["text"]

        # Assert first validation
        assert body[1]["text"] == "Validation Result (1 of 2) ✅"
        assert body[2]["facts"] == [
            {"title": "Data Asset name: ", "value": "--"},
            {"title": "Suite name: ", "value": SUITE_A},
            {
                "title": "Run name: ",
                "value": "prod_20240401",
            },
            {
                "title": "Summary:",
                "value": "*3* of *3* Expectations were met",
            },
        ]

        # Assert second validation
        assert body[3]["text"] == "Validation Result (2 of 2) ✅"
        assert body[4]["facts"] == [
            {"title": "Data Asset name: ", "value": "--"},
            {"title": "Suite name: ", "value": SUITE_B},
            {
                "title": "Run name: ",
                "value": "prod_20240402",
            },
            {
                "title": "Summary:",
                "value": "*2* of *2* Expectations were met",
            },
        ]

    @pytest.mark.unit
    def test_run_webhook_substitution(self, checkpoint_result: CheckpointResult):
        config_provider = project_manager.get_config_provider()
        assert isinstance(config_provider, mock.Mock)  # noqa: TID251 # just using for the instance compare

        MS_TEAMS_WEBHOOK_VAR = "${ms_teams_webhook}"
        MS_TEAMS_WEBHOOK_VALUE = "https://my_org.webhook.office.com/webhookb2/abc"

        action = MicrosoftTeamsNotificationAction(
            name="my_action",
            teams_webhook=MS_TEAMS_WEBHOOK_VAR,
        )

        config_from_uncommitted_config = {MS_TEAMS_WEBHOOK_VAR: MS_TEAMS_WEBHOOK_VALUE}

        config_provider.substitute_config.side_effect = lambda key: config_from_uncommitted_config[
            key
        ]
        with mock.patch.object(Session, "post") as mock_send_notification:
            action.run(checkpoint_result=checkpoint_result)

        mock_send_notification.assert_called_once_with(url=MS_TEAMS_WEBHOOK_VALUE, json=mock.ANY)

    @pytest.mark.skip(reason="CI TRANSITION")
    @pytest.mark.integration
    @pytest.mark.parametrize(
        "notify_on, expected_notification",
        [
            ("all", True),
            ("success", True),
            ("failure", False),
            ("critical", False),
            ("warning", False),
            ("info", False),
        ],
    )
    def test_run_integration_success_with_severity_filtering(
        self,
        notify_on: NotifyOn,
        expected_notification: bool,
        checkpoint_result: CheckpointResult,
    ):
        """
        Test that notify_on filtering works with successful checkpoint results.
        For this test, we are using a successful checkpoint result, so we expect
        a notification for the "all" and "success" cases only.
        """
        # Necessary to retrieve config provider
        gx.get_context(mode="ephemeral")

        action = MicrosoftTeamsNotificationAction(
            name="test-action",
            teams_webhook="${GX_MS_TEAMS_WEBHOOK}",  # Set as a secret in GH Actions
            notify_on=notify_on,
        )

        result = action.run(checkpoint_result=checkpoint_result)

        if expected_notification:
            assert result == {
                "microsoft_teams_notification_result": "Microsoft Teams notification succeeded."
            }
        else:
            assert result == {"microsoft_teams_notification_result": None}

    @pytest.mark.skip(reason="CI TRANSITION")
    @pytest.mark.integration
    @pytest.mark.parametrize(
        "notify_on, expected_notification",
        [
            ("all", True),
            ("success", False),
            ("failure", True),
            ("critical", False),
            ("warning", True),
            ("info", False),
        ],
    )
    def test_run_integration_failure_with_severity_filtering(
        self,
        notify_on: NotifyOn,
        expected_notification: bool,
        checkpoint_result_with_failure: CheckpointResult,
    ):
        """
        Test that notify_on filtering works with failed checkpoint results.
        For this test, we are using a failed checkpoint result with WARNING-level
        severity, so we expect a notification for the "all", "failure" and "warning"
        cases only.
        """
        # Necessary to retrieve config provider
        gx.get_context(mode="ephemeral")

        action = MicrosoftTeamsNotificationAction(
            name="test-action",
            teams_webhook="${GX_MS_TEAMS_WEBHOOK}",  # Set as a secret in GH Actions
            notify_on=notify_on,
        )

        result = action.run(checkpoint_result=checkpoint_result_with_failure)

        if expected_notification:
            assert result == {
                "microsoft_teams_notification_result": "Microsoft Teams notification succeeded."
            }
        else:
            assert result == {"microsoft_teams_notification_result": None}

    @pytest.mark.integration
    def test_run_integration_failure(
        self,
        checkpoint_result: CheckpointResult,
        caplog,
    ):
        # Necessary to retrieve config provider
        gx.get_context(mode="ephemeral")

        action = MicrosoftTeamsNotificationAction(
            name="test-action",
            teams_webhook="https://fake.office.com/fake",
        )
        with caplog.at_level(logging.WARNING):
            result = action.run(checkpoint_result=checkpoint_result)

        assert result == {"microsoft_teams_notification_result": None}
        assert caplog.records[-1].message.startswith("Failed to connect to Microsoft Teams webhook")


class TestOpsgenieAlertAction:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "success, message",
        [
            pytest.param(True, "succeeded!", id="success"),
            pytest.param(False, "failed!", id="failure"),
        ],
    )
    def test_run(self, checkpoint_result: CheckpointResult, success: bool, message: str):
        action = OpsgenieAlertAction(name="my_action", api_key="test", notify_on="all")
        checkpoint_result.success = success

        with mock.patch.object(Session, "post") as mock_post:
            output = action.run(checkpoint_result=checkpoint_result)

        mock_post.assert_called_once()
        assert message in mock_post.call_args.kwargs["json"]["message"]
        assert output == {"opsgenie_alert_result": True}


class TestPagerdutyAlertAction:
    @pytest.mark.unit
    def test_run_emits_events(self, checkpoint_result: CheckpointResult, mocker: MockerFixture):
        from great_expectations.checkpoint import actions

        with mock_not_imported_module(actions, "pypd", mocker):
            mock_pypd_event = actions.pypd.EventV2.create
            action = PagerdutyAlertAction(
                name="my_action", api_key="test", routing_key="test", notify_on="all"
            )
            checkpoint_name = checkpoint_result.checkpoint_config.name

            checkpoint_result.success = True
            assert action.run(checkpoint_result=checkpoint_result) == {
                "pagerduty_alert_result": "success"
            }

            checkpoint_result.success = False
            assert action.run(checkpoint_result=checkpoint_result) == {
                "pagerduty_alert_result": "success"
            }

            assert mock_pypd_event.call_count == 2
            mock_pypd_event.assert_has_calls(
                [
                    mock.call(
                        data={
                            "dedup_key": checkpoint_name,
                            "event_action": "trigger",
                            "payload": {
                                "severity": "critical",
                                "source": "Great Expectations",
                                "summary": f"Great Expectations Checkpoint {checkpoint_name} has succeeded",  # noqa: E501 # FIXME CoP
                            },
                            "routing_key": "test",
                        }
                    ),
                    mock.call(
                        data={
                            "dedup_key": checkpoint_name,
                            "event_action": "trigger",
                            "payload": {
                                "severity": "critical",
                                "source": "Great Expectations",
                                "summary": f"Great Expectations Checkpoint {checkpoint_name} has failed",  # noqa: E501 # FIXME CoP
                            },
                            "routing_key": "test",
                        }
                    ),
                ]
            )

    @pytest.mark.skipif(
        not is_library_loadable(library_name="pypd"),
        reason="pypd is not installed",
    )
    @mock.patch("pypd.EventV2.create")
    @pytest.mark.unit
    def test_run_does_not_emit_events(self, mock_pypd_event, checkpoint_result: CheckpointResult):
        action = PagerdutyAlertAction(
            name="my_action", api_key="test", routing_key="test", notify_on="failure"
        )

        checkpoint_result.success = True
        assert action.run(checkpoint_result=checkpoint_result) == {
            "pagerduty_alert_result": "none sent"
        }

        mock_pypd_event.assert_not_called()


class TestSlackNotificationAction:
    @pytest.mark.unit
    def test_equality(self):
        """I kow, this one seems silly. But this was a bug."""
        a = SlackNotificationAction(name="my_action", slack_webhook="test", notify_on="all")
        b = SlackNotificationAction(name="my_action", slack_webhook="test", notify_on="all")

        assert a == b

    @pytest.mark.unit
    def test_run(self, checkpoint_result: CheckpointResult):
        action = SlackNotificationAction(name="my_action", slack_webhook="test", notify_on="all")

        with mock.patch.object(Session, "post") as mock_post:
            output = action.run(checkpoint_result=checkpoint_result)

        mock_post.assert_called_once_with(
            url="test",
            headers=None,
            json={
                "blocks": [
                    {"text": {"text": mock.ANY, "type": "plain_text"}, "type": "header"},
                    {
                        "type": "section",
                        "text": {"type": "plain_text", "text": "Runtime: 2024/04/01 08:51 PM"},
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": (
                                "\n*Asset*: `__no_data_asset_name__`  "
                                f"\n*Expectation Suite*: `{SUITE_A}`"
                                "\n*Summary*: *3* of *3* Expectations were met"
                            ),
                        },
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": (
                                "\n*Asset*: `__no_data_asset_name__`  "
                                f"\n*Expectation Suite*: `{SUITE_B}`"
                                "\n*Summary*: *2* of *2* Expectations were met"
                            ),
                        },
                    },
                    {"type": "divider"},
                ],
            },
        )

        assert output == {"slack_notification_result": "Slack notification succeeded."}

    @pytest.mark.unit
    def test_run_with_assets(self, checkpoint_result_with_assets: CheckpointResult):
        action = SlackNotificationAction(name="my_action", slack_webhook="test", notify_on="all")

        with mock.patch.object(Session, "post") as mock_post:
            output = action.run(checkpoint_result=checkpoint_result_with_assets)

        mock_post.assert_called_once_with(
            url="test",
            headers=None,
            json={
                "blocks": [
                    {"text": {"text": mock.ANY, "type": "plain_text"}, "type": "header"},
                    {
                        "type": "section",
                        "text": {"type": "plain_text", "text": "Runtime: 2024/04/01 08:51 PM"},
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": (
                                f"\n*Asset*: `asset_1`  \n*Expectation Suite*: {SUITE_A}  "
                                "<www.testing?slack=true|View Results>"
                                "\n*Summary*: *3* of *3* Expectations were met"
                            ),
                        },
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": (
                                "\n*Asset*: `asset_2_two_wow_whoa_vroom`  "
                                f"\n*Expectation Suite*: `{SUITE_B}`"
                                "\n*Summary*: *2* of *2* Expectations were met"
                            ),
                        },
                    },
                    {"type": "divider"},
                ],
            },
        )

        assert output == {"slack_notification_result": "Slack notification succeeded."}

    @pytest.mark.unit
    def test_grabs_data_docs_pages(self, checkpoint_result_with_assets: CheckpointResult):
        action = SlackNotificationAction(name="my_action", slack_webhook="test", notify_on="all")

        site_path = "file:///var/folders/vm/wkw13lnd5vsdh3hjmcv9tym00000gn/T/tmpctw4x7yu/validations/my_suite/__none__/20240910T175850.906745Z/foo-bar.html"
        action_context = ActionContext()
        action_context.update(
            action=UpdateDataDocsAction(name="docs_action"),
            action_result={
                ValidationResultIdentifier(
                    expectation_suite_identifier=ExpectationSuiteIdentifier(name="my_suite"),
                    run_id=RunIdentifier(run_name="prod_20240401"),
                    batch_identifier="my_datasource-my_first_asset",
                ): {
                    "local_site": site_path,
                }
            },
        )
        with mock.patch.object(Session, "post") as mock_post:
            output = action.run(
                checkpoint_result=checkpoint_result_with_assets, action_context=action_context
            )

        mock_post.assert_called_once_with(
            url="test",
            headers=None,
            json={
                "blocks": [
                    {"text": {"text": mock.ANY, "type": "plain_text"}, "type": "header"},
                    {
                        "type": "section",
                        "text": {"type": "plain_text", "text": "Runtime: 2024/04/01 08:51 PM"},
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": (
                                f"\n*Asset*: `asset_1`  \n*Expectation Suite*: {SUITE_A}  "
                                "<www.testing?slack=true|View Results>"
                                "\n*Summary*: *3* of *3* Expectations were met"
                            ),
                        },
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": mock.ANY,
                        },
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": (
                                "\n*Asset*: `asset_2_two_wow_whoa_vroom`  "
                                f"\n*Expectation Suite*: `{SUITE_B}`"
                                "\n*Summary*: *2* of *2* Expectations were met"
                            ),
                        },
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": mock.ANY,
                        },
                    },
                    {"type": "divider"},
                ],
            },
        )

        docs_block_1 = mock_post.call_args.kwargs["json"]["blocks"][3]["text"]["text"]
        docs_block_2 = mock_post.call_args.kwargs["json"]["blocks"][5]["text"]["text"]

        assert "*DataDocs*" in docs_block_1
        assert site_path in docs_block_1
        assert "*DataDocs*" in docs_block_2
        assert site_path in docs_block_2
        assert output == {"slack_notification_result": "Slack notification succeeded."}

    @pytest.mark.unit
    def test_variable_substitution_webhook(self, mock_context, checkpoint_result):
        action = SlackNotificationAction(name="my_action", slack_webhook="${SLACK_WEBHOOK}")

        with mock.patch.object(Session, "post"):
            action.run(checkpoint_result)

        mock_context.config_provider.substitute_config.assert_called_once_with("${SLACK_WEBHOOK}")

    @pytest.mark.unit
    def test_variable_substitution_token_and_channel(self, mock_context, checkpoint_result):
        action = SlackNotificationAction(
            name="my_action", slack_token="${SLACK_TOKEN}", slack_channel="${SLACK_CHANNEL}"
        )

        with mock.patch.object(Session, "post"):
            action.run(checkpoint_result)

        assert mock_context.config_provider.substitute_config.call_count == 2
        mock_context.config_provider.substitute_config.assert_any_call("${SLACK_CHANNEL}")
        mock_context.config_provider.substitute_config.assert_any_call("${SLACK_TOKEN}")


class TestSNSNotificationAction:
    @pytest.mark.unit
    def test_run(self, sns, checkpoint_result: CheckpointResult):
        subj_topic = "test-subj"
        created_subj = sns.create_topic(Name=subj_topic)
        arn = created_subj.get("TopicArn")
        action = SNSNotificationAction(
            name="my_action",
            sns_topic_arn=arn,
            sns_message_subject="Subject",
        )

        result = action.run(checkpoint_result=checkpoint_result)
        assert "Successfully posted results" in result["result"]


class TestUpdateDataDocsAction:
    @pytest.mark.unit
    def test_equality(self):
        """I kow, this one seems silly. But this was a bug for other actions."""
        a = UpdateDataDocsAction(name="my_action")
        b = UpdateDataDocsAction(name="my_action")

        assert a == b

    @pytest.mark.unit
    def test_run(self, mocker: MockerFixture, checkpoint_result: CheckpointResult):
        # Arrange
        context = mocker.Mock(spec=AbstractDataContext)
        set_context(context)

        site_names = ["site_a", "site_b"]
        site_urls = [
            f"/gx/uncommitted/data_docs/{site_names[0]}/index.html",
            f"/gx/uncommitted/data_docs/{site_names[1]}/index.html",
        ]
        context.get_docs_sites_urls.return_value = [
            {
                "site_url": site_urls[0],
                "site_name": site_names[0],
            },
            {
                "site_url": site_urls[1],
                "site_name": site_names[1],
            },
        ]

        # Act
        action = UpdateDataDocsAction(name="my_action", site_names=site_names)
        res = action.run(checkpoint_result=checkpoint_result)

        # Assert
        validation_identifier_a, validation_identifier_b = tuple(
            checkpoint_result.run_results.keys()
        )
        assert context.build_data_docs.call_count == 2, (
            "Data Docs should be incrementally built (once per validation result)"
        )
        context.build_data_docs.assert_has_calls(
            [
                mock.call(
                    build_index=True,
                    dry_run=False,
                    resource_identifiers=[
                        validation_identifier_a,
                        ExpectationSuiteIdentifier(name=SUITE_A),
                    ],
                    site_names=site_names,
                ),
                mock.call(
                    build_index=True,
                    dry_run=False,
                    resource_identifiers=[
                        validation_identifier_b,
                        ExpectationSuiteIdentifier(name=SUITE_B),
                    ],
                    site_names=site_names,
                ),
            ]
        )
        assert res == {
            validation_identifier_a: {
                site_names[0]: site_urls[0],
                site_names[1]: site_urls[1],
            },
            validation_identifier_b: {
                site_names[0]: site_urls[0],
                site_names[1]: site_urls[1],
            },
        }

    @pytest.mark.cloud
    def test_run_with_cloud(self, mocker: MockerFixture, checkpoint_result: CheckpointResult):
        # Arrange
        context = mocker.Mock(spec=CloudDataContext)
        set_context(context)

        site_names = ["site_a", "site_b"]
        site_urls = [
            f"http://app.greatexpectations.io/data_docs/{site_names[0]}",
            f"http://app.greatexpectations.io/data_docs/{site_names[1]}",
        ]
        context.get_docs_sites_urls.return_value = [
            {
                "site_url": site_urls[0],
                "site_name": site_names[0],
            },
            {
                "site_url": site_urls[1],
                "site_name": site_names[1],
            },
        ]

        # Act
        action = UpdateDataDocsAction(name="my_docs_action", site_names=site_names)
        res = action.run(checkpoint_result=checkpoint_result)

        # Assert
        validation_identifier_a, validation_identifier_b = tuple(
            checkpoint_result.run_results.keys()
        )
        assert context.build_data_docs.call_count == 2, (
            "Data Docs should be incrementally built (once per validation result)"
        )
        context.build_data_docs.assert_has_calls(
            [
                mock.call(
                    build_index=True,
                    dry_run=False,
                    resource_identifiers=[
                        validation_identifier_a,
                        GXCloudIdentifier(
                            resource_type=GXCloudRESTResource.EXPECTATION_SUITE,
                            resource_name=SUITE_A,
                        ),
                    ],
                    site_names=site_names,
                ),
                mock.call(
                    build_index=True,
                    dry_run=False,
                    resource_identifiers=[
                        validation_identifier_b,
                        GXCloudIdentifier(
                            resource_type=GXCloudRESTResource.EXPECTATION_SUITE,
                            resource_name=SUITE_B,
                        ),
                    ],
                    site_names=site_names,
                ),
            ]
        )
        assert res == {
            validation_identifier_a: {},
            validation_identifier_b: {},
        }


class TestCustomActions:
    @pytest.mark.unit
    def test_custom_action_shadows_existing_type(self):
        with pytest.raises(ValidationActionAlreadyRegisteredError):

            class CustomSlackAction(ValidationAction):
                type: Literal["slack"] = "slack"  # Shadows existing value


class TestShouldNotify:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "success, notify_on, expected",
        [
            pytest.param(True, "all", True, id="all_success"),
            pytest.param(False, "all", True, id="all_failure"),
        ],
    )
    def test_all_always_true(self, success: bool, notify_on: NotifyOn, expected: bool):
        assert should_notify(success=success, notify_on=notify_on) is expected

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "success, expected",
        [
            pytest.param(True, True, id="success_true"),
            pytest.param(False, False, id="success_false"),
        ],
    )
    def test_success_mode(self, success: bool, expected: bool):
        assert should_notify(success=success, notify_on="success") is expected

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "success, expected",
        [
            pytest.param(True, False, id="failure_mode_success_true"),
            pytest.param(False, True, id="failure_mode_success_false"),
        ],
    )
    def test_failure_mode(self, success: bool, expected: bool):
        assert should_notify(success=success, notify_on="failure") is expected

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "notify_on",
        [
            pytest.param("info", id="info"),
            pytest.param("warning", id="warning"),
            pytest.param("critical", id="critical"),
        ],
    )
    def test_severity_modes_do_not_notify_on_success(self, notify_on: NotifyOn):
        assert should_notify(success=True, notify_on=notify_on) is False

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "notify_on, max_severity, expected",
        [
            # default max_severity is "critical"
            pytest.param("critical", "critical", True, id="default_max_critical"),
            pytest.param("warning", "critical", False, id="default_max_warning"),
            pytest.param("info", "critical", False, id="default_max_info"),
            # custom max_severity = warning
            pytest.param("warning", "warning", True, id="max_warning_match"),
            pytest.param("critical", "warning", False, id="max_warning_critical"),
            pytest.param("info", "warning", False, id="max_warning_info"),
            # custom max_severity = info
            pytest.param("info", "info", True, id="max_info_match"),
            pytest.param("warning", "info", False, id="max_info_warning"),
            pytest.param("critical", "info", False, id="max_info_critical"),
        ],
    )
    def test_severity_modes_on_failure(
        self, notify_on: NotifyOn, max_severity: FailureSeverity, expected: bool
    ):
        assert (
            should_notify(success=False, notify_on=notify_on, max_severity=max_severity) is expected
        )

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "notify_on",
        [
            pytest.param("info", id="info"),
            pytest.param("warning", id="warning"),
            pytest.param("critical", id="critical"),
        ],
    )
    def test_severity_modes_on_failure_with_none_max(self, notify_on: NotifyOn):
        assert should_notify(success=False, notify_on=notify_on, max_severity=None) is False
