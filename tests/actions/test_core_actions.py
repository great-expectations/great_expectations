from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from types import ModuleType
from typing import TYPE_CHECKING, Iterator, Type
from unittest import mock

import pytest
import requests
from requests import Session

from great_expectations.checkpoint.actions import (
    ActionContext,
    APINotificationAction,
    EmailAction,
    MicrosoftTeamsNotificationAction,
    OpsgenieAlertAction,
    PagerdutyAlertAction,
    SlackNotificationAction,
    SNSNotificationAction,
    UpdateDataDocsAction,
    ValidationAction,
)
from great_expectations.checkpoint.checkpoint import Checkpoint, CheckpointResult
from great_expectations.core.batch import IDDict, LegacyBatchDefinition
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.data_context.cloud_data_context import CloudDataContext
from great_expectations.data_context.data_context.context_factory import set_context
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    GXCloudIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.util import is_library_loadable

if TYPE_CHECKING:
    from pytest_mock import MockerFixture
    from typing_extensions import Never

logger = logging.getLogger(__name__)


@pytest.fixture
def mock_context(mocker: MockerFixture):
    context = mocker.MagicMock(spec=AbstractDataContext)
    set_context(context)
    return context


class MockTeamsResponse:
    def __init__(self, status_code: int, raise_for_status: bool = False):
        self.status_code = status_code
        self._raise_for_status = raise_for_status
        self.text = "test_text"

    def raise_for_status(self):
        if self._raise_for_status:
            raise requests.exceptions.HTTPError("test")


class MockSlackResponse:
    def __init__(self, status_code):
        self.status_code = status_code
        self.text = "ok"
        self.content = json.dumps({"ok": "True"})

    def json(self):
        return {"ok": "True"}

    def raise_for_status(self):
        pass


class MockCloudResponse:
    def __init__(self, status_code):
        self.status_code = status_code
        self.text = "test_text"
        self.content = json.dumps({"ok": "True"})


@pytest.mark.unit
def test_api_action_create_payload(mock_context):
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


class TestActionSerialization:
    EXAMPLE_SLACK_WEBHOOK = "https://hooks.slack.com/services/test/slack/webhook"
    EXAMPLE_TEAMS_WEBHOOK = "https://hooks.microsoft.com/services/test/teams/webhook"
    EXAMPLE_API_KEY = "testapikey"
    EXAMPLE_SMTP_ADDRESS = "smtp.test.com"
    EXAMPLE_SMTP_PORT = 587
    EXAMPLE_EMAILS = "bob@gmail.com, jim@hotmail.com"
    EXAMPLE_SITE_NAMES = ["one_site", "two_site", "red_site", "blue_site"]
    EXAMPLE_SNS_TOPIC_ARN = "my_test_arn"
    EXAMPLE_URL = "http://www.example.com"

    ACTION_INIT_PARAMS = {
        SlackNotificationAction: {
            "name": "my_slack_action",
            "slack_webhook": EXAMPLE_SLACK_WEBHOOK,
        },
        MicrosoftTeamsNotificationAction: {
            "name": "my_teams_action",
            "teams_webhook": EXAMPLE_TEAMS_WEBHOOK,
        },
        OpsgenieAlertAction: {"name": "my_opsgenie_action", "api_key": EXAMPLE_API_KEY},
        EmailAction: {
            "name": "my_email_action",
            "smtp_address": EXAMPLE_SMTP_ADDRESS,
            "smtp_port": EXAMPLE_SMTP_PORT,
            "receiver_emails": EXAMPLE_EMAILS,
        },
        UpdateDataDocsAction: {"name": "my_data_docs_action", "site_names": EXAMPLE_SITE_NAMES},
        SNSNotificationAction: {"name": "my_sns_action", "sns_topic_arn": EXAMPLE_SNS_TOPIC_ARN},
        APINotificationAction: {"name": "my_api_action", "url": EXAMPLE_URL},
    }

    SERIALIZED_ACTIONS = {
        SlackNotificationAction: {
            "name": "my_slack_action",
            "notify_on": "all",
            "notify_with": None,
            "renderer": {
                "class_name": "SlackRenderer",
                "module_name": "great_expectations.render.renderer.slack_renderer",
            },
            "show_failed_expectations": False,
            "slack_channel": None,
            "slack_token": None,
            "slack_webhook": EXAMPLE_SLACK_WEBHOOK,
            "type": "slack",
        },
        MicrosoftTeamsNotificationAction: {
            "name": "my_teams_action",
            "notify_on": "all",
            "renderer": {
                "class_name": "MicrosoftTeamsRenderer",
                "module_name": "great_expectations.render.renderer.microsoft_teams_renderer",
            },
            "teams_webhook": EXAMPLE_TEAMS_WEBHOOK,
            "type": "microsoft",
        },
        OpsgenieAlertAction: {
            "name": "my_opsgenie_action",
            "api_key": EXAMPLE_API_KEY,
            "notify_on": "failure",
            "priority": "P3",
            "region": None,
            "renderer": {
                "class_name": "OpsgenieRenderer",
                "module_name": "great_expectations.render.renderer.opsgenie_renderer",
            },
            "tags": None,
            "type": "opsgenie",
        },
        EmailAction: {
            "name": "my_email_action",
            "notify_on": "all",
            "notify_with": None,
            "receiver_emails": EXAMPLE_EMAILS,
            "renderer": {
                "class_name": "EmailRenderer",
                "module_name": "great_expectations.render.renderer.email_renderer",
            },
            "sender_alias": None,
            "sender_login": None,
            "sender_password": None,
            "smtp_address": EXAMPLE_SMTP_ADDRESS,
            "smtp_port": str(EXAMPLE_SMTP_PORT),
            "type": "email",
            "use_ssl": None,
            "use_tls": None,
        },
        UpdateDataDocsAction: {
            "name": "my_data_docs_action",
            "site_names": EXAMPLE_SITE_NAMES,
            "type": "update_data_docs",
        },
        SNSNotificationAction: {
            "name": "my_sns_action",
            "sns_message_subject": None,
            "sns_topic_arn": EXAMPLE_SNS_TOPIC_ARN,
            "type": "sns",
        },
        APINotificationAction: {
            "name": "my_api_action",
            "type": "api",
            "url": EXAMPLE_URL,
        },
    }

    @pytest.mark.parametrize(
        "action_class, init_params",
        [(k, v) for k, v in ACTION_INIT_PARAMS.items()],
        ids=[k.__name__ for k in ACTION_INIT_PARAMS],
    )
    @pytest.mark.unit
    def test_action_serialization_and_deserialization(
        self,
        mock_context,
        action_class: Type[ValidationAction],
        init_params: dict,
    ):
        expected = self.SERIALIZED_ACTIONS[action_class]

        action = action_class(**init_params)
        json_dict = action.json()
        actual = json.loads(json_dict)

        assert actual == expected

    @pytest.mark.parametrize(
        "action_class, serialized_action",
        [(k, v) for k, v in SERIALIZED_ACTIONS.items()],
        ids=[k.__name__ for k in SERIALIZED_ACTIONS],
    )
    @pytest.mark.unit
    def test_action_deserialization(
        self, action_class: Type[ValidationAction], serialized_action: dict
    ):
        actual = action_class.parse_obj(serialized_action)
        assert isinstance(actual, action_class)


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


class TestV1ActionRun:
    suite_a: str = "suite_a"
    suite_b: str = "suite_b"
    batch_id_a: str = "my_datasource-my_first_asset"
    batch_id_b: str = "my_datasource-my_second_asset"

    @pytest.fixture
    def checkpoint_result(self, mocker: MockerFixture):
        utc_datetime = datetime.fromisoformat("2024-04-01T20:51:18.077262").replace(
            tzinfo=timezone.utc
        )
        return CheckpointResult(
            run_id=RunIdentifier(run_time=utc_datetime),
            run_results={
                ValidationResultIdentifier(
                    expectation_suite_identifier=ExpectationSuiteIdentifier(
                        name=self.suite_a,
                    ),
                    run_id=RunIdentifier(run_name="prod_20240401"),
                    batch_identifier=self.batch_id_a,
                ): ExpectationSuiteValidationResult(
                    success=True,
                    statistics={"successful_expectations": 3, "evaluated_expectations": 3},
                    results=[],
                    suite_name=self.suite_a,
                ),
                ValidationResultIdentifier(
                    expectation_suite_identifier=ExpectationSuiteIdentifier(
                        name=self.suite_b,
                    ),
                    run_id=RunIdentifier(run_name="prod_20240402"),
                    batch_identifier=self.batch_id_b,
                ): ExpectationSuiteValidationResult(
                    success=True,
                    statistics={"successful_expectations": 2, "evaluated_expectations": 2},
                    results=[],
                    suite_name=self.suite_b,
                ),
            },
            checkpoint_config=mocker.Mock(spec=Checkpoint, name="my_checkpoint"),
        )

    @pytest.fixture
    def checkpoint_result_with_assets(self, mocker: MockerFixture):
        utc_datetime = datetime.fromisoformat("2024-04-01T20:51:18.077262").replace(
            tzinfo=timezone.utc
        )
        return CheckpointResult(
            run_id=RunIdentifier(run_time=utc_datetime),
            run_results={
                ValidationResultIdentifier(
                    expectation_suite_identifier=ExpectationSuiteIdentifier(
                        name=self.suite_a,
                    ),
                    run_id=RunIdentifier(run_name="prod_20240401"),
                    batch_identifier=self.batch_id_a,
                ): ExpectationSuiteValidationResult(
                    success=True,
                    statistics={"successful_expectations": 3, "evaluated_expectations": 3},
                    results=[],
                    suite_name=self.suite_a,
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
                        name=self.suite_b,
                    ),
                    run_id=RunIdentifier(run_name="prod_20240402"),
                    batch_identifier=self.batch_id_b,
                ): ExpectationSuiteValidationResult(
                    success=True,
                    statistics={"successful_expectations": 2, "evaluated_expectations": 2},
                    results=[],
                    suite_name=self.suite_b,
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

    @pytest.mark.unit
    def test_APINotificationAction_run(self, checkpoint_result: CheckpointResult):
        url = "http://www.example.com"
        action = APINotificationAction(name="my_action", url=url)

        with mock.patch.object(requests, "post") as mock_post:
            action.run(checkpoint_result=checkpoint_result)

        mock_post.assert_called_once_with(
            url,
            headers={"Content-Type": "application/json"},
            data=[
                {
                    "data_asset_name": self.batch_id_a,
                    "test_suite_name": self.suite_a,
                    "validation_results": [],
                },
                {
                    "data_asset_name": self.batch_id_b,
                    "test_suite_name": self.suite_b,
                    "validation_results": [],
                },
            ],
        )

    @pytest.mark.unit
    def test_EmailAction_equality(self):
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
    def test_EmailAction_run(
        self, checkpoint_result: CheckpointResult, emails: str, expected_email_list: list[str]
    ):
        action = EmailAction(
            name="my_action",
            smtp_address="test",
            smtp_port="587",
            receiver_emails=emails,
        )

        with mock.patch("great_expectations.checkpoint.actions.send_email") as mock_send_email:
            out = action.run(checkpoint_result=checkpoint_result)

        # Should contain success/failure in title
        assert "True" in mock_send_email.call_args.kwargs["title"]

        # Should contain suite names and other relevant domain object identifiers in the body
        run_results = tuple(checkpoint_result.run_results.values())
        suite_a = run_results[0].suite_name
        suite_b = run_results[1].suite_name
        mock_html = mock_send_email.call_args.kwargs["html"]
        assert suite_a in mock_html and suite_b in mock_html

        mock_send_email.assert_called_once_with(
            title=mock.ANY,
            html=mock.ANY,
            receiver_emails_list=expected_email_list,
            sender_alias=None,
            sender_login=None,
            sender_password=None,
            smtp_address="test",
            smtp_port="587",
            use_ssl=None,
            use_tls=None,
        )
        assert out == {"email_result": mock_send_email()}

    @pytest.mark.unit
    def test_MicrosoftTeamsNotificationAction_run(self, checkpoint_result: CheckpointResult):
        action = MicrosoftTeamsNotificationAction(name="my_action", teams_webhook="test")

        with mock.patch.object(Session, "post") as mock_post:
            action.run(checkpoint_result=checkpoint_result)

        mock_post.assert_called_once()

        body = mock_post.call_args.kwargs["json"]["attachments"][0]["content"]["body"]
        checkpoint_summary = body[0]["items"][0]["columns"][0]["items"][0]["text"]
        first_validation = body[1]["items"][0]["text"]
        second_validation = body[2]["items"][0]["text"]

        assert len(body) == 3
        assert "Success !!!" in checkpoint_summary
        assert first_validation == [
            {
                "color": "good",
                "horizontalAlignment": "left",
                "text": "**Batch Validation Status:** Success !!!",
                "type": "TextBlock",
            },
            {
                "horizontalAlignment": "left",
                "text": "**Data Asset Name:** __no_data_asset_name__",
                "type": "TextBlock",
            },
            {
                "horizontalAlignment": "left",
                "text": "**Expectation Suite Name:** suite_a",
                "type": "TextBlock",
            },
            {
                "horizontalAlignment": "left",
                "text": "**Run Name:** prod_20240401",
                "type": "TextBlock",
            },
            {
                "horizontalAlignment": "left",
                "text": "**Batch ID:** None",
                "type": "TextBlock",
            },
            {
                "horizontalAlignment": "left",
                "text": "**Summary:** *3* of *3* expectations were met",
                "type": "TextBlock",
            },
        ]
        assert second_validation == [
            {
                "color": "good",
                "horizontalAlignment": "left",
                "text": "**Batch Validation Status:** Success !!!",
                "type": "TextBlock",
            },
            {
                "horizontalAlignment": "left",
                "text": "**Data Asset Name:** __no_data_asset_name__",
                "type": "TextBlock",
            },
            {
                "horizontalAlignment": "left",
                "text": "**Expectation Suite Name:** suite_b",
                "type": "TextBlock",
            },
            {
                "horizontalAlignment": "left",
                "text": "**Run Name:** prod_20240402",
                "type": "TextBlock",
            },
            {
                "horizontalAlignment": "left",
                "text": "**Batch ID:** None",
                "type": "TextBlock",
            },
            {
                "horizontalAlignment": "left",
                "text": "**Summary:** *2* of *2* expectations were met",
                "type": "TextBlock",
            },
        ]

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "success, message",
        [
            pytest.param(True, "succeeded!", id="success"),
            pytest.param(False, "failed!", id="failure"),
        ],
    )
    def test_OpsgenieAlertAction_run(
        self, checkpoint_result: CheckpointResult, success: bool, message: str
    ):
        action = OpsgenieAlertAction(name="my_action", api_key="test", notify_on="all")
        checkpoint_result.success = success

        with mock.patch.object(Session, "post") as mock_post:
            output = action.run(checkpoint_result=checkpoint_result)

        mock_post.assert_called_once()
        assert message in mock_post.call_args.kwargs["json"]["message"]
        assert output == {"opsgenie_alert_result": True}

    @pytest.mark.unit
    def test_PagerdutyAlertAction_run_emits_events(
        self, checkpoint_result: CheckpointResult, mocker: MockerFixture
    ):
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
                                "summary": f"Great Expectations Checkpoint {checkpoint_name} has succeeded",  # noqa: E501
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
                                "summary": f"Great Expectations Checkpoint {checkpoint_name} has failed",  # noqa: E501
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
    def test_PagerdutyAlertAction_run_does_not_emit_events(
        self, mock_pypd_event, checkpoint_result: CheckpointResult
    ):
        action = PagerdutyAlertAction(
            name="my_action", api_key="test", routing_key="test", notify_on="failure"
        )

        checkpoint_result.success = True
        assert action.run(checkpoint_result=checkpoint_result) == {
            "pagerduty_alert_result": "none sent"
        }

        mock_pypd_event.assert_not_called()

    @pytest.mark.unit
    def test_SlackNotificationAction_equality(self):
        """I kow, this one seems silly. But this was a bug."""
        a = SlackNotificationAction(name="my_action", slack_webhook="test", notify_on="all")
        b = SlackNotificationAction(name="my_action", slack_webhook="test", notify_on="all")

        assert a == b

    @pytest.mark.unit
    def test_SlackNotificationAction_run(self, checkpoint_result: CheckpointResult):
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
                            "text": "*Asset*: __no_data_asset_name__  *Expectation Suite*: suite_a",
                        },
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*Asset*: __no_data_asset_name__  *Expectation Suite*: suite_b",
                        },
                    },
                    {"type": "divider"},
                ],
            },
        )

        assert output == {"slack_notification_result": "Slack notification succeeded."}

    @pytest.mark.unit
    def test_SlackNotificationAction_run_with_assets(
        self, checkpoint_result_with_assets: CheckpointResult
    ):
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
                            "text": "*Asset*: asset_1  *Expectation Suite*: suite_a  "
                            "<www.testing?slack=true|View Results>",
                        },
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*Asset*: asset_2_two_wow_whoa_vroom  "
                            "*Expectation Suite*: suite_b",
                        },
                    },
                    {"type": "divider"},
                ],
            },
        )

        assert output == {"slack_notification_result": "Slack notification succeeded."}

    @pytest.mark.unit
    def test_SlackNotificationAction_grabs_data_docs_pages(
        self, checkpoint_result_with_assets: CheckpointResult
    ):
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
                            "text": "*Asset*: asset_1  *Expectation Suite*: suite_a  "
                            "<www.testing?slack=true|View Results>",
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
                            "text": "*Asset*: asset_2_two_wow_whoa_vroom  "
                            "*Expectation Suite*: suite_b",
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
    def test_SNSNotificationAction_run(self, sns, checkpoint_result: CheckpointResult):
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

    @pytest.mark.unit
    def test_UpdateDataDocsAction_equality(self):
        """I kow, this one seems silly. But this was a bug for other actions."""
        a = UpdateDataDocsAction(name="my_action")
        b = UpdateDataDocsAction(name="my_action")

        assert a == b

    @pytest.mark.unit
    def test_UpdateDataDocsAction_run(
        self, mocker: MockerFixture, checkpoint_result: CheckpointResult
    ):
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
        assert (
            context.build_data_docs.call_count == 2
        ), "Data Docs should be incrementally built (once per validation result)"
        context.build_data_docs.assert_has_calls(
            [
                mock.call(
                    build_index=True,
                    dry_run=False,
                    resource_identifiers=[
                        validation_identifier_a,
                        ExpectationSuiteIdentifier(name=self.suite_a),
                    ],
                    site_names=site_names,
                ),
                mock.call(
                    build_index=True,
                    dry_run=False,
                    resource_identifiers=[
                        validation_identifier_b,
                        ExpectationSuiteIdentifier(name=self.suite_b),
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
    def test_UpdateDataDocsAction_run_cloud(
        self, mocker: MockerFixture, checkpoint_result: CheckpointResult
    ):
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
        assert (
            context.build_data_docs.call_count == 2
        ), "Data Docs should be incrementally built (once per validation result)"
        context.build_data_docs.assert_has_calls(
            [
                mock.call(
                    build_index=True,
                    dry_run=False,
                    resource_identifiers=[
                        validation_identifier_a,
                        GXCloudIdentifier(
                            resource_type=GXCloudRESTResource.EXPECTATION_SUITE,
                            resource_name=self.suite_a,
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
                            resource_name=self.suite_b,
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

    @pytest.mark.unit
    def test_SlackNotificationAction_variable_substitution_webhook(
        self, mock_context, checkpoint_result
    ):
        action = SlackNotificationAction(name="my_action", slack_webhook="${SLACK_WEBHOOK}")

        with mock.patch("great_expectations.checkpoint.actions.send_slack_notification"):
            action.run(checkpoint_result)

        mock_context.config_provider.substitute_config.assert_called_once_with("${SLACK_WEBHOOK}")

    @pytest.mark.unit
    def test_SlackNotificationAction_variable_substitution_token_and_channel(
        self, mock_context, checkpoint_result
    ):
        action = SlackNotificationAction(
            name="my_action", slack_token="${SLACK_TOKEN}", slack_channel="${SLACK_CHANNEL}"
        )

        with mock.patch("great_expectations.checkpoint.actions.send_slack_notification"):
            action.run(checkpoint_result)

        assert mock_context.config_provider.substitute_config.call_count == 2
        mock_context.config_provider.substitute_config.assert_any_call("${SLACK_CHANNEL}")
        mock_context.config_provider.substitute_config.assert_any_call("${SLACK_TOKEN}")
