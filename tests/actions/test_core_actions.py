import json
import logging
from unittest import mock
from urllib.parse import urljoin

import pytest
from freezegun import freeze_time
from requests import Session

from great_expectations.checkpoint.actions import SNSNotificationAction
from great_expectations.checkpoint.util import smtplib
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_context.store import ValidationsStore
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.util import is_library_loadable
from great_expectations.validation_operators import (
    CloudNotificationAction,
    EmailAction,
    MicrosoftTeamsNotificationAction,
    OpsgenieAlertAction,
    PagerdutyAlertAction,
    SlackNotificationAction,
    StoreValidationResultAction,
)

logger = logging.getLogger(__name__)


class MockTeamsResponse:
    def __init__(self, status_code):
        self.status_code = status_code
        self.text = "test_text"


class MockSlackResponse:
    def __init__(self, status_code):
        self.status_code = status_code
        self.text = "test_text"
        self.content = json.dumps({"ok": "True"})


class MockCloudResponse:
    def __init__(self, status_code):
        self.status_code = status_code
        self.text = "test_text"
        self.content = json.dumps({"ok": "True"})


@freeze_time("09/26/2019 13:42:41")
def test_StoreAction():
    fake_in_memory_store = ValidationsStore(
        store_backend={
            "class_name": "InMemoryStoreBackend",
        }
    )
    stores = {"fake_in_memory_store": fake_in_memory_store}

    class Object:
        ge_cloud_mode = False

    data_context = Object()
    data_context.stores = stores

    action = StoreValidationResultAction(
        data_context=data_context,
        target_store_name="fake_in_memory_store",
    )
    assert fake_in_memory_store.list_keys() == []

    action.run(
        validation_result_suite_identifier=ValidationResultIdentifier(
            expectation_suite_identifier=ExpectationSuiteIdentifier(
                expectation_suite_name="default_expectations"
            ),
            run_id=RunIdentifier(run_name="prod_20190801"),
            batch_identifier="1234",
        ),
        validation_result_suite=ExpectationSuiteValidationResult(
            success=False, results=[]
        ),
        data_asset=None,
    )

    expected_run_id = RunIdentifier(
        run_name="prod_20190801", run_time="20190926T134241.000000Z"
    )

    assert len(fake_in_memory_store.list_keys()) == 1
    stored_identifier = fake_in_memory_store.list_keys()[0]
    assert stored_identifier.batch_identifier == "1234"
    assert (
        stored_identifier.expectation_suite_identifier.expectation_suite_name
        == "default_expectations"
    )
    assert stored_identifier.run_id == expected_run_id

    assert fake_in_memory_store.get(
        ValidationResultIdentifier(
            expectation_suite_identifier=ExpectationSuiteIdentifier(
                expectation_suite_name="default_expectations"
            ),
            run_id=expected_run_id,
            batch_identifier="1234",
        )
    ) == ExpectationSuiteValidationResult(success=False, results=[])


@mock.patch.object(Session, "post", return_value=MockSlackResponse(200))
def test_SlackNotificationAction(
    data_context_parameterized_expectation_suite,
    validation_result_suite,
    validation_result_suite_id,
):
    renderer = {
        "module_name": "great_expectations.render.renderer.slack_renderer",
        "class_name": "SlackRenderer",
    }
    slack_webhook = "https://hooks.slack.com/services/test/slack/webhook"
    slack_token = "test"
    slack_channel = "test"
    notify_on = "all"

    # test with just web_hook set; expect pass
    slack_action = SlackNotificationAction(
        data_context=data_context_parameterized_expectation_suite,
        renderer=renderer,
        slack_webhook=slack_webhook,
        notify_on=notify_on,
    )

    assert slack_action.run(
        validation_result_suite_identifier=validation_result_suite_id,
        validation_result_suite=validation_result_suite,
        data_asset=None,
    ) == {"slack_notification_result": "Slack notification succeeded."}

    # Test with slack_token and slack_channel set; expect pass
    slack_action = SlackNotificationAction(
        data_context=data_context_parameterized_expectation_suite,
        renderer=renderer,
        slack_token=slack_token,
        slack_channel=slack_channel,
        notify_on=notify_on,
    )

    assert slack_action.run(
        validation_result_suite_identifier=validation_result_suite_id,
        validation_result_suite=validation_result_suite,
        data_asset=None,
    ) == {"slack_notification_result": "Slack notification succeeded."}

    # Test with just slack_token set; expect fail
    with pytest.raises(AssertionError):
        SlackNotificationAction(
            data_context=data_context_parameterized_expectation_suite,
            renderer=renderer,
            slack_token=slack_token,
            notify_on=notify_on,
        )

    # Test with just slack_channel set; expect fail
    with pytest.raises(AssertionError):
        slack_action = SlackNotificationAction(
            data_context=data_context_parameterized_expectation_suite,
            renderer=renderer,
            slack_channel=slack_channel,
            notify_on=notify_on,
        )

    # Test with slack_channel, slack_token, and slack_webhook set; expect fail
    with pytest.raises(AssertionError):
        SlackNotificationAction(
            data_context=data_context_parameterized_expectation_suite,
            renderer=renderer,
            slack_channel=slack_channel,
            slack_token=slack_token,
            slack_webhook=slack_webhook,
            notify_on=notify_on,
        )

    # test notify on with failed run; expect pass
    notify_on = "failure"
    slack_action = SlackNotificationAction(
        data_context=data_context_parameterized_expectation_suite,
        renderer=renderer,
        slack_webhook=slack_webhook,
        notify_on=notify_on,
    )

    assert slack_action.run(
        validation_result_suite_identifier=validation_result_suite_id,
        validation_result_suite=ExpectationSuiteValidationResult(
            success=False,
            results=[],
            statistics={
                "successful_expectations": [],
                "evaluated_expectations": [],
            },
        ),
        data_asset=None,
    ) == {"slack_notification_result": "Slack notification succeeded."}

    # test notify on with successful run; expect pass
    notify_on = "failure"
    validation_result_suite.success = False
    slack_action = SlackNotificationAction(
        data_context=data_context_parameterized_expectation_suite,
        renderer=renderer,
        slack_webhook=slack_webhook,
        notify_on=notify_on,
    )

    assert slack_action.run(
        validation_result_suite_identifier=validation_result_suite_id,
        validation_result_suite=ExpectationSuiteValidationResult(
            success=True,
            results=[],
            statistics={
                "successful_expectations": [],
                "evaluated_expectations": [],
            },
        ),
        data_asset=None,
    ) == {"slack_notification_result": "none required"}


@pytest.mark.skipif(
    not is_library_loadable(library_name="pypd"),
    reason="pypd is not installed",
)
@mock.patch("pypd.EventV2")
def test_PagerdutyAlertAction(
    data_context_parameterized_expectation_suite,
    validation_result_suite,
    validation_result_suite_id,
):
    api_key = "test"
    routing_key = "test"

    pagerduty_action = PagerdutyAlertAction(
        data_context=data_context_parameterized_expectation_suite,
        api_key=api_key,
        routing_key=routing_key,
    )

    # Make sure the alert is sent by default when the validation has success = False
    validation_result_suite.success = False

    assert pagerduty_action.run(
        validation_result_suite_identifier=validation_result_suite_id,
        validation_result_suite=validation_result_suite,
        data_asset=None,
    ) == {"pagerduty_alert_result": "success"}

    # Make sure the alert is not sent by default when the validation has success = True
    validation_result_suite.success = True

    assert pagerduty_action.run(
        validation_result_suite_identifier=validation_result_suite_id,
        validation_result_suite=validation_result_suite,
        data_asset=None,
    ) == {"pagerduty_alert_result": "none sent"}


def test_OpsgenieAlertAction(
    data_context_parameterized_expectation_suite,
    validation_result_suite,
    validation_result_suite_id,
):

    renderer = {
        "module_name": "great_expectations.render.renderer.opsgenie_renderer",
        "class_name": "OpsgenieRenderer",
    }
    opsgenie_action = OpsgenieAlertAction(
        data_context=data_context_parameterized_expectation_suite,
        renderer=renderer,
        api_key="testapikey",
        region=None,
        priority="P3",
        notify_on="all",
    )

    # Make sure the alert is sent by default when the validation has success = False
    validation_result_suite.success = False

    assert opsgenie_action.run(
        validation_result_suite_identifier=validation_result_suite_id,
        validation_result_suite=validation_result_suite,
        data_asset=None,
    ) == {"opsgenie_alert_result": "error"}

    # Make sure the alert is not sent by default when the validation has success = True
    validation_result_suite.success = True

    assert opsgenie_action.run(
        validation_result_suite_identifier=validation_result_suite_id,
        validation_result_suite=validation_result_suite,
        data_asset=None,
    ) == {"opsgenie_alert_result": "error"}


@mock.patch.object(Session, "post", return_value=MockTeamsResponse(200))
def test_MicrosoftTeamsNotificationAction_good_request(
    data_context_parameterized_expectation_suite,
    validation_result_suite,
    validation_result_suite_extended_id,
):
    renderer = {
        "module_name": "great_expectations.render.renderer.microsoft_teams_renderer",
        "class_name": "MicrosoftTeamsRenderer",
    }
    teams_webhook = "http://testing"
    notify_on = "all"
    teams_action = MicrosoftTeamsNotificationAction(
        data_context=data_context_parameterized_expectation_suite,
        renderer=renderer,
        microsoft_teams_webhook=teams_webhook,
        notify_on=notify_on,
    )

    # validation_result_suite is None
    assert (
        teams_action.run(
            validation_result_suite_identifier=validation_result_suite_extended_id,
            validation_result_suite=None,
            data_asset=None,
        )
        is None
    )

    # if validation_result_suite_identifier is not ValidationResultIdentifier
    with pytest.raises(TypeError):
        teams_action.run(
            validation_result_suite_identifier="i_wont_work",
            validation_result_suite=validation_result_suite,
            data_asset=None,
        )

    assert teams_action.run(
        validation_result_suite_identifier=validation_result_suite_extended_id,
        validation_result_suite=validation_result_suite,
        data_asset=None,
    ) == {
        "microsoft_teams_notification_result": "Microsoft Teams notification succeeded."
    }

    # notify_on = success will return "Microsoft Teams notification succeeded" message
    # only if validation_result_suite.success = True
    validation_result_suite.success = False
    notify_on = "success"
    teams_action = MicrosoftTeamsNotificationAction(
        data_context=data_context_parameterized_expectation_suite,
        renderer=renderer,
        microsoft_teams_webhook=teams_webhook,
        notify_on=notify_on,
    )
    assert teams_action.run(
        validation_result_suite_identifier=validation_result_suite_extended_id,
        validation_result_suite=validation_result_suite,
        data_asset=None,
    ) == {"microsoft_teams_notification_result": None}

    validation_result_suite.success = True
    notify_on = "success"
    teams_action = MicrosoftTeamsNotificationAction(
        data_context=data_context_parameterized_expectation_suite,
        renderer=renderer,
        microsoft_teams_webhook=teams_webhook,
        notify_on=notify_on,
    )
    assert teams_action.run(
        validation_result_suite_identifier=validation_result_suite_extended_id,
        validation_result_suite=validation_result_suite,
        data_asset=None,
    ) == {
        "microsoft_teams_notification_result": "Microsoft Teams notification succeeded."
    }

    # notify_on failure will return "Microsoft Teams notification succeeded" message
    # only if validation_result_suite.success = False
    validation_result_suite.success = False
    notify_on = "failure"
    teams_action = MicrosoftTeamsNotificationAction(
        data_context=data_context_parameterized_expectation_suite,
        renderer=renderer,
        microsoft_teams_webhook=teams_webhook,
        notify_on=notify_on,
    )
    assert teams_action.run(
        validation_result_suite_identifier=validation_result_suite_extended_id,
        validation_result_suite=validation_result_suite,
        data_asset=None,
    ) == {
        "microsoft_teams_notification_result": "Microsoft Teams notification succeeded."
    }

    validation_result_suite.success = True
    notify_on = "failure"
    teams_action = MicrosoftTeamsNotificationAction(
        data_context=data_context_parameterized_expectation_suite,
        renderer=renderer,
        microsoft_teams_webhook=teams_webhook,
        notify_on=notify_on,
    )
    assert teams_action.run(
        validation_result_suite_identifier=validation_result_suite_extended_id,
        validation_result_suite=validation_result_suite,
        data_asset=None,
    ) == {"microsoft_teams_notification_result": None}


@mock.patch.object(Session, "post", return_value=MockTeamsResponse(400))
def test_MicrosoftTeamsNotificationAction_bad_request(
    data_context_parameterized_expectation_suite,
    validation_result_suite,
    validation_result_suite_extended_id,
    caplog,
):
    caplog.set_level(logging.WARNING)
    renderer = {
        "module_name": "great_expectations.render.renderer.microsoft_teams_renderer",
        "class_name": "MicrosoftTeamsRenderer",
    }
    teams_webhook = "http://testing"

    # notify : all
    notify_on = "all"
    teams_action = MicrosoftTeamsNotificationAction(
        data_context=data_context_parameterized_expectation_suite,
        renderer=renderer,
        microsoft_teams_webhook=teams_webhook,
        notify_on=notify_on,
    )
    assert teams_action.run(
        validation_result_suite_identifier=validation_result_suite_extended_id,
        validation_result_suite=validation_result_suite,
        data_asset=None,
    ) == {"microsoft_teams_notification_result": None}

    assert "Request to Microsoft Teams webhook returned error 400" in caplog.text


class MockSMTPServer:
    def __init__(self, raise_on, exception):
        self.raise_on = raise_on
        self.exception = exception

    def __call__(self, *arg, **kwargs):
        if self.raise_on == "__init__":
            raise self.exception
        return self

    def starttls(self, *args, **kwargs):
        if self.raise_on == "starttls":
            raise self.exception
        return None

    def login(self, *args, **kwargs):
        if self.raise_on == "login":
            raise self.exception
        return None

    def sendmail(self, *args, **kwargs):
        if self.raise_on == "sendmail":
            raise self.exception
        return None

    def quit(self, *args, **kwargs):
        if self.raise_on == "quit":
            raise self.exception
        return None


@pytest.mark.parametrize(
    (
        "class_to_patch,use_tls,use_ssl,raise_on,exception,expected,"
        "data_context_parameterized_expectation_suite,"
        "validation_result_suite,validation_result_suite_id"
    ),
    [
        ("SMTP", False, False, None, None, "success", None, None, None),
        ("SMTP", True, False, None, None, "success", None, None, None),
        ("SMTP", False, False, None, None, "success", None, None, None),
        ("SMTP_SSL", False, True, None, None, "success", None, None, None),
        (
            "SMTP_SSL",
            False,
            True,
            "__init__",
            smtplib.SMTPConnectError(421, "Can't connect"),
            None,
            None,
            None,
            None,
        ),
        (
            "SMTP",
            True,
            False,
            "starttls",
            smtplib.SMTPConnectError(421, "Can't connect"),
            None,
            None,
            None,
            None,
        ),
        (
            "SMTP",
            True,
            False,
            "login",
            smtplib.SMTPAuthenticationError(534, "Can't authenticate"),
            None,
            None,
            None,
            None,
        ),
    ],
    indirect=[
        "data_context_parameterized_expectation_suite",
        "validation_result_suite",
        "validation_result_suite_id",
    ],
    scope="module",
)
def test_EmailAction(
    class_to_patch,
    use_tls,
    use_ssl,
    raise_on,
    exception,
    expected,
    data_context_parameterized_expectation_suite,
    validation_result_suite,
    validation_result_suite_id,
):
    with mock.patch.object(
        smtplib,
        class_to_patch,
        new=MockSMTPServer(raise_on=raise_on, exception=exception),
    ):
        renderer = {
            "module_name": "great_expectations.render.renderer.email_renderer",
            "class_name": "EmailRenderer",
        }
        smtp_address = "test"
        smtp_port = 999
        sender_login = "test"
        sender_password = "test"
        sender_alias = "other"
        receiver_emails = "test"
        notify_on = "all"
        email_action = EmailAction(
            data_context=data_context_parameterized_expectation_suite,
            renderer=renderer,
            smtp_address=smtp_address,
            smtp_port=smtp_port,
            sender_login=sender_login,
            sender_password=sender_password,
            sender_alias=sender_alias,
            receiver_emails=receiver_emails,
            notify_on=notify_on,
            use_tls=use_tls,
            use_ssl=use_ssl,
        )
        assert email_action.sender_login != email_action.sender_alias
        assert email_action.run(
            validation_result_suite_identifier=validation_result_suite_id,
            validation_result_suite=validation_result_suite,
            data_asset=None,
        ) == {"email_result": expected}


# def test_ExtractAndStoreEvaluationParamsAction():
#     fake_in_memory_store = ValidationsStore(
#         root_directory = None,
#         store_backend = {
#             "class_name": "InMemoryStoreBackend",
#         }
#     )
#     stores = {
#         "fake_in_memory_store" : fake_in_memory_store
#     }
#
#     # NOTE: This is a hack meant to last until we implement runtime_environments
#     class Object(object):
#         pass
#
#     data_context = Object()
#     data_context.stores = stores
#
#     action = StoreValidationResultAction(
#         data_context = data_context,
#         target_store_name = "fake_in_memory_store",
#     )
#     assert fake_in_memory_store.list_keys() == []
#
#     vr_id = "ValidationResultIdentifier.my_db.default_generator.my_table.default_expectations.prod_20190801"
#     action.run(
#         validation_result_suite_id=ValidationResultIdentifier(from_string=vr_id),
#         validation_result_suite={},
#         data_asset=None
#     )
#
#     assert len(fake_in_memory_store.list_keys()) == 1
#     assert fake_in_memory_store.list_keys()[0].to_string() == "ValidationResultIdentifier.my_db.default_generator.my_table.default_expectations.prod_20190801"
#     assert fake_in_memory_store.get(ValidationResultIdentifier(
#         from_string="ValidationResultIdentifier.my_db.default_generator.my_table.default_expectations.prod_20190801"
#     )) == {}
#


@mock.patch.object(Session, "post", return_value=MockCloudResponse(200))
def test_cloud_notification_action(
    mock_post_method,
    cloud_data_context_with_datasource_pandas_engine,
    validation_result_suite_with_ge_cloud_id,
    validation_result_suite_ge_cloud_identifier,
    checkpoint_ge_cloud_id,
    ge_cloud_access_token,
):
    cloud_action: CloudNotificationAction = CloudNotificationAction(
        data_context=cloud_data_context_with_datasource_pandas_engine,
        checkpoint_ge_cloud_id=checkpoint_ge_cloud_id,
    )
    expected_ge_cloud_url = urljoin(
        cloud_action.data_context.ge_cloud_config.base_url,
        f"/organizations/{cloud_action.data_context.ge_cloud_config.organization_id}/contracts/"
        f"{cloud_action.checkpoint_ge_cloud_id}/suite-validation-results/{validation_result_suite_ge_cloud_identifier.ge_cloud_id}/notification-actions",
    )
    expected_headers = {
        "Content-Type": "application/vnd.api+json",
        "Authorization": f"Bearer {ge_cloud_access_token}",
    }

    assert cloud_action.run(
        validation_result_suite=validation_result_suite_with_ge_cloud_id,
        validation_result_suite_identifier=validation_result_suite_ge_cloud_identifier,
        data_asset=None,
    ) == {"cloud_notification_result": "Cloud notification succeeded."}
    mock_post_method.assert_called_with(
        url=expected_ge_cloud_url, headers=expected_headers
    )


@mock.patch.object(Session, "post", return_value=MockCloudResponse(418))
def test_cloud_notification_action_bad_response(
    mock_post_method,
    cloud_data_context_with_datasource_pandas_engine,
    validation_result_suite_with_ge_cloud_id,
    validation_result_suite_ge_cloud_identifier,
    checkpoint_ge_cloud_id,
    ge_cloud_access_token,
):
    cloud_action: CloudNotificationAction = CloudNotificationAction(
        data_context=cloud_data_context_with_datasource_pandas_engine,
        checkpoint_ge_cloud_id=checkpoint_ge_cloud_id,
    )
    expected_ge_cloud_url = urljoin(
        cloud_action.data_context.ge_cloud_config.base_url,
        f"/organizations/{cloud_action.data_context.ge_cloud_config.organization_id}/contracts/"
        f"{cloud_action.checkpoint_ge_cloud_id}/suite-validation-results/{validation_result_suite_ge_cloud_identifier.ge_cloud_id}/notification-actions",
    )
    expected_headers = {
        "Content-Type": "application/vnd.api+json",
        "Authorization": f"Bearer {ge_cloud_access_token}",
    }
    expected_result = {
        "cloud_notification_result": "Cloud Notification request returned error 418: test_text"
    }

    assert (
        cloud_action.run(
            validation_result_suite=validation_result_suite_with_ge_cloud_id,
            validation_result_suite_identifier=validation_result_suite_ge_cloud_identifier,
            data_asset=None,
        )
        == expected_result
    )
    mock_post_method.assert_called_with(
        url=expected_ge_cloud_url, headers=expected_headers
    )


from .conftest import aws_credentials, sns


def test_cloud_sns_notification_action(
    sns,
    validation_result_suite,
    cloud_data_context_with_datasource_pandas_engine,
    validation_result_suite_id,
    aws_credentials,
):
    subj_topic = "test-subj"
    created_subj = sns.create_topic(Name=subj_topic)
    arn = created_subj.get("TopicArn")
    sns_action = SNSNotificationAction(
        sns_topic_arn=arn,
        sns_message_subject="Subject",
        data_context=cloud_data_context_with_datasource_pandas_engine,
    )
    assert sns_action.run(
        validation_result_suite=validation_result_suite,
        validation_result_suite_identifier=validation_result_suite_id,
        data_asset=None,
    ).endswith("Subject")
