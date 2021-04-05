import logging
from unittest import mock

import pytest
from freezegun import freeze_time
from requests import Session

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
from great_expectations.validation_operators import (
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


@freeze_time("09/26/2019 13:42:41")
def test_StoreAction():
    fake_in_memory_store = ValidationsStore(
        store_backend={
            "class_name": "InMemoryStoreBackend",
        }
    )
    stores = {"fake_in_memory_store": fake_in_memory_store}

    class Object:
        pass

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
            run_id="prod_20190801",
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

    assert (
        fake_in_memory_store.get(
            ValidationResultIdentifier(
                expectation_suite_identifier=ExpectationSuiteIdentifier(
                    expectation_suite_name="default_expectations"
                ),
                run_id=expected_run_id,
                batch_identifier="1234",
            )
        )
        == ExpectationSuiteValidationResult(success=False, results=[])
    )


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
    notify_on = "all"

    slack_action = SlackNotificationAction(
        data_context=data_context_parameterized_expectation_suite,
        renderer=renderer,
        slack_webhook=slack_webhook,
        notify_on=notify_on,
    )

    # TODO: improve this test - currently it is verifying a failed call to Slack. It returns a "empty" payload
    assert (
        slack_action.run(
            validation_result_suite_identifier=validation_result_suite_id,
            validation_result_suite=validation_result_suite,
            data_asset=None,
        )
        == {"slack_notification_result": None}
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

    assert (
        pagerduty_action.run(
            validation_result_suite_identifier=validation_result_suite_id,
            validation_result_suite=validation_result_suite,
            data_asset=None,
        )
        == {"pagerduty_alert_result": "success"}
    )

    # Make sure the alert is not sent by default when the validation has success = True
    validation_result_suite.success = True

    assert (
        pagerduty_action.run(
            validation_result_suite_identifier=validation_result_suite_id,
            validation_result_suite=validation_result_suite,
            data_asset=None,
        )
        == {"pagerduty_alert_result": "none sent"}
    )


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

    assert (
        opsgenie_action.run(
            validation_result_suite_identifier=validation_result_suite_id,
            validation_result_suite=validation_result_suite,
            data_asset=None,
        )
        == {"opsgenie_alert_result": "error"}
    )

    # Make sure the alert is not sent by default when the validation has success = True
    validation_result_suite.success = True

    assert (
        opsgenie_action.run(
            validation_result_suite_identifier=validation_result_suite_id,
            validation_result_suite=validation_result_suite,
            data_asset=None,
        )
        == {"opsgenie_alert_result": "error"}
    )


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
    assert (
        teams_action.run(
            validation_result_suite_identifier=validation_result_suite_extended_id,
            validation_result_suite=validation_result_suite,
            data_asset=None,
        )
        == {"microsoft_teams_notification_result": None}
    )

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
    assert (
        teams_action.run(
            validation_result_suite_identifier=validation_result_suite_extended_id,
            validation_result_suite=validation_result_suite,
            data_asset=None,
        )
        == {"microsoft_teams_notification_result": None}
    )


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
    assert (
        teams_action.run(
            validation_result_suite_identifier=validation_result_suite_extended_id,
            validation_result_suite=validation_result_suite,
            data_asset=None,
        )
        == {"microsoft_teams_notification_result": None}
    )

    assert (
        "Request to Microsoft Teams webhook at http://testing returned error 400"
        in caplog.text
    )


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
        assert (
            email_action.run(
                validation_result_suite_identifier=validation_result_suite_id,
                validation_result_suite=validation_result_suite,
                data_asset=None,
            )
            == {"email_result": expected}
        )


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
