from freezegun import freeze_time

from great_expectations.core import ExpectationSuiteValidationResult, RunIdentifier
from great_expectations.data_context.store import ValidationsStore
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.validation_operators import (
    SlackNotificationAction,
    StoreValidationResultAction,
)

try:
    from unittest import mock
except ImportError:
    import mock


@freeze_time("09/26/2019 13:42:41")
def test_StoreAction():
    fake_in_memory_store = ValidationsStore(
        store_backend={"class_name": "InMemoryStoreBackend",}
    )
    stores = {"fake_in_memory_store": fake_in_memory_store}

    class Object(object):
        pass

    data_context = Object()
    data_context.stores = stores

    action = StoreValidationResultAction(
        data_context=data_context, target_store_name="fake_in_memory_store",
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

    assert fake_in_memory_store.get(
        ValidationResultIdentifier(
            expectation_suite_identifier=ExpectationSuiteIdentifier(
                expectation_suite_name="default_expectations"
            ),
            run_id=expected_run_id,
            batch_identifier="1234",
        )
    ) == ExpectationSuiteValidationResult(success=False, results=[])


def test_SlackNotificationAction(data_context_parameterized_expectation_suite):
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

    validation_result_suite = ExpectationSuiteValidationResult(
        results=[],
        success=True,
        statistics={
            "evaluated_expectations": 0,
            "successful_expectations": 0,
            "unsuccessful_expectations": 0,
            "success_percent": None,
        },
        meta={
            "great_expectations_version": "v0.8.0__develop",
            "expectation_suite_name": "asset.default",
            "run_id": "test_100",
        },
    )

    validation_result_suite_id = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier("asset.default"),
        run_id="test_100",
        batch_identifier="1234",
    )

    # TODO: improve this test - currently it is verifying a failed call to Slack. It returns a "empty" payload
    assert slack_action.run(
        validation_result_suite_identifier=validation_result_suite_id,
        validation_result_suite=validation_result_suite,
        data_asset=None,
    ) == {"slack_notification_result": None}


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
