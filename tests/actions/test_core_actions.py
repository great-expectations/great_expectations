import pytest

from great_expectations.core import ExpectationSuiteValidationResult

try:
    from unittest import mock
except ImportError:
    import mock

from great_expectations.validation_operators import (
    BasicValidationAction,
    SlackNotificationAction,
    StoreAction
)
# from great_expectations.actions.types import (
#     ActionInternalConfig,
#     ActionConfig,
#     ActionSetConfig,
# )
from great_expectations.data_context.store import (
    # NamespacedInMemoryStore
    ValidationsStore,
)
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
    ExpectationSuiteIdentifier,
    DataAssetIdentifier
)
from great_expectations.data_context.types import (
    ValidationResultIdentifier
)


def test_subclass_of_BasicValidationAction():
    # I dunno. This is kind of a silly test.

    class MyCountingValidationAction(BasicValidationAction):
        def __init__(self):
            super(MyCountingValidationAction, self).__init__()
            self._counter = 0

        def take_action(self, validation_result_suite):
            self._counter += 1

    fake_validation_result_suite = {}

    my_action = MyCountingValidationAction()
    assert my_action._counter == 0

    my_action.take_action(fake_validation_result_suite)
    assert my_action._counter == 1


def test_StoreAction():
    fake_in_memory_store = ValidationsStore(
        root_directory=None,
        store_backend={
            "class_name": "InMemoryStoreBackend",
        }
    )
    stores = {
        "fake_in_memory_store": fake_in_memory_store
    }

    # NOTE: This is a hack meant to last until we implement runtime_configs
    class Object(object):
        pass

    data_context = Object()
    data_context.stores = stores

    action = StoreAction(
        data_context=data_context,
        target_store_name="fake_in_memory_store",
    )
    assert fake_in_memory_store.list_keys() == []

    action.run(
        validation_result_suite_identifier=ValidationResultIdentifier(
            expectation_suite_identifier=ExpectationSuiteIdentifier(
                data_asset_name=DataAssetIdentifier("my_db", "default_generator", "my_table"),
                expectation_suite_name="default_expectations"
            ),
            run_id="prod_20190801"
        ),
        validation_result_suite=ExpectationSuiteValidationResult(
            success=False,
            results=[]
        ),
        data_asset=None
    )

    assert len(fake_in_memory_store.list_keys()) == 1
    stored_identifier = fake_in_memory_store.list_keys()[0]
    assert stored_identifier.expectation_suite_identifier.data_asset_name.datasource == "my_db"
    assert stored_identifier.expectation_suite_identifier.data_asset_name.generator == "default_generator"
    assert stored_identifier.expectation_suite_identifier.data_asset_name.generator_asset == "my_table"
    assert stored_identifier.expectation_suite_identifier.expectation_suite_name == "default_expectations"
    assert stored_identifier.run_id == "prod_20190801"

    assert fake_in_memory_store.get(ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier(
            data_asset_name=DataAssetIdentifier("my_db", "default_generator", "my_table"),
            expectation_suite_name="default_expectations"
        ),
        run_id="prod_20190801"
    )) == ExpectationSuiteValidationResult(
        success=False,
        results=[]
    )


def test_SlackNotificationAction(data_context):
    renderer = {
        "module_name": "great_expectations.render.renderer.slack_renderer",
        "class_name": "SlackRenderer",
    }
    slack_webhook = "https://hooks.slack.com/services/test/slack/webhook"
    notify_on = "all"

    slack_action = SlackNotificationAction(
        data_context=data_context,
        renderer=renderer,
        slack_webhook=slack_webhook,
        notify_on=notify_on
    )

    validation_result_suite = ExpectationSuiteValidationResult(results=[], success=True,
                                                               statistics={'evaluated_expectations': 0,
                                                                           'successful_expectations': 0,
                                                                           'unsuccessful_expectations': 0,
                                                                           'success_percent': None},
                                                               meta={
                                                                   'great_expectations.__version__': 'v0.8.0__develop',
                                                                   'data_asset_name': {'datasource': 'x',
                                                                                       'generator': 'y',
                                                                                       'generator_asset': 'z'},
                                                                   'expectation_suite_name': 'default',
                                                                   'run_id': '2019-09-25T060538.829112Z'})

    validation_result_suite_id = ValidationResultIdentifier(**{'expectation_suite_identifier': {
        'data_asset_name': {'datasource': 'x', 'generator': 'y', 'generator_asset': 'z'},
        'expectation_suite_name': 'default'}, 'run_id': 'test_100'})

    # TODO: improve this test - currently it is verifying a failed call to Slack
    assert slack_action.run(
        validation_result_suite_identifier=validation_result_suite_id,
        validation_result_suite=validation_result_suite,
        data_asset=None
    ) == None

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
#     # NOTE: This is a hack meant to last until we implement runtime_configs
#     class Object(object):
#         pass
#
#     data_context = Object()
#     data_context.stores = stores
#
#     action = StoreAction(
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
