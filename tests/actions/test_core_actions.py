import pytest

from great_expectations.actions import (
    BasicValidationAction,
    SummarizeAndStoreAction,
)
# from great_expectations.actions.types import (
#     ActionInternalConfig,
#     ActionConfig,
#     ActionSetConfig,
# )
from great_expectations.data_context.store import (
    # NamespacedInMemoryStore
    ValidationResultStore,
)
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier
)


def test_action_config():

    action_config = {
        "module_name" : "great_expectations.actions",
        "class_name": "SummarizeAndSendToWebhookAction",
        "webhook": "http://myslackwebhook.com/",
        "summarization_module_name": "great_expectations.render",
        "summarization_class_name": "SummarizeValidationResultsForSlack",
    }

def test_action_set_config():

    action_list = [{
        "name" : "my_first_action",
        "module_name" : "great_expectations.actions",
        "class_name": "SummarizeAndSendToWebhookAction",
        "webhook": "http://myslackwebhook.com/",
        "summarization_module_name": "great_expectations.render",
        "summarization_class_name": "SummarizeValidationResultsForSlack",
    }]

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


def test_SummarizeAndStoreAction():
    fake_in_memory_store = ValidationResultStore(
        root_directory = None,
        store_backend = {
            "class_name": "InMemoryStoreBackend",
            # base_directory: uncommitted/validations/
            # filepath_template: '{4}/{0}/{1}/{2}/{3}.json'
        }
    )
    # config={
    #     "resource_identifier_class_name": "ValidationResultIdentifier"
    # })
    stores = {
        "fake_in_memory_store" : fake_in_memory_store
    }

    # NOTE: This is a hack meant to last until we implement runtime_configs
    class Object(object):
        pass

    data_context = Object()
    data_context.stores = stores

    action = SummarizeAndStoreAction(
        # name="my_action",
        data_context = data_context,
        # runtime_config = {
        #     stores = stores,
        #     services = {},
        # }
        # result_key="my_key",
        target_store_name = "fake_in_memory_store",
        summarizer = {
            "module_name": "great_expectations.actions.actions",
            "class_name": "TemporaryNoOpSummarizer",
        }
    )
    assert fake_in_memory_store.list_keys() == []

    vr_id = "ValidationResultIdentifier.my_db.default_generator.my_table.default_expectations.prod_20190801"
    action.take_action(
        validation_result_suite={},
        validation_result_id=ValidationResultIdentifier(from_string=vr_id)
    )
    assert len(fake_in_memory_store.list_keys()) == 1
    assert fake_in_memory_store.list_keys()[0].to_string() == "ValidationResultIdentifier.my_db.default_generator.my_table.default_expectations.prod_20190801"
    assert fake_in_memory_store.get(ValidationResultIdentifier(
        from_string="ValidationResultIdentifier.my_db.default_generator.my_table.default_expectations.prod_20190801"
    )) == {}


