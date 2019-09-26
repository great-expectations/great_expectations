import pytest

from great_expectations.actions import (
    BasicValidationAction,
    StoreAction,
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
    ValidationResultIdentifier,
    ExpectationSuiteIdentifier,
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
    fake_in_memory_store = ValidationResultStore(
        root_directory = None,
        store_backend = {
            "class_name": "InMemoryStoreBackend",
        }
    )
    stores = {
        "fake_in_memory_store" : fake_in_memory_store
    }

    # NOTE: This is a hack meant to last until we implement runtime_configs
    class Object(object):
        pass

    data_context = Object()
    data_context.stores = stores

    action = StoreAction(
        data_context = data_context,
        target_store_name = "fake_in_memory_store",
    )
    assert fake_in_memory_store.list_keys() == []

    vr_id = "ValidationResultIdentifier.my_db.default_generator.my_table.default_expectations.prod_20190801"
    action.run(
        validation_result_suite_id=ValidationResultIdentifier(from_string=vr_id),
        validation_result_suite={},
        data_asset=None
    )

    assert len(fake_in_memory_store.list_keys()) == 1
    assert fake_in_memory_store.list_keys()[0].to_string() == "ValidationResultIdentifier.my_db.default_generator.my_table.default_expectations.prod_20190801"
    assert fake_in_memory_store.get(ValidationResultIdentifier(
        from_string="ValidationResultIdentifier.my_db.default_generator.my_table.default_expectations.prod_20190801"
    )) == {}


# def test_ExtractAndStoreEvaluationParamsAction():
#     fake_in_memory_store = ValidationResultStore(
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

