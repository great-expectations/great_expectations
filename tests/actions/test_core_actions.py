import pytest

from great_expectations.actions import (
    BasicValidationAction,
    # SummarizeAndSendToStoreAction,
)
from great_expectations.actions.types import (
    ActionInternalConfig,
    ActionConfig,
    ActionSetConfig,
)
from great_expectations.data_context.store import (
    InMemoryStore
)


def test_action_config():

    ActionConfig(
        coerce_types=True,
        **{
            "module_name" : "great_expectations.actions",
            "class_name": "SummarizeAndSendToWebhookAction",
            "kwargs" : {
                "webhook": "http://myslackwebhook.com/",
                "summarization_module_name": "great_expectations.render",
                "summarization_class_name": "SummarizeValidationResultsForSlack",
            }
        }
    )

def test_action_set_config():

    ActionSetConfig(
        coerce_types=True,
        **{
            "action_list" : {
                "my_first_action" : {
                    "module_name" : "great_expectations.actions",
                    "class_name": "SummarizeAndSendToWebhookAction",
                    "kwargs" : {
                        "webhook": "http://myslackwebhook.com/",
                        "summarization_module_name": "great_expectations.render",
                        "summarization_class_name": "SummarizeValidationResultsForSlack",
                    }
                }
            }
        }
    )

def test_subclass_of_BasicValidationAction():
    # I dunno. This is kind of a silly test.

    class MyCountingValidationAction(BasicValidationAction):
        def __init__(self, config):
            super(MyCountingValidationAction, self).__init__(config)
            self._counter = 0

        def take_action(self, validation_result_suite):
            self._counter += 1

    fake_validation_result_suite = {}

    my_action = MyCountingValidationAction(
        ActionInternalConfig(**{})
    )
    assert my_action._counter == 0

    my_action.take_action(fake_validation_result_suite)
    assert my_action._counter == 1


# TODO: Re-activate and finish implementing after refactoring Stores from DataContestAware to NameSpaceAware
# def test_SummarizeAndSendToStoreAction():
#     action = SummarizeAndSendToStoreAction(
#         ActionInternalConfig(**{
#             "summarization_module_name" : "great_expectations.actions.actions",
#             "summarization_class_name" : "TemporaryNoOpSummarizer",
#             "target_store_name" : "fake_in_memory_store",
#         }),
#         stores = {
#             # NOTE: Might need to replace this with an actual Store at some point.
#             "fake_in_memory_store" : InMemoryStore({}, None)
#         },
#         services = {},
#     )
#     action.take_action(
#         validation_result_suite={},
#         validation_result_suite_identifier={}
#     )