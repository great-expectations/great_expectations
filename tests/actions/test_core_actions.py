import pytest

from great_expectations.actions import (
    BasicValidationAction,
)
from great_expectations.actions.types import (
    ActionConfig,
    ActionSetConfig,
)


def test_action_config():

    ActionConfig(**{
        "module_name" : "great_expectations.actions",
        "class_name": "SummarizeAndSendToWebhookAction",
        "kwargs" : {
            "webhook": "http://myslackwebhook.com/",
            "summarization_module_name": "great_expectations.render",
            "summarization_class_name": "SummarizeValidationResultsForSlack",
        }
    })

def test_action_set_config():

    ActionSetConfig(
        # coerce_types=True, #Need to merge in fixes to LooselyTypedDataDcit before this will work.
        #TODO: We'll also need to modify LLTD to take a DictOf argument
        **{
            "action_list" : [ActionConfig(**{
                "module_name" : "great_expectations.actions",
                "class_name": "SummarizeAndSendToWebhookAction",
                "kwargs" : {
                    "webhook": "http://myslackwebhook.com/",
                    "summarization_module_name": "great_expectations.render",
                    "summarization_class_name": "SummarizeValidationResultsForSlack",
                }
            })]
        }
    )
