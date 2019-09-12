import pytest
import json

import pandas as pd

import great_expectations as ge
from great_expectations.actions.validation_operators import (
    DataContextAwareValidationOperator,
)
from great_expectations.data_context import (
    ConfigOnlyDataContext,
)
from great_expectations.data_context.types import (
    DataContextConfig,
    DataAssetIdentifier,
)

@pytest.fixture()
def basic_data_context_config_for_validation_operator():
    return DataContextConfig(**{
        "plugins_directory": "plugins/",
        # "expectations_directory": "expectations/",
        "evaluation_parameter_store_name" : "evaluation_parameter_store",
        "expectations_store" : {
            "class_name": "ExpectationStore",
            "store_backend": {
                "class_name": "FixedLengthTupleFilesystemStoreBackend",
                "base_directory": "expectations/",
            }
        },
        "datasources": {},
        "stores": {
            # This isn't currently used for Validation Actions, but it's required for DataContext to work.
            "evaluation_parameter_store" : {
                "module_name": "great_expectations.data_context.store",
                "class_name": "InMemoryStoreBackend",
            },
            "warning_validation_result_store" : {
                "module_name": "great_expectations.data_context.store",
                "class_name": "ValidationResultStore",#NamespacedInMemoryStore",
                "store_backend": {
                    "class_name": "InMemoryStoreBackend",
                }
                # "store_config" : {
                #     "resource_identifier_class_name" : "ValidationResultIdentifier",
                # },
            }
        },
        "data_docs": {
            "sites": {}
        }
    })

def test_hello_world(basic_data_context_config_for_validation_operator):

    # FIXME:
    return

    context = ConfigOnlyDataContext(
        basic_data_context_config_for_validation_operator,
        "fake/testing/path/",
    )

    vo = DataContextAwareValidationOperator(
        # TODO: Turn this into a typed object.
        config={
            "default" : {
                "add_warnings_to_store" : {
                    "module_name" : "great_expectations.actions",
                    "class_name" : "SummarizeAndStoreAction",
                    "kwargs" : {
                        "result_key": "warnings",
                        "summarization_module_name": "great_expectations.actions.actions",
                        "summarization_class_name": "TemporaryNoOpSummarizer",
                        "target_store_name": "warning_validation_result_store",
                    }
                },
            }
        },
        context=context,
    )

    my_df = pd.DataFrame({"x": [1,2,3,4,5]})
    my_ge_df = ge.from_pandas(my_df)

    assert context.stores["warning_validation_result_store"].list_keys() == []

    results = vo.process_batch(
        batch=my_ge_df,
        data_asset_identifier=DataAssetIdentifier("a", "b", "c"),
        run_identifier="test-100",
        # action_set_name="default",
    )
    # print(json.dumps(results["validation_results"], indent=2))

    warning_validation_result_store_keys = context.stores["warning_validation_result_store"].list_keys() 
    print(warning_validation_result_store_keys)
    assert len(warning_validation_result_store_keys) == 1

    first_validation_result = context.stores["warning_validation_result_store"].get(warning_validation_result_store_keys[0])
    print(json.dumps(first_validation_result, indent=2))
    # assert context.stores["warning_validation_result_store"].get(warning_validation_result_store_keys[0]) == 1
    