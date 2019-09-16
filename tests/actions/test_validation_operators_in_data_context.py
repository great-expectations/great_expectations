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
    ExpectationSuiteIdentifier,
)

@pytest.fixture()
def basic_data_context_config_for_validation_operator():
    return DataContextConfig(**{
        "plugins_directory": "plugins/",
        "evaluation_parameter_store_name" : "evaluation_parameter_store",
        "expectations_store" : {
            "class_name": "ExpectationStore",
            "store_backend": {
                "class_name": "InMemoryStoreBackend",
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
                "class_name": "ValidationResultStore",
                "store_backend": {
                    "class_name": "InMemoryStoreBackend",
                }
            }
        },
        "data_docs": {
            "sites": {}
        },
        "validation_operators": {
            "default" : {
                # "module_name" : "great_expectations.actions.validation_operators",
                "class_name" : "DefaultDataContextAwareValidationOperator",
                "process_warnings_and_quarantine_rows_on_error" : True,
                "action_list" : [{
                    "name": "add_warnings_to_store",
                    "result_key": "warning",
                    "action" : {
                        # "module_name" : "great_expectations.actions",
                        "class_name" : "SummarizeAndStoreAction",
                        "target_store_name": "warning_validation_result_store",
                        "summarizer":{
                            "module_name": "great_expectations.actions.actions",
                            "class_name": "TemporaryNoOpSummarizer",
                        },

                    }
                },{
                    "name" : "send_slack_message",
                    "result_key": None,
                    "action" : {
                        "module_name" : "great_expectations.actions",
                        "class_name" : "NoOpAction",
                    }
                }]
            }
        }
    })

def test_hello_world(basic_data_context_config_for_validation_operator):

    data_context = ConfigOnlyDataContext(
        basic_data_context_config_for_validation_operator,
        "fake/testing/path/",
    )

    data_asset_identifier = DataAssetIdentifier("a", "b", "c")
    data_context.stores["expectations_store"].set(
        ExpectationSuiteIdentifier(
            data_asset_name=data_asset_identifier,
            expectation_suite_name="warning"
        ),
        # TODO:
        None
    )

    my_df = pd.DataFrame({"x": [1,2,3,4,5]})
    my_ge_df = ge.from_pandas(my_df)

    assert data_context.stores["warning_validation_result_store"].list_keys() == []

    results = data_context.run_validation_operator(
        # TODO: Allow this to take 0, 1, or n data_assets
        data_asset=my_ge_df,
        data_asset_identifier=data_asset_identifier,
        run_identifier="test-100",
        validation_operator_name="default",
    )
    # TODO: Add tests for other argument structures for run_validation_operator
    # results = data_context.run_validation_operator(
    #     data_asset=my_ge_df,
    #     # validation_operator_name="default",
    # )

    # print(json.dumps(results["validation_results"], indent=2))

    warning_validation_result_store_keys = data_context.stores["warning_validation_result_store"].list_keys() 
    print(warning_validation_result_store_keys)
    assert len(warning_validation_result_store_keys) == 1

    first_validation_result = data_context.stores["warning_validation_result_store"].get(warning_validation_result_store_keys[0])
    print(json.dumps(first_validation_result, indent=2))
    # assert context.stores["warning_validation_result_store"].get(warning_validation_result_store_keys[0]) == 1
    