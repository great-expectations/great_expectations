import pytest
import json

import pandas as pd

import great_expectations as ge
from great_expectations.actions.validation_operators import (
    DataContextAwareValidationOperator,
    DefaultDataContextAwareValidationOperator,
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
                "class_name": "ValidationResultStore",
                "store_backend": {
                    "class_name": "InMemoryStoreBackend",
                }
            }
        },
        "data_docs": {
            "sites": {}
        },
        "validation_operators" : {},
    })

def test_hello_world(basic_data_context_config_for_validation_operator):

    data_context = ConfigOnlyDataContext(
        basic_data_context_config_for_validation_operator,
        "fake/testing/path/",
    )

    vo = DefaultDataContextAwareValidationOperator(
        data_context=data_context,
        process_warnings_and_quarantine_rows_on_error=True,
        action_list = [{
            "name": "add_warnings_to_store",
            "result_key": "warning",
            "action" : {
                "module_name" : "great_expectations.actions",
                "class_name" : "SummarizeAndStoreAction",
                "target_store_name": "warning_validation_result_store",
                "summarizer":{
                    "module_name": "great_expectations.actions.actions",
                    "class_name": "TemporaryNoOpSummarizer",
                }
            }
        }],
    )

    my_df = pd.DataFrame({"x": [1,2,3,4,5]})
    my_ge_df = ge.from_pandas(my_df)

    assert data_context.stores["warning_validation_result_store"].list_keys() == []

    results = vo.run(
        data_asset=my_ge_df,
        data_asset_identifier=DataAssetIdentifier(
            from_string="DataAssetIdentifier.a.b.c"
        ),
        run_identifier="test_100"
    )
    # print(json.dumps(results["validation_results"], indent=2))

    warning_validation_result_store_keys = data_context.stores["warning_validation_result_store"].list_keys() 
    print(warning_validation_result_store_keys)
    assert len(warning_validation_result_store_keys) == 1

    first_validation_result = data_context.stores["warning_validation_result_store"].get(warning_validation_result_store_keys[0])
    print(json.dumps(first_validation_result, indent=2))
    # assert data_context.stores["warning_validation_result_store"].get(warning_validation_result_store_keys[0]) == 1
    