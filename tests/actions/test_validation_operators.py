import pytest
import json

import pandas as pd

import great_expectations as ge
from great_expectations.actions.validation_operators import (
    DataContextAwareValidationOperator,
)
from great_expectations.data_context.store import (
    NamespacedInMemoryStore
)
from great_expectations.data_context import (
    ConfigOnlyDataContext,
)
from great_expectations.data_context.types import (
    DataContextConfig,
    DataAssetIdentifier,
    RunIdentifier
)

@pytest.fixture()
def basic_data_context_config_for_validation_operator():
    return DataContextConfig(**{
        "plugins_directory": "plugins/",
        "expectations_directory": "expectations/",
        "evaluation_parameter_store_name" : "evaluation_parameter_store",
        "datasources": {},
        "stores": {
            "evaluation_parameter_store" : {
                "module_name": "great_expectations.data_context.store",
                "class_name": "InMemoryStore",
            },
            "warning_validation_result_store" : {
                "module_name": "great_expectations.data_context.store",
                "class_name": "InMemoryStore",
            }
        },
        "data_docs": {
            "sites": {}
        }
    })

def test_hello_world(basic_data_context_config_for_validation_operator):
    context = ConfigOnlyDataContext(
        basic_data_context_config_for_validation_operator,
        "fake/testing/path/",
    )

    vo = DataContextAwareValidationOperator(
        config={
            "default" : {
                "add_warnings_to_store" : {
                    "module_name" : "great_expectations.actions",
                    "class_name" : "SummarizeAndStoreAction",
                    "kwargs" : {
                        "result_key": "warnings",
                        "target_store_name": "warning_validation_result_store",
                        "summarization_module_name": "great_expectations.actions.actions",
                        "summarization_class_name": "TemporaryNoOpSummarizer",
                    }
                },
                # "send_quarantined_values_to_store" : {},
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
        run_identifier=RunIdentifier("test", 100),
        # action_set_name="default",
    )
    # print(json.dumps(results["validation_results"], indent=2))

    print(context.stores["warning_validation_result_store"].list_keys())

    assert len(context.stores["warning_validation_result_store"].list_keys()) == 1
    # assert False