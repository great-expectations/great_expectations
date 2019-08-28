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

# FIXME : This is duplicated in test_data_context
@pytest.fixture()
def basic_data_context_config():
    return DataContextConfig(**{
        "plugins_directory": "plugins/",
        "expectations_directory": "expectations/",
        "evaluation_parameter_store_name" : "evaluation_parameter_store",
        "datasources": {},
        "stores": {
            "evaluation_parameter_store" : {
                "module_name": "great_expectations.data_context.store",
                "class_name": "InMemoryStore",
            }
        },
        "data_docs": {
            "sites": {}
        }
    })

def test_hello_world(basic_data_context_config):
    context = ConfigOnlyDataContext(
        basic_data_context_config,
        "fake/testing/path/",
    )

    vo = DataContextAwareValidationOperator(
        config={},
        context=context,
    )

    my_df = pd.DataFrame({"x": [1,2,3,4,5]})
    my_ge_df = ge.from_pandas(my_df)

    results = vo.process_batch(
        batch=my_ge_df,
        data_asset_identifier=DataAssetIdentifier("a", "b", "c"),
        run_identifier=RunIdentifier("test", 100),
        # action_set_name="default",
    )
    print(json.dumps(results["validation_results"], indent=2))
    # assert False