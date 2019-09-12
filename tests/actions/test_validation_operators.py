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
        },
        "validation_operators" : {},
    })


# This is the at-the-boundary method for DataContext.
# As such, it includes the ability to get a batch from a variety of identification methods
# def run_validation_operator(self, 
#     expectation_suite_name, # In the future, maybe we can allow this emthod to accept and expectation_suite instead of an es_name
#     validation_operator_name,
#     data_asset=None, # A data asset that COULD be a batch, OR a generic data asset
#     data_asset_id_string=None, # If data_asset isn't a batch, then
#     data_asset_identifier=None, # Required
#     run_identifier=None,
#     # action_set_name="default",
#     # process_warnings_and_quarantine_rows_on_error=False,
# ):
# def convert_to_batch(self, 
#     data_asset=None, # A data asset that COULD be a batch, OR a generic data asset
#     data_asset_id_string=None, # If data_asset isn't a batch, then
#     data_asset_identifier=None, # Required
#     run_identifier=None,
# ):
#     if data_asset != None:
#         # Get a valid data_asset_identifier or raise an error
#         if isinstance(data_asset, Batch):
#             data_asset_identifier = data_asset.data_asset_identifier

#         else:
#             if data_asset_identifier is not None:
#                 assert isinstance(data_asset_identifier, DataAssetIdentifier)

#             else:
#                 data_asset_identifier = self._normalize_data_asset_name(data_asset_id_string)

#         # We already have a data_asset identifier, so no further action needed.

#     else:
#         if data_asset_identifier is not None:
#             assert isinstance(data_asset_identifier, DataAssetIdentifier)

#         else:
#             data_asset_identifier = self._normalize_data_asset_name(data_asset_id_string)

#         # FIXME: Need to look up the API for this.
#         batch = self.get_batch(data_asset_identifier, run_identifier)

#     # At this point, we should have a properly typed and instantiated data_asset_identifier and batch
#     assert isinstance(data_asset_identifier, DataAssetIdentifier)
#     # assert isinstance(batch, Batch)
#     assert isinstance(batch, DataAsset)

#     batch.data_asset_identifier = data_asset_identifier

#     return batch

    # # Get the appropriate expectations
    # self.stores["expectations_store"].get(
    #     ExpectationSuiteIdentifier(
    #         data_asset_identifier=data_asset_identifier,
    #         expectation_suite_name=expectation_suite_name,
    #     )
    # )






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

    my_batch = data_context.convert_to_batch(
        my_ge_df,
        data_asset_identifier=DataAssetIdentifier(
            from_string="DataAssetIdentifier.a.b.c"
        ),
        run_identifier="test_100"
    )

    results = vo.process_batch(
        batch=my_batch,
        # expectation_suite_name_prefix="my_expectations",
    )
    # print(json.dumps(results["validation_results"], indent=2))

    warning_validation_result_store_keys = data_context.stores["warning_validation_result_store"].list_keys() 
    print(warning_validation_result_store_keys)
    assert len(warning_validation_result_store_keys) == 1

    first_validation_result = data_context.stores["warning_validation_result_store"].get(warning_validation_result_store_keys[0])
    print(json.dumps(first_validation_result, indent=2))
    # assert context.stores["warning_validation_result_store"].get(warning_validation_result_store_keys[0]) == 1
    