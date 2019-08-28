import logging
logger = logging.getLogger(__name__)

import importlib
import pandas as pd
import great_expectations as ge
from ..util import (
    get_class_from_module_name_and_class_name,
)
from .types import (
    ActionConfig,
    ActionInternalConfig,
)
from ..data_context.types import (
    ValidationResultIdentifier,
    ExpectationSuiteIdentifier,
)

# NOTE: Abe 2019/08/24 : This is first implementation of all these classes. Consider them UNSTABLE for now. 

class ValidationOperator(object):
    """Zoned for expansion"""
    pass

class ActionAwareValidationOperator(ValidationOperator):

    def validate_and_take_action(self, batch, expectation_purpose="default"):
        raise NotImplementedError

class DefaultActionAwareValidationOperator(ActionAwareValidationOperator):
    pass

class DataContextAwareValidationOperator(ActionAwareValidationOperator):
    def __init__(self, config, context):
        self.config = config
        self.context = context

    # TODO: Move this to DefaultDataContextAwareValidationOperator
    def process_batch(self, 
        batch,
        data_asset_identifier=None,
        purpose="default",
        run_identifier=None,
        action_set_name="default",
        process_warnings_and_quarantine_rows_on_error=False,
    ):
        # Get batch_identifier.
        # TODO : We should be using typed batch
        if data_asset_identifier is None:
            data_asset_identifier = batch.batch_identifier.data_asset_identifier

        failure_expectations = self._get_expectation_suite(data_asset_identifier, purpose, "failure")
        warning_expectations = self._get_expectation_suite(data_asset_identifier, purpose, "warning")
        quarantine_expectations = self._get_expectation_suite(data_asset_identifier, purpose, "quarantine")
        
        validation_result_dict = {
            "failure" : None,
            "warning" : None,
            "quarantine" : None,
        }

        validation_result_dict["failure"] = batch.validate(failure_expectations)
        #TODO: Add checking for exceptions in Expectations

        if validation_result_dict["failure"]["success"] == False:
            if process_warnings_and_quarantine_rows_on_error == False:

                #Process actions here
                # TODO: This should include the whole return object, not just validation_results.
                self._process_actions(validation_result_dict, self.config[action_set_name])
                
                return {
                    "validation_results" : validation_result_dict,
                }

        validation_result_dict["warning"] = batch.validate(warning_expectations)
        validation_result_dict["quarantine"] = batch.validate(quarantine_expectations, result_format="COMPLETE")
        #TODO: Add checking for exceptions in Expectations

        unexpected_index_set = set()
        for validation_result_suite in validation_result_dict["quarantine"]["results"]:
            if validation_result_suite["success"] == False:
                # print(evr["result"].keys())
                unexpected_index_set = unexpected_index_set.union(evr["result"]["unexpected_index_list"])

        quarantine_df = batch.loc[unexpected_index_set]
        # Pull non-quarantine batch here

        print("Validation successful")
        
        #Process actions here
        # TODO: This should include the whole return object, not just validation_results.
        self._process_actions(validation_result_dict, self.config[action_set_name])

        return {
            "validation_results" : validation_result_dict,
            # "non_quarantine_dataframe" : 
            "quarantine_data_frame": quarantine_df,
        }
    
    def _get_expectation_suite(
        self,
        data_asset_identifier,
        purpose,
        level,
    ):
        # FIXME: THis is a dummy method to quickly generate some ExpectationSuites
        df = pd.DataFrame({"x": range(10)})
        ge_df = ge.from_pandas(df)
        ge_df.expect_column_to_exist("x")
        ge_df.expect_column_values_to_be_in_set("x", [1,2,3,4])
        dummy_expectation_suite = ge_df.get_expectation_suite(discard_failed_expectations=False)
        return dummy_expectation_suite

        # TODO: Here's the REAL method.
        # return self.context.get_expectation_suite(
        #     ExpectationSuiteIdentifier(**{
        #         "data_asset_identifier" : data_asset_identifier,
        #         "suite_purpose" : purpose,
        #         "level" : level,
        #     })
        # )
    
    def _process_actions(self, validation_results, action_set_config):
        for k,v in action_set_config.items():
            print(k,v)
            loaded_module = importlib.import_module(v.pop("module_name"))
            action_class = getattr(loaded_module, v.pop("class_name"))

            action = action_class(
                ActionInternalConfig(**v["kwargs"]
                    #!!! Where does this comes from?
                ),
                stores = self.context.stores,
                services = {},
            )
            action.take_action(
                validation_result_suite=validation_results,
                validation_result_suite_identifier=ValidationResultIdentifier(from_string="ValidationResultIdentifier.a.b.c.hello.quarantine.prod.100")
            )


            print(k,v)


class DefaultDataContextAwareValidationOperator(DataContextAwareValidationOperator, DefaultActionAwareValidationOperator):
    pass
