import logging
logger = logging.getLogger(__name__)

from ..util import (
    get_class_from_module_name_and_class_name,
)
from .types import (
    ActionConfig,
    ActionInternalConfig,
)
from ..data_context.types import (
    ValidationResultIdentifier
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

    def process_batch(self, 
        batch,
        data_asset_identifier=None,
        run_identifier=None,
        action_set_name="default"
    ):
        # Get batch_identifier.
        # TODO : We should be using typed batch
        if data_asset_identifier is None:
            data_asset_identifier = batch.batch_identifier.data_asset_identifier

        # TODO : This is a substantive revision to the way ExpectationSuites are used in 
        failure_expectations = self._get_expectation_suite(data_asset_identifier, purpose, "failure")
        warning_expectations = self._get_expectation_suite(data_asset_identifier, purpose, "warning")
        quarantine_expectations = self._get_expectation_suite(data_asset_identifier, purpose, "quarantine")
        
        validation_result_dict = {
            "failure" : None,
            "warning" : None,
            "quarantine" : None,
        }

        validation_result_dict["failure"] = batch.validate(halting_expectations)
        #TODO: Add checking for exceptions in Expectations

        if validation_result_dict["failure"]["success"] == False:
            if process_warnings_and_quarantine_rows_if_halted != False:

                #Process actions here
                
                return result_evrs, None

        validation_result_dict["warning"] = batch.validate(warning_expectations)
        validation_result_dict["quarantine"] = batch.validate(quarantine_expectations, result_format="COMPLETE")
        #TODO: Add checking for exceptions in Expectations

        unexpected_index_set = set()
        for evr in result_evrs["quarantine"]["results"]:
            if evr["success"] == False:
                # print(evr["result"].keys())
                unexpected_index_set = unexpected_index_set.union(evr["result"]["unexpected_index_list"])

        quarantine_df = batch.ix[unexpected_index_set]

        print("Validation successful")
        return result_evrs, quarantine_df
    
    def _get_expectation_suite(
        data_asset_identifier,
        purpose,
        level,
    ):
        return self.context.get_expectation_suite(ExpectationSuiteIdentifier(**{
            "data_asset_identifier" : data_asset_identifier,
            "purpose" : purpose,
            "level" : "failure",
        }))

class DefaultDataContextAwareValidationOperator(DataContextAwareValidationOperator, DefaultActionAwareValidationOperator):
    pass
