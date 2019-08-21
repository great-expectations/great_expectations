from .types import (
    ActionConfig
)

class BasicValidationAction(object):
    def __init__(self, config):
        #TODO: Add type checking
        # assert isinstance(config, ActionInternalConfig)

        self.config = config

    def take_action(self, validation_result_suite):
        return NotImplementedError

class NameSpaceAwareValidationAction(BasicValidationAction):

    def __init__(self, config, stores, services):
        #Uses config to instantiate itself
        super(NameSpaceAwareValidationAction, self).__init__(config)

        #The config may include references to stores and services.
        #Both act like endpoints to which results can be sent.
        #Stores support both reading and writing, in key-value fashion.
        #Services only support writing.
        #Future versions of Services may get results returned as part of the call.
        #(Some) Stores and Services will need persistent connections, which are managed by the DataContext.

    def take_action(self, validation_result_suite, validation_result_suite_identifier):
        return NotImplementedError


class SummarizeAndSendToStoreAction(NameSpaceAwareValidationAction):

    def __init__(self, config, stores, services):
        self.config = config

        self.summarization_class = self._get_class_from_module_and_class_name(
            self.config.summarization_module_name,
            self.config.summarization_class_name,
        ) #This might be a utility method... It's shared across so many classes.
        #Eventually, we probably need some checks to verify that this renderer class is compatible with validation_result_suite_identifiers.

        #??? Do we need a view_class as well?

        # NOTE: Eventually, we probably need some checks to verify that this store is compatible with validation_result_suite_identifiers.
        self.target_store = stores[self.config.target_store_name]

    def take_action(self, validation_result_suite, validation_result_suite_identifier):
        rendered_summary = self.summarization_class.render(validation_result_suite)
        store.set(validation_result_suite_identifier, rendered_summary)
    

# ### Pseudocode for ValidationAction classes:


# class NameSpaceAwareValidationOperator(ActionAwareValidationOperator):

#     def __init__(self, config, context):
#         self.config = config
#         self.context = context

#     def validate aka validate_and_take_actions(
#         batch,
#         purpose="default"
#     ):
#         data_asset_identifier = batch.batch_identifier.data_asset_identifier

#         halting_expectations = context.get_expectation_suite(ExpectationSuiteIdentifier(**{
#             "data_asset_identifier" : data_asset_identifier,
#             "purpose" : purpose,
#             "level" : "error",
#         }))
#         warning_expectations = context.get_expectation_suite(ExpectationSuiteIdentifier(**{
#             "data_asset_identifier" : data_asset_identifier,
#             "purpose" : purpose,
#             "level" : "warn",
#         }))
#         quarantine_expectations = context.get_expectation_suite(ExpectationSuiteIdentifier(**{
#             "data_asset_identifier" : data_asset_identifier,
#             "purpose" : purpose,
#             "level" : "quarantine",
#         }))

#         result_evrs = {
#             "halting_evrs" : None,
#             "warning_evrs" : None,
#             "quarantine_evrs" : None,
#         }

#         result_evrs["halting_evrs"] = batch.validate(halting_expectations)
#         #TODO: Add checking for exceptions in Expectations

#         if result_evrs["halting_evrs"]["success"] == False:
#             if process_warnings_and_quarantine_rows_if_halted != False:
#                 return result_evrs, None

#         result_evrs["warning_evrs"] = batch.validate(warning_expectations)
#         result_evrs["quarantine_evrs"] = batch.validate(quarantine_expectations, result_format="COMPLETE")
#         #TODO: Add checking for exceptions in Expectations

#         unexpected_index_set = set()
#         for evr in result_evrs["quarantine_evrs"]["results"]:
#             if evr["success"] == False:
#                 # print(evr["result"].keys())
#                 unexpected_index_set = unexpected_index_set.union(evr["result"]["unexpected_index_list"])

#         quarantine_df = batch.ix[unexpected_index_set]

#         print("Validation successful")
#         return result_evrs, quarantine_df
