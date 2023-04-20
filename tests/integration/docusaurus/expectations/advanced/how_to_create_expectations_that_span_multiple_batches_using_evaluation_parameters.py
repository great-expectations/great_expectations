import great_expectations as gx
# TODO: Change to fluent batch request
from great_expectations.core.batch import BatchRequest

context = gx.get_context()

# TODO: Change to fluent batch request
batch_request_1 = BatchRequest(
  datasource_name="my_datasource",
  data_connector_name="my_data_connector",
  data_asset_name="my_data_asset_1"
)
upstream_validator = context.get_validator(batch_request=batch_request_1, expectation_suite_name="my_expectation_suite_1")

# TODO: Change to fluent batch request
batch_request_2 = BatchRequest(
  datasource_name="my_datasource",
  data_connector_name="my_data_connector",
  data_asset_name="my_data_asset_2"
)
downstream_validator = context.get_validator(batch_request=batch_request_2, expectation_suite_name="my_expectation_suite_2")


downstream_validator.interactive_evaluation = False


eval_param_urn = 'urn:great_expectations:validations:my_expectation_suite_1:expect_table_row_count_to_be_between.result.observed_value'
validation_result = downstream_validator.expect_table_row_count_to_equal(
   value={
      '$PARAMETER': eval_param_urn, # this is the actual parameter we're going to use in the validation
   }
)


expected_validation_result = {
   "result": {},
   "success": None,
   "meta": {},
   "exception_info": {
      "raised_exception": False,
      "exception_traceback": None,
      "exception_message": None
   }
}

assert expected_validation_result == validation_result

downstream_validator.save_expectation_suite(discard_failed_expectations=False)

results = context.run_checkpoint(
  checkpoint_name="my_checkpoint"
)


context.build_data_docs()