# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.py get_context">
import great_expectations as gx

context = gx.get_context()
# </snippet>

# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.py get validators">
upstream_validator = context.sources.pandas_default.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
)
downstream_validator = context.sources.pandas_default.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
)
# </snippet>


# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.py create upstream_expectation_suite">
upstream_validator.expect_table_row_count_to_be_between(min_value=5000, max_value=20000)
upstream_validator.expectation_suite_name = "upstream_expectation_suite"
upstream_validator.save_expectation_suite(discard_failed_expectations=False)
# </snippet>

# TRANSFORM DATA BUT DON'T REMOVE ROWS - CHECK TO MAKE SURE ROWS AREN'T REMOVED

# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.py disable interactive_evaluation">
downstream_validator.interactive_evaluation = False
# </snippet>

# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.py add expectation with evaluation parameter">
eval_param_urn = "urn:great_expectations:validations:upstream_expectation_suite:expect_table_row_count_to_be_between.result.observed_value"
downstream_validator_validation_result = downstream_validator.expect_table_row_count_to_equal(
    value={
        "$PARAMETER": eval_param_urn,  # this is the actual parameter we're going to use in the validation
    }
)
# </snippet>

# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.py expected_validation_result">
expected_validation_result = {
    "success": None,
    "expectation_config": {
        "kwargs": {
            "value": {
                "$PARAMETER": "urn:great_expectations:validations:upstream_expectation_suite:expect_table_row_count_to_be_between.result.observed_value"
            }
        },
        "expectation_type": "expect_table_row_count_to_equal",
        "meta": {},
    },
    "meta": {},
    "exception_info": {
        "raised_exception": False,
        "exception_traceback": None,
        "exception_message": None,
    },
    "result": {},
}
# </snippet>

assert (
    expected_validation_result == downstream_validator_validation_result.to_json_dict()
)

# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.py save downstream_expectation_suite">
downstream_validator.expectation_suite_name = "downstream_expectation_suite"
downstream_validator.save_expectation_suite(discard_failed_expectations=False)
# </snippet>


# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.py run checkpoint">
checkpoint = gx.checkpoint.SimpleCheckpoint(
    name="checkpoint",
    data_context=context,
    validations=[
        {
            "batch_request": upstream_validator.active_batch.batch_request,
            "expectation_suite_name": upstream_validator.expectation_suite_name,
        },
        {
            "batch_request": downstream_validator.active_batch.batch_request,
            "expectation_suite_name": downstream_validator.expectation_suite_name,
        },
    ],
)

checkpoint_result = checkpoint.run()
# </snippet>

assert checkpoint_result.success

# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.py build data docs">
context.build_data_docs()
# </snippet>
