import pathlib
import great_expectations as gx

# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.py get_context">
import great_expectations as gx

context = gx.get_context()
# </snippet>
data_directory = pathlib.Path(
    gx.__file__,
    "..",
    "..",
    "tests",
    "test_sets",
    "taxi_yellow_tripdata_samples",
).resolve(strict=True)


# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.py get validators">
datasource = context.sources.add_pandas_filesystem(
    name="demo_pandas", base_directory=data_directory
)

asset = datasource.add_csv_asset(
    "yellow_tripdata",
    batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
    order_by=["-year", "month"],
)

upstream_batch_request = asset.build_batch_request({"year": "2020", "month": "04"})
downstream_batch_request = asset.build_batch_request({"year": "2020", "month": "05"})

upstream_validator = context.get_validator(
    batch_request=upstream_batch_request,
    create_expectation_suite_with_name="upstream_expectation_suite",
)
downstream_validator = context.get_validator(
    batch_request=downstream_batch_request,
    create_expectation_suite_with_name="downstream_expectation_suite",
)
# </snippet>


# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.py create upstream_expectation_suite">
upstream_validator.expect_table_row_count_to_be_between(min_value=5000, max_value=20000)
upstream_validator.save_expectation_suite(discard_failed_expectations=False)
# </snippet>

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
downstream_validator.save_expectation_suite(discard_failed_expectations=False)
# </snippet>

# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.py run checkpoint">
checkpoint = gx.checkpoint.SimpleCheckpoint(
    name="checkpoint",
    data_context=context,
    validations=[
        {
            "batch_request": upstream_batch_request,
            "expectation_suite_name": upstream_validator.expectation_suite_name,
        },
        {
            "batch_request": downstream_batch_request,
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
