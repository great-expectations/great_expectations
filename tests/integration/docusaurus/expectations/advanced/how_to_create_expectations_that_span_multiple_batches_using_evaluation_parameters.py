import pathlib
import great_expectations as gx
# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.py get_context">
import great_expectations as gx

context = gx.get_context()
# </snippet>
############################# TODO: DEBUG
print(context.evaluation_parameter_store.list_keys())
data_directory = pathlib.Path(
    gx.__file__,
    "..",
    "..",
    "tests",
    "test_sets",
    "taxi_yellow_tripdata_samples",
).resolve(strict=True)
############################# TODO: DEBUG


# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.py get validators">
############################# TODO: DEBUG
# upstream_validator = context.sources.pandas_default.read_csv(
#     "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
# )
# downstream_validator = context.sources.pandas_default.read_csv(
#     "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
# )
# Try using pandas datasources
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

upstream_validator = context.get_validator(batch_request=upstream_batch_request, create_expectation_suite_with_name="upstream_expectation_suite")
downstream_validator = context.get_validator(batch_request=downstream_batch_request, create_expectation_suite_with_name="downstream_expectation_suite")
############################# TODO: DEBUG
# </snippet>


# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.py create upstream_expectation_suite">
upstream_validator.expect_table_row_count_to_be_between(min_value=5000, max_value=20000)
# upstream_validator.expectation_suite_name = "upstream_expectation_suite"
upstream_validator.save_expectation_suite(discard_failed_expectations=False)
# </snippet>

# TRANSFORM DATA BUT DON'T REMOVE ROWS - CHECK TO MAKE SURE ROWS AREN'T REMOVED

# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.py disable interactive_evaluation">
downstream_validator.interactive_evaluation = False
# </snippet>

# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.py add expectation with evaluation parameter">

# TODO: should urn change? This is the ValidationResultIdentifier::upstream_expectation_suite/__none__/20230421T152423.676916Z/demo_pandas-yellow_tripdata-year_2020-month_05
eval_param_urn = "urn:great_expectations:validations:upstream_expectation_suite:expect_table_row_count_to_be_between.result.observed_value"
############################# TODO: DEBUG
# eval_param_urn = "urn:great_expectations:stores:validations_store:upstream_expectation_suite:expect_table_row_count_to_be_between.result.observed_value"
############################# TODO: DEBUG
downstream_validator_validation_result = downstream_validator.expect_table_row_count_to_equal(
    value={
        "$PARAMETER": eval_param_urn,  # this is the actual parameter we're going to use in the validation
        ############################# TODO: DEBUG WHY IS THIS OVERRIDING
        # f"$PARAMETER.{eval_param_urn}": 1
        ############################# TODO: DEBUG
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

# assert (
#     expected_validation_result == downstream_validator_validation_result.to_json_dict()
# )

# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.py save downstream_expectation_suite">
# downstream_validator.expectation_suite_name = "downstream_expectation_suite"
downstream_validator.save_expectation_suite(discard_failed_expectations=False)
# </snippet>

downstream_validator.interactive_evaluation = True

# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.py run checkpoint">
############################# TODO: DEBUG
# Single checkpoint with non fluent batch requests
# checkpoint = gx.checkpoint.SimpleCheckpoint(
#     name="checkpoint",
#     data_context=context,
#     validations=[
#         {
#             "batch_request": upstream_validator.active_batch.batch_request,
#             "expectation_suite_name": upstream_validator.expectation_suite_name,
#         },
#         {
#             "batch_request": downstream_validator.active_batch.batch_request,
#             "expectation_suite_name": downstream_validator.expectation_suite_name,
#         },
#     ],
# )
#
# checkpoint_result = checkpoint.run()

# Single checkpoint with fluent batch requests
# checkpoint = gx.checkpoint.SimpleCheckpoint(
#     name="checkpoint",
#     data_context=context,
#     validations=[
#         {
#             "batch_request": upstream_batch_request,
#             "expectation_suite_name": upstream_validator.expectation_suite_name,
#         },
#         {
#             "batch_request": downstream_batch_request,
#             "expectation_suite_name": downstream_validator.expectation_suite_name,
#         },
#     ],
# )
#
# checkpoint_result = checkpoint.run()

# Multiple checkpoints with validators
# upstream_checkpoint = gx.checkpoint.SimpleCheckpoint(
#     name="upstream_checkpoint",
#     data_context=context,
#     validator=upstream_validator,
# )
# downstream_checkpoint = gx.checkpoint.SimpleCheckpoint(
#     name="downstream_checkpoint",
#     data_context=context,
#     validator=downstream_validator,
# )
# upstream_checkpoint_result = upstream_checkpoint.run()
# downstream_checkpoint_result = downstream_checkpoint.run()

# Multiple checkpoints with fluent batch requests
upstream_checkpoint = gx.checkpoint.SimpleCheckpoint(
    name="upstream_checkpoint",
    data_context=context,
    validations=[
        {
            "batch_request": upstream_batch_request,
            "expectation_suite_name": upstream_validator.expectation_suite_name
        }
    ],
)
downstream_checkpoint = gx.checkpoint.SimpleCheckpoint(
    name="downstream_checkpoint",
    data_context=context,
    validations=[
        {
            "batch_request": downstream_batch_request,
            "expectation_suite_name": downstream_validator.expectation_suite_name
        }
    ],
)
upstream_checkpoint_result = upstream_checkpoint.run()
downstream_checkpoint_result = downstream_checkpoint.run()


# breakpoint()
############################# TODO: DEBUG
# </snippet>

############################# TODO: DEBUG
print(context.evaluation_parameter_store.list_keys())
eval_param_store_key = context.evaluation_parameter_store.list_keys()[0]

assert eval_param_store_key.to_evaluation_parameter_urn() == eval_param_urn
assert context.evaluation_parameter_store.get(context.evaluation_parameter_store.list_keys()[0]) == 10000
# [ValidationMetricIdentifier::__none__/20230421T145800.664428Z/#ephemeral_pandas_asset/upstream_expectation_suite/expect_table_row_count_to_be_between.result.observed_value/__]
# breakpoint()
############################# TODO: DEBUG

############################# TODO: DEBUG
# assert checkpoint_result.success
assert upstream_checkpoint_result.success
# TODO: THIS SHOULD BE TRUE:
assert downstream_checkpoint_result.success
############################# TODO: DEBUG

# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.py build data docs">
context.build_data_docs()
# </snippet>
