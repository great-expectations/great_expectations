import pathlib

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_validate_data_by_running_a_checkpoint.py get_context">
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


# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_validate_data_by_running_a_checkpoint.py create asset and get validator">
import sys
import great_expectations as gx

from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult

context = gx.get_context()

result: CheckpointResult = context.run_checkpoint(
    checkpoint_name="my_checkpoint",
    batch_request=None,
    run_name=None,
)

if not result["success"]:
    print("Validation failed!")
    sys.exit(1)

print("Validation succeeded!")


datasource = context.sources.add_pandas_filesystem(
    name="demo_pandas", base_directory=data_directory
)

asset = datasource.add_csv_asset(
    "yellow_tripdata",
    batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
    order_by=["-year", "month"],
)

batch_request = asset.build_batch_request({"year": "2020", "month": "04"})
validator = context.get_validator(
    batch_request=batch_request,
    create_expectation_suite_with_name="my_expectation_suite",
)
# </snippet>


# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_validate_data_by_running_a_checkpoint.py create expectation suite">
validator.expect_table_row_count_to_be_between(min_value=5000, max_value=20000)
validator.save_expectation_suite(discard_failed_expectations=False)
# </snippet>


# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_validate_data_by_running_a_checkpoint.py create checkpoint batch_request">
checkpoint = gx.checkpoint.SimpleCheckpoint(
    name="my_checkpoint",
    data_context=context,
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": "my_expectation_suite",
        },
    ],
)
# </snippet>
