import pathlib
import great_expectations as gx

data_directory = pathlib.Path(
    gx.__file__,
    "..",
    "..",
    "tests",
    "test_sets",
    "taxi_yellow_tripdata_samples",
).resolve(strict=True)

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_validate_data_by_running_a_checkpoint.py setup">
# setup
import sys
import great_expectations as gx

context = gx.get_context()

# starting from scratch, we add a datasource and asset
datasource = context.sources.add_pandas_filesystem(
    name="taxi_source", base_directory=data_directory
)

asset = datasource.add_csv_asset(
    "yellow_tripdata",
    batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
    order_by=["-year", "month"],
)

# use a validator to create an expectation suite
validator = context.get_validator(
    datasource_name="taxi_source", data_asset_name="yellow_tripdata"
)
validator.expect_column_values_to_not_be_null("pickup_datetime")
context.add_expectation_suite("yellow_tripdata_suite")

# create a checkpoint
checkpoint = gx.checkpoint.SimpleCheckpoint(
    name="my_checkpoint",
    data_context=context,
    expectation_suite_name="yellow_tripdata_suite",
)

# add (save) the checkpoint to the data context
context.add_checkpoint(checkpoint=checkpoint)
cp = context.get_checkpoint(name="my_checkpoint")
assert cp.name == "my_checkpoint"
# </snippet>

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_validate_data_by_running_a_checkpoint.py checkpoint script">
# context = gx.get_context()
result = context.run_checkpoint(
    checkpoint_name="my_checkpoint",
    batch_request={
        "datasource_name": "taxi_source",
        "data_asset_name": "yellow_tripdata",
    },
    run_name=None,
)

if not result["success"]:
    print("Validation failed!")
    sys.exit(1)

print("Validation succeeded!")
# </snippet>
