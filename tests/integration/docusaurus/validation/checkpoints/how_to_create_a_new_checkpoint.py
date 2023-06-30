import pathlib

import great_expectations as gx

context = gx.get_context()
data_directory = pathlib.Path(
    gx.__file__,
    "..",
    "..",
    "tests",
    "test_sets",
    "taxi_yellow_tripdata_samples",
).resolve(strict=True)


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


validator.expect_table_row_count_to_be_between(min_value=5000, max_value=20000)
validator.save_expectation_suite(discard_failed_expectations=False)


# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_create_a_new_checkpoint.py create checkpoint batch_request">
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

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_create_a_new_checkpoint.py run checkpoint batch_request">
checkpoint_result = checkpoint.run()
# </snippet>

assert checkpoint_result.success

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_create_a_new_checkpoint.py build data docs">
context.build_data_docs()
# </snippet>

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_create_a_new_checkpoint.py add checkpoint">
context.add_checkpoint(checkpoint=checkpoint)
# </snippet>

assert context.list_checkpoints() == ["my_checkpoint"]

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_create_a_new_checkpoint.py get checkpoint">
retrieved_checkpoint = context.get_checkpoint(name="my_checkpoint")
# </snippet>

assert retrieved_checkpoint
