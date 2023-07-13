import great_expectations as gx
from great_expectations.core.expectation_configuration import ExpectationConfiguration

context = gx.get_context()

datasource = context.sources.add_pandas_filesystem(
    name="example_datasource", base_directory="./data"
)

MY_BATCHING_REGEX = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"

asset = datasource.add_csv_asset("asset", batching_regex=MY_BATCHING_REGEX)

ec = ExpectationConfiguration(
    expectation_type="expect_column_values_to_not_be_null",
    kwargs={"column": "passenger_count"},
)
suite = context.add_expectation_suite(
    expectation_suite_name="example_suite", expectations=[ec]
)

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_validate_multiple_batches_within_single_checkpoint.py build_a_batch_request_with_multiple_batches">
batch_request = asset.build_batch_request()
# </snippet>

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_validate_multiple_batches_within_single_checkpoint.py add_batch_list">
batch_list = asset.get_batch_list_from_batch_request(batch_request)
batch_request_list = [batch.batch_request for batch in batch_list]
# </snippet>

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_validate_multiple_batches_within_single_checkpoint.py add_validations">
validations = [
    {"batch_request": batch.batch_request, "expectation_suite_name": "example_suite"}
    for batch in batch_list
]
# </snippet>

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_validate_multiple_batches_within_single_checkpoint.py add_checkpoint">
checkpoint = gx.checkpoint.SimpleCheckpoint(
    name="example_checkpoint", data_context=context, validations=validations
)

checkpoint_result = checkpoint.run()

context.build_data_docs()
context.open_data_docs()
# </snippet>
