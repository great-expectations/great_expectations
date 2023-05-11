import great_expectations as gx

print("Setting up")
# Set up
from great_expectations.data_context import FileDataContext

context = FileDataContext.create(project_root_dir="/gx")

# Connect to data
# validator = context.sources.pandas_default.read_csv(
#     "data/yellow_tripdata_sample_2019-01.csv"
# )
validator = context.sources.pandas_default.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
)


print("Creating Expectations")
# Create Expectations
validator.expect_column_values_to_not_be_null("pickup_datetime")
validator.expect_column_values_to_be_between("passenger_count", auto=True)

# # Connect to data
# validator = context.sources.pandas_default.read_csv(
#     "data/Titanic.csv"
# )
#
# # Create Expectations
# validator.expect_column_values_to_not_be_null("Name")
# validator.expect_column_values_to_be_between("Age", auto=True)

print("Validating Data")
# Validate data
checkpoint = gx.checkpoint.SimpleCheckpoint(
    name="my_quickstart_checkpoint",
    data_context=context,
    validator=validator,
)

checkpoint_result = checkpoint.run()
print(checkpoint_result)

print("See above for checkpoint result ^^^")

# View results
validation_result_identifier = checkpoint_result.list_validation_result_identifiers()[0]
# context.open_data_docs(resource_identifier=validation_result_identifier)
