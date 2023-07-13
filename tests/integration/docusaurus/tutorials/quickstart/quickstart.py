# <snippet name="tutorials/quickstart/quickstart.py import_gx">
import great_expectations as gx

# </snippet>

# Set up
# <snippet name="tutorials/quickstart/quickstart.py get_context">
context = gx.get_context()
# </snippet>

# Connect to data
# <snippet name="tutorials/quickstart/quickstart.py connect_to_data">
validator = context.sources.pandas_default.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
)
# </snippet>

# Create Expectations
# <snippet name="tutorials/quickstart/quickstart.py create_expectation">
validator.expect_column_values_to_not_be_null("pickup_datetime")
validator.expect_column_values_to_be_between("passenger_count", auto=True)
validator.save_expectation_suite()
# </snippet>

# Validate data
# <snippet name="tutorials/quickstart/quickstart.py create_checkpoint">
checkpoint = context.add_or_update_checkpoint(
    name="my_quickstart_checkpoint",
    validator=validator,
)
# </snippet>

# <snippet name="tutorials/quickstart/quickstart.py run_checkpoint">
checkpoint_result = checkpoint.run()
# </snippet>

# View results
# <snippet name="tutorials/quickstart/quickstart.py view_results">
context.view_validation_result(checkpoint_result)
# </snippet>
