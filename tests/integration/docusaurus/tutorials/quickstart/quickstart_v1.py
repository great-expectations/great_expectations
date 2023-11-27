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
suite = context.add_expectation_suite("quickstart")
# How did I find out about this expectation? Before, I had tab completion. Now...the gallery? :(
expectation = ExpectColumnValuesToNotBeNull("pickup_datetime")
validator.validate(expectation)
suite.add_expectation(expectation)
suite.add_expectation(
    ExpectColumnValuesToBeBetween(
        "passenger_count", min_value=1, max_value=6
    )  # Note: we are removing "auto" at this point
)
# </snippet>

# Validate data
# <snippet name="tutorials/quickstart/quickstart.py create_checkpoint">
batch_expectations = context.add_batch_expectations(
    name="quickstart",
    expectation_suite=suite,
    batch_config=validator.batch_config,  # not discussed yet
)
# </snippet>

# <snippet name="tutorials/quickstart/quickstart.py run_checkpoint">
results = batch_expectations.run()
# </snippet>

# View results
# <snippet name="tutorials/quickstart/quickstart.py view_results">
context.view_validation_result(results)
# </snippet>
