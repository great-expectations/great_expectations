# <snippet name="tutorials/quickstart/quickstart.py import_gx">
import great_expectations as gx
import great_expectations.expectations as gxe

# </snippet>

# Set up
# <snippet name="tutorials/quickstart/quickstart.py get_context">
context = gx.get_context()
# </snippet>

# Connect to data
# <snippet name="tutorials/quickstart/quickstart.py connect_to_data">
batch = context.data_sources.pandas_default.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
)
# </snippet>

# Create Expectations
# <snippet name="tutorials/quickstart/quickstart.py create_expectation">
suite = context.add_expectation_suite("my_suite")

# TODO: update where these expectations are imported
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="pickup_datetime"))
suite.add_expectation(
    gxe.ExpectColumnValuesToBeBetween(
        column="passenger_count", min_value=1, max_value=6
    )
)
# </snippet>

# Validate data
# TODO: update docs using this snippet and name of this snippet
# <snippet name="tutorials/quickstart/quickstart.py create_checkpoint">
# We no longer need to create a checkpoint to interactively validate data
# </snippet>

# <snippet name="tutorials/quickstart/quickstart.py run_checkpoint">
results = batch.validate(suite)
# </snippet>

# View results
# <snippet name="tutorials/quickstart/quickstart.py view_results">
print(results.describe())
# </snippet>
