# <snippet name="tutorials/quickstart/quickstart.py import_gx">
import great_expectations as gx

# </snippet>

# Set up
# <snippet name="tutorials/quickstart/quickstart.py get_context">
context = gx.get_context()
# </snippet>

# Connect to data
# <snippet name="tutorials/quickstart/quickstart.py connect_to_data">
# what does this return? batch? batch_config with no options?
batch = context.sources.pandas_default.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
)

# What happened in the background?
# Datsource: "default_pandas_datasource"
# Asset: "#ephemeral_pandas_asset" -- CSVAsset
# BatchConfig (Splitters): (none configured)
# BatchOptions: (none needed)

batch = context.sources.postgresql.add_query_asset(
    name="top1000" "SELECT * FROM taxi LIMIT 1000"
).get_batch()

# What happened in the background?
# Datsource: "postgresql"  # had to be configured by user
# Asset: "top1000"
# BatchConfig: (none configured)
# BatchOptions: (none needed)
# TODO: we can use a batchconfig and a data asset interchangeably. That seems a bit messy.
# </snippet>

# Create Expectations
# <snippet name="tutorials/quickstart/quickstart.py create_expectation">
suite = context.add_expectation_suite("quickstart")
# Note that we're using the gx namespace here for all expectations; will require dynamic import at top of package
# Demo beats:
# 1. We can still use our postional arguments
# 2. Notice that the "note" option is now a top-level concern!
expectation = gx.ExpectColumnValuesToNotBeNull(
    "pu_datetime",
    note="These are filtered out upstream, because the entire record is garbage if there is no pu_datetime",
)
batch.validate(expectation)
suite.add_expectation(expectation)
suite.add_expectation(
    # Note: we are removing the option to "auto" configure the expectation
    gx.ExpectColumnValuesToBeBetween("passenger_count", min_value=1, max_value=6)
)
# </snippet>

# Validate data
# <snippet name="tutorials/quickstart/quickstart.py create_checkpoint">
batch_expectations = context.batch_expectations.add(
    name="quickstart",
    expectation_suite=suite,
    batch_config=batch.batch_config,  # TODO: need to resolve in concert with absent batch_config
)
# </snippet>

# <snippet name="tutorials/quickstart/quickstart.py run_checkpoint">
results = batch_expectations.run()
# </snippet>

# View results
# <snippet name="tutorials/quickstart/quickstart.py view_results">
context.view_validation_result(results)
# </snippet>
