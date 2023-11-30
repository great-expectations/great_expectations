"""
This workflow demonstrates how to use Great Expectations in a quick-start
notebook environment. It is useful to explore and understand how Great Expectations
works, using your own data.

This workflow is not recommended for use in production environments because:
1. it uses ephemeral assets
2. it uses an iterative workflow of trying and refining expectations.
"""

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
batch = context.sources.pandas_default.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
)

# What happened in the background?
# Datsource: "default_pandas_datasource"
# Asset: "#ephemeral_pandas_asset" -- CSVAsset (path lives here)
# BatchConfig (Splitters): No Splitters
# BatchOptions: (none needed)

# TODO: ticket We can also use a SQL query as a data source
context.sources.add_postgresql(
    name="postgresql", connection_string="postgresql://localhost"
)
batch = context.sources.postgresql.query_batch("SELECT * FROM taxi LIMIT 1000")
# </snippet>

# Create Expectations
# <snippet name="tutorials/quickstart/quickstart.py create_expectation">
# Note that we're using the gx namespace here for all expectations; will require dynamic import at top of package
# Demo beats:
# 1. TODO: Check with champions on positional args -- proposal is to allow positional args only for domain
# 2. Notice that the "notes" option is now a top-level concern!
expectation = gxe.ExpectColumnValuesToNotBeNull(
    "pu_datetime",
    notes="These are filtered out upstream, because the entire record is garbage if there is no pu_datetime",
)
batch.validate(
    expectation
)  # returns a result that we expect them to review in a REPL loop
# Review the results of the expectation! Change parameters as needed.
expectation.mostly = 0.8
batch.validate(expectation)
suite = context.add_expectation_suite("quickstart")
suite.add(expectation)
suite.add(
    # Note: we are removing the option to "auto" configure the expectation
    gxe.ExpectColumnValuesToBeBetween("passenger_count", min_value=1, max_value=6)
)
# </snippet>

validation_result = batch.validate(suite)

# TODO: ticket for cloud UI needs to support ephemeral assets for this to make sense
validation_result.build_docs()
