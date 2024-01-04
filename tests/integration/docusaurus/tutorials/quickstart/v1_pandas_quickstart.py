"""
This guide demonstrates how to use Great Expectations in a quick-start
notebook environment with Pandas. It is useful to explore and understand how Great Expectations
works, using your own data.

Note:
- Do not follow this workflow when you are building a durable workflow because it uses
ephemeral assets.
- Do not use this workflow for embedding in a pipeline or CI system, because it uses an
iterative process for trying and refining expectations.
"""


# <snippet name="tests/integration/docusaurus/tutorials/quickstart/v1_pandas_quickstart.py import_gx">
import great_expectations as gx
import great_expectations.expectations as gxe

# </snippet>

# Set up
# NOTE: Context is a singleton now. Once the context has been set instantiated in a session
# <snippet name="tests/integration/docusaurus/tutorials/quickstart/v1_pandas_quickstart.py get_context">
context = gx.get_context()
# </snippet>

# <snippet name="tests/integration/docusaurus/tutorials/quickstart/v1_pandas_quickstart.py connect_to_data">
batch = context.sources.pandas_default.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
)
# </snippet>

# <snippet name="tests/integration/docusaurus/tutorials/quickstart/v1_pandas_quickstart.py create_expectation">
expectation = gxe.ExpectColumnValuesToNotBeNull(
    column="pickup_datetime",
    notes="These are filtered out upstream, because the entire record is garbage if there is no pickup_datetime",
)
result = batch.validate(expectation)
# </snippet>
assert result.success

# <snippet name="tests/integration/docusaurus/tutorials/quickstart/v1_pandas_quickstart.py update_expectation">
# Review the results of the expectation! Change parameters as needed.
expectation.mostly = 0.8
result = batch.validate(expectation)

suite = context.add_expectation_suite("quickstart")
suite.add(expectation)
suite.add(
    gxe.ExpectColumnValuesToBeBetween(
        column="passenger_count", min_value=1, max_value=6
    )
)
# </snippet>
assert result.success
assert result.expectation_config.kwargs["mostly"] == 0.8

suite_result = batch.validate(suite)
assert suite_result.success

# TODO: Need to implement this
# validation_result.open_docs()
