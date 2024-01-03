"""
This guide demonstrates how to use Great Expectations in a quick-start
notebook environment with SQL. It is useful to explore and understand how Great Expectations
works, using your own data.

Note:
- Do not follow this workflow when you are building a durable workflow because it uses
ephemeral assets.
- Do not use this workflow for embedding in a pipeline or CI system, because it uses an
iterative process for trying and refining expectations.
"""

# <snippet name="tests/integration/docusaurus/tutorials/quickstart/v1_sql_quickstart.py import_gx">
import great_expectations as gx
import great_expectations.expectations as gxe

# </snippet>

# Set up
# NOTE: Context is a singleton now. Once the context has been set instantiated in a session
# <snippet name="tests/integration/docusaurus/tutorials/quickstart/v1_sql_quickstart.py get_context">
context = gx.get_context()
# </snippet>

connection_string = "postgresql://postgres:postgres@localhost:5432/postgres"
# <snippet name="tests/integration/docusaurus/tutorials/quickstart/v1_sql_quickstart.py connect_to_data sql_query">
batch = context.sources.pandas_default.read_sql(
    "SELECT * FROM yellow_tripdata_sample_2019_01", connection_string
)
# </snippet>

# <snippet name="tests/integration/docusaurus/tutorials/quickstart/v1_sql_quickstart.py create_expectation">
expectation = gxe.ExpectColumnValuesToNotBeNull(
    column="pu_datetime",
    notes="These are filtered out upstream, because the entire record is garbage if there is no pu_datetime",
)
batch.validate(expectation)
# Review the results of the expectation! Change parameters as needed.
expectation.mostly = 0.8
batch.validate(expectation)
suite = context.add_expectation_suite("quickstart")
suite.add(expectation)
suite.add(
    gxe.ExpectColumnValuesToBeBetween("passenger_count", min_value=1, max_value=6)
)
# </snippet>

validation_result = batch.validate(suite)

validation_result.open_docs()
