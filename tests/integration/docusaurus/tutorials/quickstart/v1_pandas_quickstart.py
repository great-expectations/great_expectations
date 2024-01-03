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

import pandas as pd

# <snippet name="tests/integration/docusaurus/tutorials/quickstart/v1_pandas_quickstart.py import_gx">
import great_expectations as gx
import great_expectations.expectations as gxe

# </snippet>

# Set up
# NOTE: Context is a singleton now. Once the context has been set instantiated in a session
# <snippet name="tests/integration/docusaurus/tutorials/quickstart/v1_pandas_quickstart.py get_context">
context = gx.get_context()
# </snippet>

batches = []

# <snippet name="tests/integration/docusaurus/tutorials/quickstart/v1_pandas_quickstart.py connect_to_data pandas_csv">
batch = context.sources.pandas_default.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
)
# </snippet>

batches.append(batch)

df = pd.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
)
# <snippet name="tests/integration/docusaurus/tutorials/quickstart/v1_pandas_quickstart.py connect_to_data pandas_dataframe">
batch = context.sources.pandas_default.from_dataframe(df)
# </snippet>

batches.append(batch)

for batch in batches:
    # <snippet name="tests/integration/docusaurus/tutorials/quickstart/v1_pandas_quickstart.py create_expectation">
    expectation = gxe.ExpectColumnValuesToNotBeNull(
        "pu_datetime",
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
