"""
This guide demonstrates how to use Great Expectations in a quick-start
notebook environment. It is useful to explore and understand how Great Expectations
works, using your own data.

Note:
- Do not follow this workflow when you are building a durable workflow because it uses
ephemeral assets.
- Do not use this workflow for embedding in a pipeline or CI system, because it uses an
iterative process for trying and refining expectations.
"""

from enum import Enum

# <snippet name="tutorials/quickstart/quickstart.py import_gx">
import great_expectations as gx
import great_expectations.expectations as gxe

# </snippet>

# Set up
# NOTE: Context is a singleton now. Once the context has been set instantiated in a session
# <snippet name="tutorials/quickstart/quickstart.py get_context">
context = gx.get_context()
# </snippet>


class QuickstartDatasourceTabs(Enum):
    PANDAS_CSV = "pandas_csv"
    PANDAS_DATAFRAME = "pandas_dataframe"
    SQL_QUERY = "sql_query"


# TODO: Where in the GX namespace does Batch live?
def get_quickstart_batch(datasource_type: QuickstartDatasourceTabs) -> Batch:
    if datasource_type == QuickstartDatasourceTabs.PANDAS_CSV:
        # <snippet name="tutorials/quickstart/quickstart.py connect_to_data pandas_csv">
        batch = context.sources.pandas_default.read_csv(
            "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
        )
        # </snippet>
        return batch

    elif datasource_type == QuickstartDatasourceTabs.SQL_QUERY:
        connection_string = "postgresql://postgres:postgres@localhost:5432/postgres"
        # <snippet name="tutorials/quickstart/quickstart.py connect_to_data sql_query">
        batch = context.sources.pandas_default.read_sql(
            "SELECT * FROM yellow_tripdata_sample_2019_01", connection_string
        )
        # </snippet>
        return batch
    elif datasource_type == QuickstartDatasourceTabs.PANDAS_DATAFRAME:
        import pandas as pd

        df = pd.read_csv(
            "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
        )
        # <snippet name="tutorials/quickstart/quickstart.py connect_to_data pandas_dataframe">
        batch = context.sources.pandas_default.from_dataframe(df)
        # </snippet>
        return batch


for tab_name in QuickstartDatasourceTabs:
    batch = get_quickstart_batch(datasource_type=tab_name)

# <snippet name="tutorials/quickstart/quickstart.py create_expectation">
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
    # Note: we are removing the option to "auto" configure the expectation
    gxe.ExpectColumnValuesToBeBetween("passenger_count", min_value=1, max_value=6)
)
# </snippet>

validation_result = batch.validate(suite)

# TODO: ticket for cloud UI needs to support ephemeral assets for this to make sense
validation_result.open_docs()
