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
# <snippet name="tutorials/quickstart/quickstart.py get_context">
# NOTE: Context is a singleton now. Once the context has been set instantiated in a session
context = gx.set_context()
# </snippet>

class QuickstartDatasourceTabs(Enum):
    PANDAS_DEFAULT = "pandas_default"
    SQL_QUERY = "sql_query"

# TODO: Where in the GX namespace does Batch live?
def get_quickstart_batch(datasource_type: QuickstartDatasourceTabs) -> Batch:
    if datasource_type == QuickstartDatasourceTabs.PANDAS_DEFAULT:      
        # <snippet name="tutorials/quickstart/quickstart.py connect_to_data pandas_csv">
        batch = context.sources.pandas_default.read_csv(
            "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
        )
        # </snippet>
        return batch
    
    elif datasource_type == QuickstartDatasourceTabs.SQL_QUERY:
        # TODO: add local postgresql db to quickstart? or add public snowflake?
        # <snippet name="tutorials/quickstart/quickstart.py connect_to_data pandas_csv">
        datasource = context.sources.add_postgresql_datasource(
            name = "quickstart_db",
            connection_string = "postgresql://localhost/quickstart",
        )
        batch = datasource.read_query("SELECT * FROM yellow_tripdata_sample_2019_01")
        # </snippet>
        return batch

for tab_name in QuickstartDatasourceTabs:
    batch = get_quickstart_batch(datasource_type=tab_name)

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
validation_result.build_docs()
