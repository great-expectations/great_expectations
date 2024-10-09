"""
To run this test locally, run:
1. Activate docker.
2. From the repo root dir, activate the postgresql database docker container:

pytest  --postgresql --docs-tests -k "create_a_batch_definition_postgres" tests/integration/test_script_runner.py
"""

# The following import and setup method sets up the data and environment used to test this example.
# It can be disregarded by anyone using referencing this script as an example of a
# GX workflow.
from docs.docusaurus.docs.components._testing.utility_scripts.postgres_preconfigured_data_asset import (
    setup,
)

setup()

# <snippet name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_batch_definition/_create_a_batch_definition.md full example">
import great_expectations as gx

context = gx.get_context()
# <snippet name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_asset/create_a_data_asset.py retrieve a Data Asset">

# Retrieve a Data Source
datasource_name = "my_datasource"
data_source = context.data_sources.get(datasource_name)

# Get the Data Asset from the Data Source
asset_name = "MY_TABLE_ASSET"
data_asset = data_source.get_asset(asset_name)
# </snippet>

# Example of a full table Batch Definition
# highlight-start
# <snippet name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_batch_definition/_create_a_batch_definition.md full table batch definition">
full_table_batch_definition = data_asset.add_batch_definition_whole_table(
    name="FULL_TABLE"
)
# </snippet>
# highlight-end

# Verify that the Batch Definition is valid
# <snippet name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_batch_definition/_create_a_batch_definition.md verify full table">
full_table_batch = full_table_batch_definition.get_batch()
full_table_batch.head()
# </snippet>

# Examples of partitioned Batch Definitions
# highlight-start
# <snippet name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_batch_definition/_create_a_batch_definition.md daily batch definition">
date_column = "pickup_datetime"
daily_batch_definition = data_asset.add_batch_definition_daily(
    name="DAILY", column=date_column
)
monthly_batch_definition = data_asset.add_batch_definition_monthly(
    name="MONTHLY", column=date_column
)
yearly_batch_definition = data_asset.add_batch_definition_yearly(
    name="YEARLY", column=date_column
)
# </snippet>
# highlight-end

# Verify that the partitioned Batch Definitions are valid
# <snippet name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_batch_definition/_create_a_batch_definition.md verify daily">
daily_batch = daily_batch_definition.get_batch(
    batch_parameters={"year": 2020, "month": 1, "day": 14}
)
daily_batch.head()

monthly_batch = monthly_batch_definition.get_batch(
    batch_parameters={"year": 2020, "month": 1}
)
monthly_batch.head()

yearly_batch = yearly_batch_definition.get_batch(batch_parameters={"year": 2020})
yearly_batch.head()
# </snippet>
# </snippet>
