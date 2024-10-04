"""
This is an example script for how to use SQL to define a custom Expectation.

To test, run:
pytest --docs-tests -k "docs_example_use_sql_to_define_a_custom_expectation" tests/integration/test_script_runner.py
"""


def set_up_context_for_example(context):
    # Create the Data Source
    connection_string = "sqlite:///data/yellow_tripdata.db"
    data_source_name = "my_sql_data_source"
    data_source = context.data_sources.add_sqlite(
        name=data_source_name, connection_string=connection_string
    )
    assert data_source.name == data_source_name

    # Add a Data Asset
    asset_name = "my_data_asset"
    database_table_name = "yellow_tripdata_sample_2019_01"
    data_asset = data_source.add_table_asset(
        table_name=database_table_name, name=asset_name
    )
    assert data_asset.name == asset_name

    # Add a Batch Definition
    batch_definition_name = "my_batch_definition"
    batch_definition = data_asset.add_batch_definition_whole_table(
        batch_definition_name
    )
    assert batch_definition.name == batch_definition_name


# EXAMPLE SCRIPT STARTS HERE:
# <snippet name="docs/docusaurus/docs/core/customize_expectations/_examples/use_sql_to_define_a_custom_expectation.py - full code example">
import great_expectations as gx


# Define a custom Expectation that uses SQL by subclassing UnexpectedRowsExpectation
# <snippet name="docs/docusaurus/docs/core/customize_expectations/_examples/use_sql_to_define_a_custom_expectation.py - define a custom UnexpectedRowsExpectation">
# <snippet name="docs/docusaurus/docs/core/customize_expectations/_examples/use_sql_to_define_a_custom_expectation.py - define the query for an UnexpectedRowsExpectation">
# <snippet name="docs/docusaurus/docs/core/customize_expectations/_examples/use_sql_to_define_a_custom_expectation.py - define a more descriptive name for an UnexpectedRowsExpectation">
class ExpectPassengerCountToBeLegal(gx.expectations.UnexpectedRowsExpectation):
    # </snippet>
    unexpected_rows_query: str = (
        "SELECT * FROM {batch} WHERE passenger_count > 6 or passenger_count < 0"
    )
    # </snippet>
    description: str = "There should be no more than **6** passengers."


# </snippet>

context = gx.get_context()
# Hide this
set_up_context_for_example(context)

# Instantiate the custom Expectation
# <snippet name="docs/docusaurus/docs/core/customize_expectations/_examples/use_sql_to_define_a_custom_expectation.py - instantiate the custom SQL Expectation">
expectation = ExpectPassengerCountToBeLegal()
# </snippet>

# Test the Expectation
data_source_name = "my_sql_data_source"
data_asset_name = "my_data_asset"
batch_definition_name = "my_batch_definition"
batch = (
    context.data_sources.get(data_source_name)
    .get_asset(data_asset_name)
    .get_batch_definition(batch_definition_name)
    .get_batch()
)

batch.validate(expectation)
# </snippet>
