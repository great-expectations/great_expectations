"""
This is an example script for how to retrieve a Batch of sample data from a Batch Definition, for use in testing individual Expectations or engaging in data exploration.

To test, run:
pytest --docs-tests -k "doc_example_retrieve_a_batch_of_test_data_from_batch_definition" tests/integration/test_script_runner.py
"""


def set_up_context_for_example(context):
    # Create the Data Source
    source_folder = "./data/folder_with_data"
    data_source_name = "my_data_source"
    data_source = context.data_sources.add_pandas_filesystem(
        name=data_source_name, base_directory=source_folder
    )
    assert data_source.name == data_source_name

    # Add a Data Asset
    asset_name = "my_data_asset"
    data_asset = data_source.add_csv_asset(name=asset_name)
    assert data_asset.name == asset_name

    # Add a Batch Definition
    batch_definition_name = "my_batch_definition"
    batch_definition_regex = (
        r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
    )
    batch_definition = data_asset.add_batch_definition_monthly(
        name=batch_definition_name, regex=batch_definition_regex
    )
    assert batch_definition.name == batch_definition_name


# EXAMPLE SCRIPT STARTS HERE:
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/retrieve_a_batch_of_test_data_from_a_batch_definition.py - full code example">
import great_expectations as gx

context = gx.get_context()
# Hide this
set_up_context_for_example(context)

# Retrieve the Batch Definition:
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/retrieve_a_batch_of_test_data_from_a_batch_definition.py - retrieve Batch Definition">
data_source_name = "my_data_source"
data_asset_name = "my_data_asset"
batch_definition_name = "my_batch_definition"
batch_definition = (
    context.data_sources.get(data_source_name)
    .get_asset(data_asset_name)
    .get_batch_definition(batch_definition_name)
)
# </snippet>

# Retrieve the first valid Batch of data:
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/retrieve_a_batch_of_test_data_from_a_batch_definition.py - retrieve most recent Batch">
batch = batch_definition.get_batch()
# </snippet>

# Or use a Batch Parameter dictionary to specify a Batch to retrieve
# These are sample Batch Parameter dictionaries:
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/retrieve_a_batch_of_test_data_from_a_batch_definition.py - sample Batch Parameter dictionaries">
yearly_batch_parameters = {"year": "2019"}
monthly_batch_parameters = {"year": "2019", "month": "01"}
daily_batch_parameters = {"year": "2019", "month": "01", "day": "01"}
# </snippet>

# This code retrieves the Batch from a monthly Batch Definition:
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/retrieve_a_batch_of_test_data_from_a_batch_definition.py - retrieve specific Batch">

batch = batch_definition.get_batch(batch_parameters={"year": "2019", "month": "01"})
# </snippet>

# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/retrieve_a_batch_of_test_data_from_a_batch_definition.py - verify populated Batch">
print(batch.head())
# </snippet>
# </snippet>
