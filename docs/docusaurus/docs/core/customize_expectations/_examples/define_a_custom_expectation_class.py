"""
This is an example script for how to define a Custom Expectation class.

To test, run:
pytest --docs-tests -k "docs_example_define_a_custom_expectation_class" tests/integration/test_script_runner.py
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
# <snippet name="docs/docusaurus/docs/core/customize_expectations/_examples/define_a_custom_expectation_class.py - full code example">
import great_expectations as gx

context = gx.get_context()
# Hide this
set_up_context_for_example(context)


# <snippet name="docs/docusaurus/docs/core/customize_expectations/_examples/define_a_custom_expectation_class.py - define description attribute for a cusom Expectation">
# <snippet name="docs/docusaurus/docs/core/customize_expectations/_examples/define_a_custom_expectation_class.py - define default attributes for a custom Expectation class">
# <snippet name="docs/docusaurus/docs/core/customize_expectations/_examples/define_a_custom_expectation_class.py - define a custom Expectation subclass">
class ExpectValidPassengerCount(gx.expectations.ExpectColumnValuesToBeBetween):
    # </snippet>
    column: str = "passenger_count"
    min_value: int = 1
    max_value: int = 6
    # </snippet>
    description: str = "There should be between **1** and **6** passengers."


# </snippet>

# Create an instance of the custom Expectation
# <snippet name="docs/docusaurus/docs/core/customize_expectations/_examples/define_a_custom_expectation_class.py - instantiate a Custom Expectation">
expectation = ExpectValidPassengerCount()  # Uses the predefined default values
# </snippet>

# Optional. Test the Expectation with some sample data
data_source_name = "my_data_source"
asset_name = "my_data_asset"
batch_definition_name = "my_batch_definition"
batch = (
    context.data_sources.get(data_source_name)
    .get_asset(asset_name)
    .get_batch_definition(batch_definition_name)
    .get_batch()
)

print(batch.validate(expectation))
# </snippet>
