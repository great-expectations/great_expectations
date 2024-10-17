"""
This is an example script for how to create a Validation Definition.

To test, run:
pytest --docs-tests -k "docs_example_create_a_validation_definition" tests/integration/test_script_runner.py
"""


def set_up_context_for_example(context):
    # Create a Data Source
    data_source = context.data_sources.add_pandas_filesystem(
        name="my_data_source", base_directory="./data/folder_with_data"
    )
    # Create a Data Asset
    data_asset = data_source.add_csv_asset(name="my_data_asset")
    # Create a Batch Definition
    data_asset.add_batch_definition_path(
        name="my_batch_definition", path="yellow_tripdata_sample_2019-01.csv"
    )
    # Create an Expectation Suite
    suite = context.suites.add(gx.ExpectationSuite(name="my_expectation_suite"))
    # Add some Expectations
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="pickup_datetime")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="passenger_count")
    )


# EXAMPLE SCRIPT STARTS HERE:
# <snippet name="docs/docusaurus/docs/core/run_validations/_examples/create_a_validation_definition.py - full code example">
import great_expectations as gx

context = gx.get_context()
# Hide this
set_up_context_for_example(context)

# Retrieve an Expectation Suite
# <snippet name="docs/docusaurus/docs/core/run_validations/_examples/create_a_validation_definition.py - retrieve an Expectation Suite">
expectation_suite_name = "my_expectation_suite"
expectation_suite = context.suites.get(name=expectation_suite_name)
# </snippet>

# Retrieve a Batch Definition
# <snippet name="docs/docusaurus/docs/core/run_validations/_examples/create_a_validation_definition.py - retrieve a Batch Definition">
data_source_name = "my_data_source"
data_asset_name = "my_data_asset"
batch_definition_name = "my_batch_definition"
batch_definition = (
    context.data_sources.get(data_source_name)
    .get_asset(data_asset_name)
    .get_batch_definition(batch_definition_name)
)
# </snippet>

# Create a Validation Definition
# <snippet name="docs/docusaurus/docs/core/run_validations/_examples/create_a_validation_definition.py - create a Validation Definition">
definition_name = "my_validation_definition"
validation_definition = gx.ValidationDefinition(
    data=batch_definition, suite=expectation_suite, name=definition_name
)
# </snippet>

# Add the Validation Definition to the Data Context
# <snippet name="docs/docusaurus/docs/core/run_validations/_examples/create_a_validation_definition.py - save the Validation Definition to the Data Context">
validation_definition = context.validation_definitions.add(validation_definition)
# </snippet>
# </snippet>
