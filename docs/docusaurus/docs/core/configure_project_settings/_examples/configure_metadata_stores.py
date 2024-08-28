"""
This is an example script for how to configure Metadata Stores using Data Context variables.

To test, run:
pytest --docs-tests -k "docs_example_configure_metadata_stores" tests/integration/test_script_runner.py
"""


def set_up_context_for_example(context):
    pass


# EXAMPLE SCRIPT STARTS HERE:
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/_examples/configure_metadata_stores.py - full code example">
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/_examples/configure_metadata_stores.py - retrieve a File Data Context">
import great_expectations as gx

# Load a File Data Context
context = gx.get_context(mode="file")
# </snippet>
# Hide this
set_up_context_for_example(context)

# Access the Stores through the Data Context's `variables` attribute:
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/_examples/configure_metadata_stores.py - access Metadata Store configurations">
print(context.variables.config.stores["expectations_store"])
print(context.variables.config.stores["validation_definition_store"])
print(context.variables.config.stores["checkpoint_store"])
print(context.variables.config.stores["validation_results_store"])
# </snippet>

# Update the path of the Data Context's Expectations Store:
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/_examples/configure_metadata_stores.py - example update Expectations Store base directory">
expectation_store_directory = "my_expectations_store/"
context.variables.config.stores["expectations_store"]["store_backend"][
    "base_directory"
] = expectation_store_directory
# </snippet>

# Save changes to the Data Context's configuration:
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/_examples/configure_metadata_stores.py - save changes to the Data Context">
context.variables.save()
# </snippet>

# Re-initialize the Data Context:
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/_examples/configure_metadata_stores.py - re-initialize the Data Context">
context = gx.get_context(mode="file")
# </snippet>
# </snippet>

assert (
    context.variables.config.stores["expectations_store"]["store_backend"][
        "base_directory"
    ]
    == expectation_store_directory
)
