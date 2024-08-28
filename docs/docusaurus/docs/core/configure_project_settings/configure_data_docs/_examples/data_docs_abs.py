"""
This is an example script for how to configure Data Docs in an Azure Blob Storage environment.

To test, run:

"""


def set_up_context_for_example(context):
    # Create a Batch Definition
    batch_definition = (
        context.data_sources.add_pandas_filesystem(
            name="my_data_source", base_directory="./data/folder_with_data"
        )
        .add_csv_asset(name="my_data_asset")
        .add_batch_definition_path(
            name="my_batch_definition", path="yellow_tripdata_sample_2019-01.csv"
        )
    )

    # Create an Expectation Suite
    expectation_suite = context.suites.add(
        gx.ExpectationSuite(name="my_expectation_suite")
    )
    # Add some Expectations
    expectation_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="pickup_datetime")
    )
    expectation_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="passenger_count")
    )

    # Create a Validation Definition
    context.validation_definitions.add(
        gx.ValidationDefinition(
            data=batch_definition,
            suite=expectation_suite,
            name="my_validation_definition",
        )
    )


# EXAMPLE SCRIPT STARTS HERE:
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_abs.py - full code example">
import great_expectations as gx

context = gx.get_context(mode="file")
# Hide this
set_up_context_for_example(context)

# Define a Data Docs configuration dictionary
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_abs.py - define a Data Docs configuratin dictionary">
container = "my_abs_container"
prefix = "data_docs/"
connection_string = "${AZURE_STORAGE_CONNECTION_STRING}"  # This uses string substitution to get the actual connection string from an environment variable or config file.

site_config = {
    "class_name": "SiteBuilder",
    "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
    "store_backend": {
        "class_name": "TupleAzureBlobStoreBackend",
        "container": container,
        "prefix": prefix,
        "connection_string": connection_string,
    },
}
# </snippet>

# Add the Data Docs configuration to the Data Context
site_name = "my_data_docs_site"
context.add_data_docs_site(site_name=site_name, site_config=site_config)

# Manually build the Data Docs
context.build_data_docs(site_names=site_name)

# Automate Data Docs updates with a Checkpoint Action
checkpoint_name = "my_checkpoint"
validation_definition_name = "my_validaton_definition"
validation_definition = context.validation_definitions.get(validation_definition_name)
actions = [
    gx.checkpoint.actions.UpdateDataDocsAction(
        name="update_my_site", site_names=[site_name]
    )
]
checkpoint = context.checkpoints.add(
    gx.Checkpoint(
        name=checkpoint_name,
        validation_definitions=[validation_definition],
        actions=actions,
    )
)

result = checkpoint.run()

# View the Data Docs
context.open_data_docs()
# </snippet>
