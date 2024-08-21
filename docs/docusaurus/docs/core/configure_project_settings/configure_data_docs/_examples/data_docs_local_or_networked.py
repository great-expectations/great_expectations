"""
This is an example script for how to configure Data Docs in a local or networked filesystem environment.

To test, run:
pytest --docs-tests -k "docs_example_configure_data_docs_filesystem" tests/integration/test_script_runner.py
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
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_local_or_networked.py - full code example">
import great_expectations as gx

context = gx.get_context(mode="file")
# Hide this
set_up_context_for_example(context)

# Define a Data Docs site configuration dictionary
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_local_or_networked.py - define a data docs config dictionary">
base_directory = "uncommitted/data_docs/local_site/"  # this is the default path (relative to the root folder of the Data Context) but can be changed as required
site_config = {
    "class_name": "SiteBuilder",
    "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
    "store_backend": {
        "class_name": "TupleFilesystemStoreBackend",
        "base_directory": base_directory,
    },
}
# </snippet>

# Add the Data Docs configuration to the Data Context
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_local_or_networked.py - add data docs config to Data Context">
site_name = "my_data_docs_site"
context.add_data_docs_site(site_name=site_name, site_config=site_config)
# </snippet>

# Manually build the Data Docs
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_local_or_networked.py - manually build Data Docs">
context.build_data_docs(site_names=site_name)
# </snippet>

# Automate Data Docs updates with a Checkpoint Action
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_local_or_networked.py - automate data docs with a Checkpoint Action">
checkpoint_name = "my_checkpoint"
validation_definition_name = "my_validation_definition"
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
# </snippet>

# View the Data Docs
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_local_or_networked.py - view data docs">
context.open_data_docs()
# </snippet>
# </snippet>
