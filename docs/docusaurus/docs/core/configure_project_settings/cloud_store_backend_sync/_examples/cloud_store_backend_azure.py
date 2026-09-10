"""
Example: Sync-based workflow for storing GX artifacts in Azure Blob Storage.

To test, run:
pytest --docs-tests -k "docs_example_cloud_store_backend_azure" tests/integration/test_script_runner.py
"""

import uuid


def set_up_context_for_example(context):
    """Create a minimal GX setup for the example."""
    batch_definition = (
        context.data_sources.add_pandas_filesystem(
            name="my_data_source", base_directory="./data/folder_with_data"
        )
        .add_csv_asset(name="my_data_asset")
        .add_batch_definition_path(
            name="my_batch_definition", path="yellow_tripdata_sample_2019-01.csv"
        )
    )

    expectation_suite = context.suites.add(
        gx.ExpectationSuite(name="my_expectation_suite")
    )
    expectation_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="pickup_datetime")
    )

    context.validation_definitions.add(
        gx.ValidationDefinition(
            data=batch_definition,
            suite=expectation_suite,
            name="my_validation_definition",
        )
    )


# EXAMPLE SCRIPT STARTS HERE:
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/cloud_store_backend_sync/_examples/cloud_store_backend_azure.py - full code example">
import great_expectations as gx
from great_expectations.core.run_identifier import RunIdentifier

context = gx.get_context(mode="file")
# Hide this
set_up_context_for_example(context)

# Step 1: Pull config from Azure Blob (simulated)
# In production: az storage blob download-batch --account-name myaccount --source my-gx-container --pattern gx/* --destination ./
ACCOUNT_NAME = "myaccount"
CONTAINER_NAME = "my-gx-container"
SUITE_NAME = "my_expectation_suite"

# Step 2: Run validation locally
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/cloud_store_backend_sync/_examples/cloud_store_backend_azure.py - run validation locally">
validation_definition = context.validation_definitions.get("my_validation_definition")
run_name = f"{SUITE_NAME}_{uuid.uuid4().hex[:8]}"
result = validation_definition.run(run_id=RunIdentifier(run_name=run_name))
# </snippet>

print(f"Validation {'succeeded' if result.success else 'failed'}")

# Step 3: Push results back to Azure Blob with unique key
# In production:
# az storage blob upload-batch --account-name {ACCOUNT_NAME} --destination {CONTAINER_NAME} --source gx/uncommitted/validations/{SUITE_NAME}/{run_name}/ --destination-path gx/uncommitted/validations/{SUITE_NAME}/{run_name}/

# Step 4: Optional - Build and upload Data Docs
# context.build_data_docs()
# az storage blob upload-batch --account-name {ACCOUNT_NAME} --destination {CONTAINER_NAME} --source gx/uncommitted/data_docs/local_site/ --destination-path gx/uncommitted/data_docs/local_site/
# </snippet>
