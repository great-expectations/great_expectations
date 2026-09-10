"""
Example: Sync-based workflow for storing GX artifacts in Google Cloud Storage.

To test, run:
pytest --docs-tests -k "docs_example_cloud_store_backend_gcs" tests/integration/test_script_runner.py
"""

import uuid


def set_up_context_for_example(context):
    """Create a minimal GX setup for the example."""
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
    expectation_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="pickup_datetime")
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
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/cloud_store_backend_sync/_examples/cloud_store_backend_gcs.py - full code example">
import great_expectations as gx
from great_expectations.core.run_identifier import RunIdentifier

context = gx.get_context(mode="file")
# Hide this
set_up_context_for_example(context)

# Step 1: Pull config from GCS (simulated for this example)
# In production, you would run: gsutil -m cp -r gs://my-gx-bucket/gx/ ./gx/
BUCKET_NAME = "my-gx-bucket"
SUITE_NAME = "my_expectation_suite"

# Step 2: Run validation locally
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/cloud_store_backend_sync/_examples/cloud_store_backend_gcs.py - run validation locally">
validation_definition = context.validation_definitions.get("my_validation_definition")
run_name = f"{SUITE_NAME}_{uuid.uuid4().hex[:8]}"
result = validation_definition.run(run_id=RunIdentifier(run_name=run_name))
# </snippet>

print(f"Validation {'succeeded' if result.success else 'failed'}")
print(f"Run name: {run_name}")

# Step 3: Push results back to GCS with unique key
# In production, you would run:
# gsutil -m cp -r gx/uncommitted/validations/{SUITE_NAME}/{run_name}/ gs://{BUCKET_NAME}/gx/uncommitted/validations/{SUITE_NAME}/{run_name}/

# Step 4: Optional - Build and upload Data Docs
# context.build_data_docs()
# gsutil -m cp -r gx/uncommitted/data_docs/local_site/ gs://{BUCKET_NAME}/gx/uncommitted/data_docs/local_site/
# </snippet>
