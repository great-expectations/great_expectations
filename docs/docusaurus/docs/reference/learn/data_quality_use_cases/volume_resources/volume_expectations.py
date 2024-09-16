"""
To run this test locally, use the postgresql database docker container.

1. From the repo root dir, run:
cd assets/docker/postgresql
docker compose up

2. Run the following command from the repo root dir in a second terminal:
pytest --postgresql --docs-tests -k "data_quality_use_case_volume_expectations" tests/integration/test_script_runner.py
"""

# This section loads sample data to use for CI testing of the script.
import pathlib

import great_expectations as gx
import great_expectations.expectations as gxe
from tests.test_utils import load_data_into_test_database

CONNECTION_STRING = "postgresql+psycopg2://postgres:@localhost/test_ci"

GX_ROOT_DIR = pathlib.Path(gx.__file__).parent.parent

# Add test data to database for testing.
load_data_into_test_database(
    table_name="transfers",
    csv_path=str(
        GX_ROOT_DIR
        / "tests/test_sets/learn_data_quality_use_cases/volume_financial_transfers.csv"
    ),
    connection_string=CONNECTION_STRING,
)

context = gx.get_context()

datasource = context.data_sources.add_postgres(
    "postgres database", connection_string=CONNECTION_STRING
)

data_asset = datasource.add_table_asset(name="data asset", table_name="transfers")
batch_definition = data_asset.add_batch_definition_whole_table("batch definition")
batch = batch_definition.get_batch()

suite = context.suites.add(gx.ExpectationSuite(name="example volume expectations"))

#############################
# Start Expectation snippets.

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/volume_resources/volume_expectations.py ExpectTableRowCountToBeBetween">
    gxe.ExpectTableRowCountToBeBetween(min_value=2, max_value=5)
    # </snippet>
)

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/volume_resources/volume_expectations.py ExpectTableRowCountToEqual">
    gxe.ExpectTableRowCountToEqual(value=4)
    # </snippet>
)

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/volume_resources/volume_expectations.py ExpectTableRowCountToEqualOtherTable">
    gxe.ExpectTableRowCountToEqualOtherTable(other_table_name="transactions_summary")
    # </snippet>
)

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/volume_resources/volume_expectations.py reconciliation_across_systems">
    gxe.ExpectTableRowCountToEqualOtherTable(
        other_table_name="target_system_transactions"
    )
    # </snippet>
)

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/volume_resources/volume_expectations.py monitoring_streaming_pipelines">
    gxe.ExpectTableRowCountToBeBetween(min_value=2, max_value=5)
    # </snippet>
)

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/volume_resources/volume_expectations.py batch_processing_verification">
    gxe.ExpectTableRowCountToEqual(value=4)
    # </snippet>
)

results = batch.validate(suite)
