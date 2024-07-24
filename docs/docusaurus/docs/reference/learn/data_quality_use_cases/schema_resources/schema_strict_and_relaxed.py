"""
To run this test locally, use the postgresql database docker container.

1. From the repo root dir, run:
cd assets/docker/postgresql
docker compose up

2. Run the following command from the repo root dir in a second terminal:
pytest --postgresql --docs-tests -k "data_quality_use_case_schema" tests/integration/test_script_runner.py
"""

# This section loads sample data to use for CI testing of the script.
import pathlib

import great_expectations as gx
from tests.test_utils import load_data_into_test_database

CONNECTION_STRING = "postgresql+psycopg2://postgres:@localhost/test_ci"

GX_ROOT_DIR = pathlib.Path(gx.__file__).parent.parent

# Add test data to database for testing.
load_data_into_test_database(
    table_name="transfers",
    csv_path=str(
        GX_ROOT_DIR
        / "tests/test_sets/learn_data_quality_use_cases/schema_financial_transfers.csv"
    ),
    connection_string=CONNECTION_STRING,
)

# <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/schema_resources/schema_strict_and_relaxed.py full sample code">
import great_expectations as gx
import great_expectations.expectations as gxe

context = gx.get_context()

# Create Data Source, Data Asset, and Batch Definition.
# CONNECTION_STRING contains the connection string for the Postgres database.
datasource = context.data_sources.add_postgres(
    "postgres database", connection_string=CONNECTION_STRING
)

data_asset = datasource.add_table_asset(name="data asset", table_name="transfers")

batch_definition = data_asset.add_batch_definition_whole_table("batch definition")

batch = batch_definition.get_batch()

# Create Expectation Suite with strict type and column Expectations. Validate data.
strict_suite = context.suites.add(
    gx.core.expectation_suite.ExpectationSuite(name="strict checks")
)

strict_suite.add_expectation(
    gxe.ExpectTableColumnsToMatchSet(
        column_set=[
            "type",
            "sender_account_number",
            "recipient_fullname",
            "transfer_amount",
            "transfer_date",
        ]
    )
)

strict_suite.add_expectation(
    gxe.ExpectColumnValuesToBeOfType(column="transfer_amount", type_="DOUBLE_PRECISION")
)

strict_results = batch.validate(strict_suite)

# Create Expectation Suite with relaxed type and column Expectations. Validate data.
relaxed_suite = context.suites.add(
    gx.core.expectation_suite.ExpectationSuite(name="relaxed checks")
)

relaxed_suite.add_expectation(
    gxe.ExpectTableColumnsToMatchSet(
        column_set=[
            "type",
            "sender_account_number",
            "transfer_amount",
            "transfer_date",
        ],
        exact_match=False,
    )
)

relaxed_suite.add_expectation(
    gxe.ExpectColumnValuesToBeInTypeList(
        column="transfer_amount", type_list=["DOUBLE_PRECISION", "STRING"]
    )
)

relaxed_results = batch.validate(relaxed_suite)

print(f"Strict validation passes: {strict_results['success']}")
print(f"Relaxed validation passes: {relaxed_results['success']}")
# </snippet>

# Check output matches what is in the docs.
assert strict_results["success"] is True
assert relaxed_results["success"] is True
