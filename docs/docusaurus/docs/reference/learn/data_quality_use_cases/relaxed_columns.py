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

TEST_DATA_SETS = [
    {
        "table_name": "transfers_1",
        "test_data_path": "tests/test_sets/learn_data_quality_use_cases/schema_financial_transfers_1.csv",
    },
    {
        "table_name": "transfers_2",
        "test_data_path": "tests/test_sets/learn_data_quality_use_cases/schema_financial_transfers_2.csv",
    },
]

# Add test data to database for testing.
for test_data_set in TEST_DATA_SETS:
    load_data_into_test_database(
        table_name=test_data_set["table_name"],
        csv_path=str(GX_ROOT_DIR / test_data_set["test_data_path"]),
        connection_string=CONNECTION_STRING,
    )

# <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/relaxed_columns.py relaxed columns sample code">
import great_expectations as gx
import great_expectations.expectations as gxe

context = gx.get_context()

# Create the Data Source and Data Assets.
# CONNECTION_STRING contains the connection string to connect to the postgres database.
datasource = context.data_sources.add_postgres(
    "postgres database", connection_string=CONNECTION_STRING
)

data_asset_1 = datasource.add_table_asset(name="data asset 1", table_name="transfers_1")

data_asset_2 = datasource.add_table_asset(name="data asset 2", table_name="transfers_2")

# Create the Expectation Suite and add an Expectation.
suite = context.suites.add(
    gx.core.expectation_suite.ExpectationSuite(name="relaxed column order")
)

suite.add_expectation(
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

# Create the Batch Definitions.
batch_definition_1 = data_asset_1.add_batch_definition_whole_table("batch definition 1")
batch_1 = batch_definition_1.get_batch()

batch_definition_2 = data_asset_1.add_batch_definition_whole_table("batch definition 2")
batch_2 = batch_definition_2.get_batch()

# Validate Batches using the Expectation Suite.
results_1 = batch_1.validate(suite)
results_2 = batch_2.validate(suite)

print(f"Validation results 1:\n{results_1}")
print(f"Validation results 2:\n{results_2}")
# </snippet>
