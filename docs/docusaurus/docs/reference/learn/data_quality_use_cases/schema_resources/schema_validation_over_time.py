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

import psycopg2

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
        / "tests/test_sets/learn_data_quality_use_cases/schema_financial_transfers.csv"
    ),
    connection_string=CONNECTION_STRING,
)

def add_column_to_transfers_table() -> None:
    connection = psycopg2.connect("host=db dbname=gx_example_db user=example_user")
    query = "ALTER TABLE transfers ADD COLUMN recipient_account_number INTEGER"
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    connection.close()


# <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/schema_resources/schema_validation_over_time.py full sample code">
import pandas as pd

import great_expectations as gx

# Create Data Context.
context = gx.get_context()

# Create Data Source and Data Asset.
# CONNECTION_STRING contains the connection string for the Postgres database.
datasource = context.data_sources.add_postgres(
    "postgres database", connection_string=CONNECTION_STRING
)

data_asset = datasource.add_table_asset(name="data asset", table_name="transfers")

# Create Expectation Suite and add Expectations.
suite = context.suites.add(
    gx.core.expectation_suite.ExpectationSuite(name="schema expectations")
)

suite.add_expectation(
    gxe.ExpectTableColumnsToMatchSet(
        column_set=[
            "type",
            "sender_account_number",
            "recipient_account_number",
            "transfer_amount",
            "transfer_date",
        ]
    )
)
suite.add_expectation(gxe.ExpectTableColumnCountToEqual(value=5))

# Create Batch Definition.
batch_definition = data_asset.add_batch_definition_whole_table("batch definition")
batch = batch_definition.get_batch()

# Validate Batch.
validation_definition = gx.core.validation_definition.ValidationDefinition(
    name="validation definition",
    data=batch_definition,
    suite=suite,
)

# Define and run Checkpoint.
checkpoint = context.checkpoints.add(
    gx.checkpoint.checkpoint.Checkpoint(
        name="checkpoint",
        validation_definitions=[validation_definition]
    )
)

checkpoint.run()

# Add a column to alter the table schema.
# update_table_schema() updates the underlying transfers table.
add_column_to_transfers_table()

# Rerun the Checkpoint.
checkpoint.run()

# Fetch and display Validation Results.
validation_results = context.validation_results_store.get_all()

validation_results = []

for x in context.validation_results_store.get_all():
    validation_results.append(
        {
            "validation_timestamp": x["meta"]["run_id"]["run_time"],
            "success": x["success"],
            "evaluated_expectations": x["statistics"]["evaluated_expectations"],
            "passed_expectations": x["statistics"]["successful_expectations"],
            "failure_rate": 1 - (x["statistics"]["success_percent"]/100),
        }
    )

pd.DataFrame(validation_results)
# </snippet>

# Check output matches what is in the docs.
first_validation = validation_results.iloc[0]
second_validation = validation_results.iloc[1]

assert first_validation["success"] == True
assert first_validation["evaluated_expectations"] == 2
assert first_validation["passed_expectations"] == 2
assert first_validation["failure_rate"] == 0

assert second_validation["success"] == False
assert second_validation["evaluated_expectations"] == 2
assert second_validation["passed_expectations"] == 1
assert second_validation["failure_rate"] == 0.5