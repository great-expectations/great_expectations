"""
To run this test locally, use the postgresql database docker container.

1. From the repo root dir, run:
cd assets/docker/postgresql
docker compose up

2. Run the following command from the repo root dir in a second terminal:
pytest --postgresql --docs-tests -k "data_quality_use_case_missingness_expectations" tests/integration/test_script_runner.py
"""

# This section loads sample data to use for CI testing of the script.
import pathlib
from datetime import datetime

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

# <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/volume_resources/volume_workflow.py full example code">

context = gx.get_context()
data_source = context.data_sources.add_postgres(
    "postgres database", connection_string=CONNECTION_STRING
)
data_asset = data_source.add_table_asset(name="data asset", table_name="transfers")

# Define a function to extract the transfer date day


def transfer_date_day(batch_spec):
    transfer_date = datetime.strptime(batch_spec["transfer_date"], "%Y-%m-%d")
    return transfer_date.strftime("%Y-%m-%d")


# Add a Batch Definition with a partition key
batch_definition = data_asset.add_batch_definition(
    name="daily_batch",
)

# Create an Expectation Suite
expectation_suite = context.suites.add(
    gx.core.expectation_suite.ExpectationSuite(name="transactions_suite")
)

# Add an Expectation for row count to be between 1 and 5 for each partition
expectation_suite.add_expectation(
    gxe.ExpectTableRowCountToBeBetween(
        min_value=1,
        max_value=5,
        batch_definition_parameters={"transfer_date": transfer_date_day},
    )
)

# Get partitions based on transfer_date
partitions = df.groupby(df["transfer_date"].dt.date)

# Validate each partition
for transfer_date, partition_df in partitions:
    print(f"Validating partition for transfer_date: {transfer_date}")

    # Create a Batch for the partition
    batch = batch_definition.get_batch(
        batch_parameters={"transfer_date": transfer_date.strftime("%Y-%m-%d")},
        dataframe=partition_df,
    )

    # Validate the Batch
    validation_result = batch.validate(expectation_suite)
    print(validation_result.success)
# </snippet>
