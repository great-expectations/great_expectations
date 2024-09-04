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

import great_expectations as gx
import great_expectations.expectations as gxe
from tests.test_utils import load_data_into_test_database

CONNECTION_STRING = "postgresql+psycopg2://postgres:@localhost/test_ci"

GX_ROOT_DIR = pathlib.Path(gx.__file__).parent.parent

# Add test data to database for testing.
load_data_into_test_database(
    table_name="transfers",
    csv_path=str(
        GX_ROOT_DIR / "tests/test_sets/learn_data_quality_use_cases/missingness.csv"
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

suite = context.suites.add(gx.ExpectationSuite(name="example missingness expectations"))

#############################
# Start Expectation snippets.

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/missingness_resources/missingness_expectations.py ExpectColumnValuesToBeNull">
    gxe.ExpectColumnValuesToBeNull(column="errors")
    # </snippet>
)

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/missingness_resources/missing_expectations.py ExpectColumnValuesToNotBeNull">
    gxe.ExpectColumnValuesToNotBeNull(column="transfer_amount")
    # </snippet>
)

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/missingness_resources/missingness_expectations.py intermittent_missing_values">
    gxe.ExpectColumnValuesToNotBeNull(column="transfer_date", mostly=0.999)
    # </snippet>
)

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/missingness_resources/missingness_expectations.py missing_critical_data">
    gxe.ExpectColumnValuesToNotBeNull(column="transfer_amount")
    # </snippet>
)

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/missingness_resources/missingness_expectations.py new_cases_appear">
    gxe.ExpectColumnValuesToNotBeNull(
        column="type",
        mostly=0.95,  # Allows for up to 5% of values to be NULL
    )
    # </snippet>
)

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/missingness_resources/missingness_expectations.py incorrectly_defaulted">
    gxe.ExpectColumnValuesToBeNull(column="sender_account_number", mostly=0.001)
    # </snippet>
)

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/missingness_resources/missingness_expectations.py system_anomalies">
    gxe.ExpectColumnValuesToBeNull(column="errors")
    # </snippet>
)

results = batch.validate(suite)
