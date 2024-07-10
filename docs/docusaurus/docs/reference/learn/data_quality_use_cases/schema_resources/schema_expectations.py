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
import great_expectations.expectations as gxe
from tests.test_utils import load_data_into_test_database

CONNECTION_STRING = "postgresql+psycopg2://postgres:@localhost/test_ci"

GX_ROOT_DIR = pathlib.Path(gx.__file__).parent.parent

# Add test data to database for testing.
load_data_into_test_database(
    table_name="transfers_1",
    csv_path=str(
        GX_ROOT_DIR
        / "tests/test_sets/learn_data_quality_use_cases/schema_financial_transfers_1.csv"
    ),
    connection_string=CONNECTION_STRING,
)

context = gx.get_context()

datasource = context.data_sources.add_postgres(
    "postgres database", connection_string=CONNECTION_STRING
)

data_asset = datasource.add_table_asset(name="data asset", table_name="transfers_1")
batch_definition = data_asset.add_batch_definition_whole_table("batch definition")
batch = batch_definition.get_batch()

suite = context.suites.add(
    gx.core.expectation_suite.ExpectationSuite(name="example schema expectations")
)

#############################
## Start Expectation snippets.

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/schema_resources/schema_expectations.py ExpectColumnValuesToBeOfType">
    gxe.ExpectColumnValuesToBeOfType(column="transfer_amount", type_="DOUBLE_PRECISION")
    # </snippet>
)

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/schema_resources/schema_expectations.py ExpectColumnValuesToBeInTypeList">
    gxe.ExpectColumnValuesToBeInTypeList(
        column="account_type", type_list=["INTEGER", "STRING"]
    )
    # </snippet>
)

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/schema_resources/schema_expectations.py ExpectColumnToExist">
    gxe.ExpectColumnToExist(column="sender_account_number")
    # </snippet>
)

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/schema_resources/schema_expectations.py ExpectTableColumnCountToEqual">
    gxe.ExpectTableColumnCountToEqual(value=5)
    # </snippet>
)

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/schema_resources/schema_expectations.py ExpectTableColumnsToMatchOrderedList">
    gxe.ExpectTableColumnsToMatchOrderedList(
        [
            "sender_account_number",
            "recipient_account_number",
            "transfer_amount",
            "transfer_date",
        ]
    )
    # </snippet>
)

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/schema_resources/schema_expectations.py ExpectTableColumnsToMatchSet">
    gxe.ExpectTableColumnsToMatchSet(
        column_set=[
            "sender_account_number",
            "recipient_account_number",
            "transfer_amount",
            "transfer_date",
        ],
        exact_match=False,
    )
    # </snippet>
)

suite.add_expectation(
    # <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/schema_resources/schema_expectations.py ExpectTableColumnCountToBeBetween">
    gxe.ExpectTableColumnCountToBeBetween(min_value=6, max_value=8)
    # </snippet>
)

results = batch.validate(suite)
