# This section loads sample data to use for CI testing of the script.
import pathlib

import great_expectations as gx
from tests.test_utils import load_data_into_test_database

CONNECTION_STRING = "postgresql+psycopg2://postgres:@localhost/test_ci"

GX_ROOT_DIR = pathlib.Path(gx.__file__).parent.parent

test_data_1_path = (
    GX_ROOT_DIR
    / "tests/test_sets/learn_data_quality_use_cases/schema_financial_transfers_1.csv"
)

# Add test data to database for testing.
load_data_into_test_database(
    table_name="transfers_1",
    csv_path=str(test_data_1_path),
    connection_string=CONNECTION_STRING,
)

# <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/schema.py full sample code">
import great_expectations as gx
import great_expectations.expectations as gxe

context = gx.get_context()

# Create the Data Source and Data Assets.
# CONNECTION_STRING contains the connection string to connect to the postgres database.
datasource = context.data_sources.add_postgres(
    "postgres database", connection_string=CONNECTION_STRING
)

data_asset_1 = datasource.add_table_asset(name="data asset 1", table_name="transfers_1")

# Create the Expectation Suite and add an Expectation.
expectation = gxe.ExpectColumnToExist(column="recipient_fullname")

batch_definition_1 = data_asset_1.add_batch_definition_whole_table("batch definition 1")

batch_1 = batch_definition_1.get_batch()
batch_1.validate(expectation)
# </snippet>
