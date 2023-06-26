# <snippet name="tests/integration/docusaurus/expectations/advanced/data_assistant_cross_table_comparison.py imports">
import great_expectations as gx

context = gx.get_context()
# </snippet>

# This utility is not for general use. It is only to support testing.
from tests.test_utils import load_data_into_test_database

# The following load & config blocks up until the batch requests are only to support testing.
MYSQL_CONNECTION_STRING = "mysql+pymysql://root@localhost/test_ci"

PG_CONNECTION_STRING = "postgresql+psycopg2://postgres:@localhost/test_ci"

# Only try to drop mysql table if it exists. This is due to table not found errors experienced
# when using DROP TABLE IF EXISTS with mysql.
import sqlalchemy as sa
from sqlalchemy import inspect

engine = sa.create_engine(MYSQL_CONNECTION_STRING)

inspector = inspect(engine)
table_names = [table_name for table_name in inspector.get_table_names(schema="test_ci")]
drop_existing_table = "mysql_taxi_data" in table_names

load_data_into_test_database(
    table_name="mysql_taxi_data",
    csv_path="./data/yellow_tripdata_sample_2019-01.csv",
    connection_string=MYSQL_CONNECTION_STRING,
    drop_existing_table=drop_existing_table,
)

load_data_into_test_database(
    table_name="postgres_taxi_data",
    csv_path="./data/yellow_tripdata_sample_2019-01.csv",
    connection_string=PG_CONNECTION_STRING,
)

# Table should now exist in mysql
inspector = inspect(engine)
table_names = [table_name for table_name in inspector.get_table_names(schema="test_ci")]
assert "mysql_taxi_data" in table_names

pg_datasource = context.sources.add_sql(
    name="pg_datasource", connection_string=PG_CONNECTION_STRING
)
pg_datasource.add_table_asset(
    name="postgres_taxi_data", table_name="postgres_taxi_data"
)

mysql_datasource = context.sources.add_sql(
    name="mysql_datasource", connection_string=MYSQL_CONNECTION_STRING
)
mysql_datasource.add_table_asset(name="mysql_taxi_data", table_name="mysql_taxi_data")

# Tutorial content resumes here.
# <snippet name="tests/integration/docusaurus/expectations/advanced/data_assistant_cross_table_comparison.py mysql_batch_request">
mysql_batch_request = mysql_datasource.get_asset(
    "mysql_taxi_data"
).build_batch_request()
# </snippet>
# <snippet name="tests/integration/docusaurus/expectations/advanced/data_assistant_cross_table_comparison.py pg_batch_request">
pg_batch_request = pg_datasource.get_asset("postgres_taxi_data").build_batch_request()
# </snippet>
# <snippet name="tests/integration/docusaurus/expectations/advanced/data_assistant_cross_table_comparison.py run_assistant">
data_assistant_result = context.assistants.onboarding.run(
    batch_request=pg_batch_request, exclude_column_names=["VendorID"]
)
# </snippet>
# <snippet name="tests/integration/docusaurus/expectations/advanced/data_assistant_cross_table_comparison.py build_suite">
expectation_suite_name = "compare_two_tables"
expectation_suite = data_assistant_result.get_expectation_suite(
    expectation_suite_name=expectation_suite_name
)
context.add_or_update_expectation_suite(expectation_suite=expectation_suite)
# </snippet>
# <snippet name="tests/integration/docusaurus/expectations/advanced/data_assistant_cross_table_comparison.py checkpoint_config">
checkpoint = gx.checkpoint.SimpleCheckpoint(
    name="comparison_checkpoint",
    data_context=context,
    validations=[
        {
            "batch_request": pg_batch_request,
            "expectation_suite_name": expectation_suite_name,
        }
    ],
)
# </snippet>
# <snippet name="tests/integration/docusaurus/expectations/advanced/data_assistant_cross_table_comparison.py run_checkpoint">
checkpoint_result = checkpoint.run()
# </snippet>

# Note to users: code below this line is only for integration testing -- ignore!

assert checkpoint_result["success"] is True
statistics = checkpoint_result["run_results"][
    list(checkpoint_result["run_results"].keys())[0]
]["validation_result"]["statistics"]
assert statistics["evaluated_expectations"] != 0
assert statistics["evaluated_expectations"] == statistics["successful_expectations"]
assert statistics["unsuccessful_expectations"] == 0
assert statistics["success_percent"] == 100.0
