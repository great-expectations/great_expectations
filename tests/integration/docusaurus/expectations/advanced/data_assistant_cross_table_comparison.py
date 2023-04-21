# <snippet name="tests/integration/docusaurus/expectations/advanced/data_assistant_cross_table_comparison.py imports">
import great_expectations as gx
from great_expectations.core.batch import BatchRequest
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.rule_based_profiler.data_assistant.onboarding_data_assistant import (
    OnboardingDataAssistant,
)

context = gx.get_context()
# </snippet>

# This utility is not for general use. It is only to support testing.
from tests.test_utils import load_data_into_test_database

# The following load & config blocks up until the batch requests are only to support testing.
MYSQL_CONNECTION_STRING = "mysql+pymysql://root@localhost/test_ci"

PG_CONNECTION_STRING = "postgresql+psycopg2://postgres:@localhost/test_ci"

load_data_into_test_database(
    table_name="mysql_taxi_data",
    csv_path="./data/yellow_tripdata_sample_2019-01.csv",
    connection_string=MYSQL_CONNECTION_STRING,
)

load_data_into_test_database(
    table_name="postgres_taxi_data",
    csv_path="./data/yellow_tripdata_sample_2019-01.csv",
    connection_string=PG_CONNECTION_STRING,
)

pg_datasource = context.sources.add_sql(name="pg_datasource", connection_string=PG_CONNECTION_STRING)
pg_datasource.add_table_asset(name="postgres_taxi_data", table_name="postgres_taxi_data")

mysql_datasource = context.sources.add_sql(name="mysql_datasource", connection_string=MYSQL_CONNECTION_STRING)
mysql_datasource.add_table_asset(name="mysql_taxi_data", table_name="mysql_taxi_data")

# Tutorial content resumes here.
# <snippet name="tests/integration/docusaurus/expectations/advanced/data_assistant_cross_table_comparison.py mysql_batch_request">
mysql_batch_request = mysql_datasource.get_asset('mysql_taxi_data').build_batch_request()
# </snippet>
# <snippet name="tests/integration/docusaurus/expectations/advanced/data_assistant_cross_table_comparison.py pg_batch_request">
pg_batch_request = pg_datasource.get_asset('postgres_taxi_data').build_batch_request()
# </snippet>
# <snippet name="tests/integration/docusaurus/expectations/advanced/data_assistant_cross_table_comparison.py run_assistant">
data_assistant_result = context.assistants.onboarding.run(
    batch_request=pg_batch_request,
    excluded_expectations=[
        "expect_column_quantile_values_to_be_between",
        "expect_column_mean_to_be_between",
    ]
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
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name
        }
    ]
)
# </snippet>
# <snippet name="tests/integration/docusaurus/expectations/advanced/data_assistant_cross_table_comparison.py run_checkpoint">
checkpoint_result = checkpoint.run()
# </snippet>

# Note to users: code below this line is only for integration testing -- ignore!

assert results["success"] is True
statistics = results["run_results"][list(results["run_results"].keys())[0]][
    "validation_result"
]["statistics"]
assert statistics["evaluated_expectations"] != 0
assert statistics["evaluated_expectations"] == statistics["successful_expectations"]
assert statistics["unsuccessful_expectations"] == 0
assert statistics["success_percent"] == 100.0
