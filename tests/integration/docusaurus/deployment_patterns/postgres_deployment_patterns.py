# <snippet name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py imports">
import great_expectations as gx

from great_expectations.checkpoint import Checkpoint

# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py set up context">
context = gx.get_context()
# </snippet>

# This utility is not for general use. It is only to support testing.
from tests.test_utils import load_data_into_test_database

# The following load & config blocks up until the batch requests are only to support testing.
PG_CONNECTION_STRING = "postgresql+psycopg2://postgres:@localhost/test_ci"

load_data_into_test_database(
    table_name="postgres_taxi_data",
    csv_path="./data/yellow_tripdata_sample_2019-01.csv",
    connection_string=PG_CONNECTION_STRING,
)

# <snippet name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py add_datasource">
pg_datasource = context.sources.add_sql(
    name="pg_datasource", connection_string=PG_CONNECTION_STRING
)
# </snippet>
# <snippet name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py add_asset">
pg_datasource.add_table_asset(
    name="postgres_taxi_data", table_name="postgres_taxi_data"
)
# </snippet>
# <snippet name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py pg_batch_request">
batch_request = pg_datasource.get_asset("postgres_taxi_data").build_batch_request()
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py get validator">
expectation_suite_name = "insert_your_expectation_suite_name_here"
context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

print(validator.head())
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py add expectations">
validator.expect_column_values_to_not_be_null(column="passenger_count")

validator.expect_column_values_to_be_between(
    column="congestion_surcharge", min_value=0, max_value=1000
)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py save suite">
validator.save_expectation_suite(discard_failed_expectations=False)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py checkpoint config">
my_checkpoint_name = "my_sql_checkpoint"

checkpoint = Checkpoint(
    name=my_checkpoint_name,
    run_name_template="%Y%m%d-%H%M%S-my-run-name-template",
    data_context=context,
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
    action_list=[
        {
            "name": "store_validation_result",
            "action": {"class_name": "StoreValidationResultAction"},
        },
        {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}},
    ],
)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py add checkpoint config">
context.add_or_update_checkpoint(checkpoint=checkpoint)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py run checkpoint">
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
