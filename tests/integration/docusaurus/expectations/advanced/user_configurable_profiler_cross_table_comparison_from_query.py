import os

from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.profile.user_configurable_profiler import (
    UserConfigurableProfiler,
)

# This utility is not for general use. It is only to support testing.
from tests.test_utils import load_data_into_test_database

MY_CONNECTION_STRING = "mysql+pymysql://root:root@localhost/test_ci"

PG_CONNECTION_STRING = "postgresql+psycopg2://postgres:@localhost/test_ci"

load_data_into_test_database(
    table_name="taxi_data",
    csv_path="tests/test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-01.csv",
    connection_string=MY_CONNECTION_STRING,
)

load_data_into_test_database(
    table_name="taxi_data",
    csv_path="tests/test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-01.csv",
    connection_string=PG_CONNECTION_STRING,
)

context = ge.get_context()

pg_datasource_config = {
    "name": "my_postgresql_datasource",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": f"{PG_CONNECTION_STRING}",
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["batch_id"],
        },
    },
}

mysql_datasource_config = {
    "name": "my_mysql_datasource",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": f"{MY_CONNECTION_STRING}",
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["batch_id"],
        },
    },
}

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
pg_datasource_config["execution_engine"]["connection_string"] = PG_CONNECTION_STRING

context.test_yaml_config(yaml.dump(pg_datasource_config))

context.add_datasource(**pg_datasource_config)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
mysql_datasource_config["execution_engine"]["connection_string"] = MY_CONNECTION_STRING

context.test_yaml_config(yaml.dump(mysql_datasource_config))

context.add_datasource(**mysql_datasource_config)

mysql_runtime_batch_request = RuntimeBatchRequest(
    datasource_name="my_mysql_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="mysql_asset",
    runtime_parameters={"query": "SELECT * from taxi_data LIMIT 10"},
    batch_identifiers={"batch_id": "default_identifier"},
)

pg_runtime_batch_request = RuntimeBatchRequest(
    datasource_name="my_postgresql_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="postgres_asset",
    runtime_parameters={"query": "SELECT * from taxi_data LIMIT 10"},
    batch_identifiers={"batch_id": "default_identifier"},
)

expectation_suite_name = "compare_two_tables"

validator = context.get_validator(
    batch_request=mysql_runtime_batch_request,
)

profiler = UserConfigurableProfiler(
    profile_dataset=validator,
    excluded_expectations=["expect_column_quantile_values_to_be_between"],
)

suite = profiler.build_suite()
context.save_expectation_suite(
    expectation_suite=suite, expectation_suite_name=expectation_suite_name
)

my_checkpoint_name = "comparison_checkpoint"

yaml_config = f"""
name: {my_checkpoint_name}
config_version: 1.0
class_name: SimpleCheckpoint
run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
expectation_suite_name: {expectation_suite_name}
"""

context.add_checkpoint(**yaml.load(yaml_config))

results = context.run_checkpoint(
    checkpoint_name=my_checkpoint_name, batch_request=pg_runtime_batch_request
)

# Note to users: code below this line is only for integration testing -- ignore!

assert results["success"] is True
statistics = results["run_results"][list(results["run_results"].keys())[0]][
    "validation_result"
]["statistics"]
assert statistics["evaluated_expectations"] != 0
assert statistics["evaluated_expectations"] == statistics["successful_expectations"]
assert statistics["unsuccessful_expectations"] == 0
assert statistics["success_percent"] == 100.0
