# <snippet>
import os

import great_expectations as gx
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.core.yaml_handler import YAMLHandler

# </snippet>

yaml = YAMLHandler()
# NOTE: The following code is only for testing and depends on an environment
# variable to set the gcp_project. You can replace the value with your own
# GCP project information
gcp_project = os.environ.get("GE_TEST_GCP_PROJECT")
if not gcp_project:
    raise ValueError(
        "Environment Variable GE_TEST_GCP_PROJECT is required to run BigQuery integration tests"
    )
bigquery_dataset = "demo"

CONNECTION_STRING = f"bigquery://{gcp_project}/{bigquery_dataset}"

# <snippet>
context = gx.get_context()
# </snippet>

# <snippet>
datasource_yaml = f"""
name: my_bigquery_datasource
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  connection_string: bigquery://<GCP_PROJECT_NAME>/<BIGQUERY_DATASET>
data_connectors:
   default_runtime_data_connector_name:
       class_name: RuntimeDataConnector
       batch_identifiers:
           - default_identifier_name
   default_inferred_data_connector_name:
       class_name: InferredAssetSqlDataConnector
       include_schema_name: true
"""
# </snippet>

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_yaml = datasource_yaml.replace(
    "bigquery://<GCP_PROJECT_NAME>/<BIGQUERY_DATASET>",
    CONNECTION_STRING,
)

# <snippet>
context.test_yaml_config(datasource_yaml)
# </snippet>

# <snippet>
context.add_datasource(**yaml.load(datasource_yaml))
# </snippet>

# Test for RuntimeBatchRequest using a query.
# <snippet>
batch_request = RuntimeBatchRequest(
    datasource_name="my_bigquery_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="default_name",  # this can be anything that identifies this data
    runtime_parameters={"query": "SELECT * from demo.taxi_data LIMIT 10"},
    batch_identifiers={"default_identifier_name": "default_identifier"},
)

context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head())
# </snippet>

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, gx.validator.validator.Validator)

# Test for BatchRequest naming a table.
batch_request = BatchRequest(
    datasource_name="my_bigquery_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="demo.taxi_data",  # this is the name of the table you want to retrieve
)
context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head())

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, gx.validator.validator.Validator)
assert [ds["name"] for ds in context.list_datasources()] == ["my_bigquery_datasource"]
assert "demo.taxi_data" in set(
    context.get_available_data_asset_names()["my_bigquery_datasource"][
        "default_inferred_data_connector_name"
    ]
)
