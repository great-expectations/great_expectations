import json

import pandas as pd

# This utility is not for general use. It is only to support testing.
from tests.test_utils import load_data_into_test_database

# The following load & config blocks up until the batch requests are only to support testing.
PG_CONNECTION_STRING = "postgresql+psycopg2://postgres:@localhost/test_ci"

csv_path = "./data/yellow_tripdata_sample_2019-01.csv"

load_data_into_test_database(
    table_name="postgres_taxi_data",
    csv_path=csv_path,
    connection_string=PG_CONNECTION_STRING,
    load_full_dataset=True,
)

# Make sure the test data is as expected
df = pd.read_csv(csv_path)
assert df.passenger_count.unique().tolist() == [1, 2, 3, 4, 5, 6]
assert len(df) == 10000

# Tutorial content resumes here.

# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_dynamically_load_evaluation_parameters_from_a_database.py get_validator">
import great_expectations as gx

context = gx.get_context()

pg_datasource = context.sources.add_sql(
    name="pg_datasource", connection_string=PG_CONNECTION_STRING
)
table_asset = pg_datasource.add_table_asset(
    name="postgres_taxi_data", table_name="postgres_taxi_data"
)
batch_request = table_asset.build_batch_request()

validator = context.get_validator(
    batch_request=batch_request, create_expectation_suite_with_name="my_suite_name"
)
# </snippet>


# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_dynamically_load_evaluation_parameters_from_a_database.py define expectation">
validator_results = validator.expect_column_values_to_be_in_set(
    column="passenger_count",
    value_set={
        "$PARAMETER": "urn:great_expectations:stores:my_query_store:unique_passenger_counts"
    },
)
# </snippet>


# <snippet name="tests/integration/docusaurus/expectations/advanced/how_to_dynamically_load_evaluation_parameters_from_a_database.py expected_validator_results">
expected_validator_results = """
{
  "expectation_config": {
    "expectation_type": "expect_column_values_to_be_in_set",
    "meta": {},
    "kwargs": {
      "column": "passenger_count",
      "value_set": [
        1,
        2,
        3,
        4,
        5,
        6
      ],
      "batch_id": "pg_datasource-postgres_taxi_data"
    }
  },
  "meta": {},
  "result": {
    "element_count": 10000,
    "unexpected_count": 0,
    "unexpected_percent": 0.0,
    "partial_unexpected_list": [],
    "missing_count": 0,
    "missing_percent": 0.0,
    "unexpected_percent_total": 0.0,
    "unexpected_percent_nonmissing": 0.0
  },
  "success": true,
  "exception_info": {
    "raised_exception": false,
    "exception_traceback": null,
    "exception_message": null
  }
}
"""
# </snippet>


# Note to users: code below this line is only for integration testing -- ignore!
assert validator_results.to_json_dict() == json.loads(expected_validator_results)
