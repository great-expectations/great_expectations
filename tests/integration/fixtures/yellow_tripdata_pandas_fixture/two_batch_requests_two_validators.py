from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.data_context import DataContext
from great_expectations.validator.metric_configuration import MetricConfiguration

context = DataContext()
suite = context.get_expectation_suite("yellow_tripdata_validations")

# Get February BatchRequest and Validator
batch_request_february = BatchRequest(
    datasource_name="taxi_pandas",
    data_connector_name="monthly",
    data_asset_name="my_reports",
    data_connector_query={"index": -2},
)
validator_february = context.get_validator(
    batch_request=batch_request_february, expectation_suite=suite
)

# Get the table row count for February
february_table_row_count = validator_february.get_metric(
    MetricConfiguration("table.row_count", metric_domain_kwargs={})
)

# Get March BatchRequest and Validator
batch_request_march = BatchRequest(
    datasource_name="taxi_pandas",
    data_connector_name="monthly",
    data_asset_name="my_reports",
    data_connector_query={"index": -1},
)
validator_march = context.get_validator(
    batch_request=batch_request_march, expectation_suite=suite
)

# Create a row count expectation based on the February row count, and validate it against the March row count
result = validator_march.expect_table_row_count_to_equal(value=february_table_row_count)
assert result["success"]
