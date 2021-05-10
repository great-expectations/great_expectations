from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.data_context import DataContext
from great_expectations.validator.validation_graph import MetricConfiguration

context = DataContext()
suite = context.get_expectation_suite("yellow_trip_data_validations")

# February BatchRequest and Validator
batch_request_february = BatchRequest(
    datasource_name="taxi_pandas",
    data_connector_name="monthly",
    data_asset_name="my_reports",
    data_connector_query={"index": -2},
)
validator_february = context.get_validator(
    batch_request=batch_request_february, expectation_suite=suite
)
february_table_row_count = validator_february.get_metric(
    MetricConfiguration("table.row_count", metric_domain_kwargs={})
)

# March BatchRequest and Validator
batch_request_march = BatchRequest(
    datasource_name="taxi_pandas",
    data_connector_name="monthly",
    data_asset_name="my_reports",
    data_connector_query={"index": -1},
)
validator_march = context.get_validator(
    batch_request=batch_request_march, expectation_suite=suite
)
result = validator_march.expect_table_row_count_to_equal(value=february_table_row_count)
assert result["success"]
