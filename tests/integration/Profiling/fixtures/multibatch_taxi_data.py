from great_expectations.core.batch import BatchRequest
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.data_context.data_context import DataContext

context = DataContext()
suite = context.get_expectation_suite("yellow_trip_data_validations")

### Workflow 1

batch_request_march = BatchRequest(
  datasource_name="taxi_pandas",
  data_connector_name="monthly",
  data_asset_name="my_reports",
  data_connector_query={
    "index": -1
  }
)

validator_march = context.get_validator(batch_request=batch_request_march, expectation_suite=suite)
march_table_row_count = validator_march.get_metric(MetricConfiguration("table.row_count", metric_domain_kwargs={}))

batch_request_february = BatchRequest(
  datasource_name="taxi_pandas",
  data_connector_name="monthly",
  data_asset_name="my_reports",
  data_connector_query={
    "index": -2
  }
)

validator_february = context.get_validator(batch_request=batch_request_february, expectation_suite=suite)

print(validator_february.expect_table_row_count_to_equal(value=march_table_row_count))


### Workflow 2

multi_batch_request = BatchRequest(
  datasource_name="taxi_pandas",
  data_connector_name="monthly",
  data_asset_name="my_reports",
  data_connector_query={
    "index": "[-2:]"
  }
)

validator = context.get_validator(batch_request=multi_batch_request, expectation_suite=suite)
validator.expect_table_row_count_to_equal(
  value=validator.get_metric(MetricConfiguration("table.row_count", metric_domain_kwargs={batch_id=...}))
)


### Workflow 3

validator = context.get_validator(batch_request=batch_request_february, expectation_suite=suite)
batch_march = context.get_batch(batch_request=batch_request_march)
validator.load_batch(batch_march)
validator.expect_table_row_count_to_equal(
  value=validator.get_metric(MetricConfiguration("table.row_count", metric_domain_kwargs={batch_id=...}))
)

### Workflow 4

batch_february = context.get_batch(batch_request=batch_request_february)
batch_march = context.get_batch(batch_request=batch_request_march)
validator = context.get_validator(batches=[batch_february, batch_march], expectation_suite=suite)

validator.expect_table_row_count_to_equal(
  value=validator.get_metric(MetricConfiguration("table.row_count", metric_domain_kwargs={batch_id=...}))
)
