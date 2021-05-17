import numpy as np

from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.data_context import DataContext
from great_expectations.datasource.data_connector.batch_filter import (
    BatchFilter,
    build_batch_filter,
)
from great_expectations.validator.validation_graph import MetricConfiguration

context = DataContext()
suite = context.get_expectation_suite("yellow_trip_data_validations")

# This BatchRequest will retrieve all three batches from 2019 ("01", "02", "03")
multi_batch_request = BatchRequest(
    datasource_name="taxi_pandas",
    data_connector_name="monthly",
    data_asset_name="my_reports",
    data_connector_query={"batch_filter_parameters": {"year": "2019"}},
)

# Instantiate the Validator
validator_multi_batch = context.get_validator(
    batch_request=multi_batch_request, expectation_suite=suite
)

# The active batch should be March, as this should be the last one loaded. Confirming here.
assert validator_multi_batch.active_batch_definition.batch_identifiers["month"] == "03"

# Get the list of all batches contained by the Validator for use in the BatchFileter
total_batch_definition_list: list = [
    v.batch_definition for k, v in validator_multi_batch.batches.items()
]

# Filter to all batch_definitions prior to March
jan_feb_batch_filter: BatchFilter = build_batch_filter(
    data_connector_query_dict={
        "custom_filter_function": lambda batch_identifiers: int(
            batch_identifiers["month"]
        )
        < 3
    }
)
jan_feb_batch_definition_list: list = (
    jan_feb_batch_filter.select_from_data_connector_query(
        batch_definition_list=total_batch_definition_list
    )
)

# Get the highest max and lowest min between January and February
cumulative_max = 0
cumulative_min = np.Inf
for batch_definition in jan_feb_batch_definition_list:
    batch_id: str = batch_definition.id
    current_max = validator_multi_batch.get_metric(
        MetricConfiguration(
            "column.max",
            metric_domain_kwargs={"column": "fare_amount", "batch_id": batch_id},
        )
    )
    cumulative_max = current_max if current_max > cumulative_max else cumulative_max

    current_min = validator_multi_batch.get_metric(
        MetricConfiguration(
            "column.min",
            metric_domain_kwargs={"column": "fare_amount", "batch_id": batch_id},
        )
    )
    cumulative_min = current_min if current_min < cumulative_min else cumulative_min

# Use the highest max and lowest min from Jan and Feb to create an expectation which we validate against March
result = validator_multi_batch.expect_column_values_to_be_between(
    "fare_amount", min_value=cumulative_min, max_value=cumulative_max
)
assert result["success"]
