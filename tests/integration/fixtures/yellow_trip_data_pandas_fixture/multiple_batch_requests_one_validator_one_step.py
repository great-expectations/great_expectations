from typing import List

import numpy as np

from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.data_context import DataContext
from great_expectations.datasource.data_connector.batch_filter import (
    BatchFilter,
    build_batch_filter,
)
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.validator.validator import Validator

context = DataContext()
suite = context.get_expectation_suite("yellow_trip_data_validations")

# Create three BatchRequests for Jan, Feb, and March 2019 data and instantiate a Validator with all three BatchRequests
jan_batch_request: BatchRequest = BatchRequest(
    datasource_name="taxi_pandas",
    data_connector_name="monthly",
    data_asset_name="my_reports",
    data_connector_query={"batch_filter_parameters": {"month": "01", "year": "2019"}},
)

feb_batch_request: BatchRequest = BatchRequest(
    datasource_name="taxi_pandas",
    data_connector_name="monthly",
    data_asset_name="my_reports",
    data_connector_query={"batch_filter_parameters": {"month": "02", "year": "2019"}},
)

march_batch_request: BatchRequest = BatchRequest(
    datasource_name="taxi_pandas",
    data_connector_name="monthly",
    data_asset_name="my_reports",
    data_connector_query={"batch_filter_parameters": {"month": "03", "year": "2019"}},
)

validator: Validator = context.get_validator(
    batch_request_list=[jan_batch_request, feb_batch_request, march_batch_request],
    expectation_suite=suite,
)
assert validator.active_batch_definition.batch_identifiers["month"] == "03"
assert validator.active_batch_definition.batch_identifiers["year"] == "2019"

# Get the list of all batches contained by the Validator for use in the BatchFileter
total_batch_definition_list: List = [
    v.batch_definition for k, v in validator.batches.items()
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
    current_max = validator.get_metric(
        MetricConfiguration(
            "column.max",
            metric_domain_kwargs={"column": "fare_amount", "batch_id": batch_id},
        )
    )
    cumulative_max = current_max if current_max > cumulative_max else cumulative_max

    current_min = validator.get_metric(
        MetricConfiguration(
            "column.min",
            metric_domain_kwargs={"column": "fare_amount", "batch_id": batch_id},
        )
    )
    cumulative_min = current_min if current_min < cumulative_min else cumulative_min

# Use the highest max and lowest min from Jan and Feb to create an expectation which we validate against March
result = validator.expect_column_values_to_be_between(
    "fare_amount", min_value=cumulative_min, max_value=cumulative_max
)
assert result["success"]
