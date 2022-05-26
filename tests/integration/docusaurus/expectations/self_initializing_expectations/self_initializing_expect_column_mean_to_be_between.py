"""

"""
from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import BatchRequest
from great_expectations.expectations.expectation import Expectation
from great_expectations.rule_based_profiler.config import RuleBasedProfilerConfig

data_context: ge.DataContext = ge.get_context()

data_path: str = "../../../../test_sets/taxi_yellow_tripdata_samples"

datasource_config = {
    "name": "taxi_multi_batch_datasource",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "module_name": "great_expectations.execution_engine",
        "class_name": "PandasExecutionEngine",
    },
    "data_connectors": {
        "2018_data": {
            "class_name": "InferredAssetFilesystemDataConnector",
            "base_directory": data_path,
            "default_regex": {
                "group_names": ["data_asset_name", "month"],
                "pattern": "(yellow_tripdata_sample_2018)-(\\d.*)\\.csv",
            },
        },
    },
}

data_context.test_yaml_config(yaml.dump(datasource_config))

# add_datasource only if it doesn't already exist in our configuration
try:
    data_context.get_datasource(datasource_config["name"])
except ValueError:
    data_context.add_datasource(**datasource_config)

batch_request_2018_data: BatchRequest = BatchRequest(
    datasource_name="taxi_multi_batch_datasource",
    data_connector_name="2018_data",
    data_asset_name="yellow_tripdata_sample_2018",
)

suite = data_context.create_expectation_suite(
    expectation_suite_name="new_expectation_suite", overwrite_existing=True
)
validator = data_context.get_validator(
    expectation_suite=suite, batch_request=batch_request_2018_data
)

# NOTE: The following assertion is only for testing and can be ignored by users.
assert len(validator.batches) == 12

expectation_result = validator.expect_column_mean_to_be_between(
    column="trip_distance", auto=True
)

# NOTE: The following assertion is only for testing and can be ignored by users.
assert expectation_result["expectation_config"] == {
    "expectation_type": "expect_column_mean_to_be_between",
    "kwargs": {
        "column": "trip_distance",
        "min_value": 2.83,
        "max_value": 3.06,
        "strict_min": False,
        "strict_max": False,
    },
}
