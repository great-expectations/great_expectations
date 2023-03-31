"""Example Script: Using an auto-initializing Expectation

This example script is intended for use in documentation on how to use a auto-initializing Expectation.

Assert statements are included to ensure that if the behaviour shown in this script breaks it will not pass
tests and will be updated.  These statements can be ignored by users.

Comments with the tags `<snippet>` and `</snippet>` are used to ensure that if this script is updated
the snippets that are specified for use in documentation are maintained.  These comments can be ignored by users.

--documentation--
    https://docs.greatexpectations.io/docs/
"""

import great_expectations as gx
from great_expectations.core.batch import BatchRequest
from great_expectations.core.yaml_handler import YAMLHandler

yaml = YAMLHandler()
data_context: gx.DataContext = gx.get_context()

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
            "base_directory": "<PATH_TO_YOUR_DATA_HERE>",
            "default_regex": {
                "group_names": ["data_asset_name", "month"],
                "pattern": "(yellow_tripdata_sample_2018)-(\\d.*)\\.csv",
            },
        },
    },
}

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_config["data_connectors"]["2018_data"]["base_directory"] = "../data/"


data_context.test_yaml_config(yaml.dump(datasource_config))

multi_batch_request = BatchRequest(
    datasource_name="taxi_pandas",
    data_connector_name="monthly",
    data_asset_name="my_reports",
    data_connector_query={"batch_filter_parameters": {"year": "2019"}},
)

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

suite = data_context.add_or_update_expectation_suite(
    expectation_suite_name="new_expectation_suite"
)
validator = data_context.get_validator(
    expectation_suite=suite, batch_request=batch_request_2018_data
)

# NOTE: The following assertion is only for testing and can be ignored by users.
assert len(validator.batches) == 12

# <snippet name="tests/integration/docusaurus/expectations/auto_initializing_expectations/auto_initializing_expect_column_mean_to_be_between.py run expectation">
expectation_result = validator.expect_column_mean_to_be_between(
    column="trip_distance", auto=True
)
# </snippet>

# <snippet name="tests/integration/docusaurus/expectations/auto_initializing_expectations/auto_initializing_expect_column_mean_to_be_between.py save suite">
validator.save_expectation_suite(discard_failed_expectations=False)
# </snippet>

# NOTE: The following assertions are only for testing and can be ignored by users.
assert (
    expectation_result["expectation_config"]["expectation_type"]
    == "expect_column_mean_to_be_between"
)
assert expectation_result["expectation_config"]["kwargs"]["column"] == "trip_distance"
assert expectation_result["expectation_config"]["kwargs"]["auto"] is True
assert (
    expectation_result["expectation_config"]["kwargs"]["min_value"]
    == 2.8342089999999995
)
assert expectation_result["expectation_config"]["kwargs"]["max_value"] == 3.075627
assert expectation_result["expectation_config"]["kwargs"]["strict_min"] is False
assert expectation_result["expectation_config"]["kwargs"]["strict_max"] is False
