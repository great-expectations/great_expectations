from ruamel import yaml

import great_expectations as ge
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.batch import BatchRequest
from great_expectations.profile.user_configurable_profiler import (
    UserConfigurableProfiler,
)
from great_expectations.validator.validator import Validator

context = ge.get_context()
# NOTE: The following assertion is only for testing and can be ignored by users.
assert context

# First configure a new Datasource and add to DataContext
datasource_yaml = f"""
name: data__dir
class_name: Datasource
module_name: great_expectations.datasource
execution_engine:
  module_name: great_expectations.execution_engine
  class_name: PandasExecutionEngine
data_connectors:
    default_runtime_data_connector_name:
        class_name: RuntimeDataConnector
        batch_identifiers:
            - default_identifier_name
    default_inferred_data_connector_name:
        class_name: InferredAssetFilesystemDataConnector
        base_directory: ../data/
        default_regex:
          group_names:
            - data_asset_name
          pattern: (.*)
"""
context.test_yaml_config(datasource_yaml)
context.add_datasource(**yaml.load(datasource_yaml))

# Get Validator by creating ExpectationSuite and passing in BatchRequest
batch_request = BatchRequest(
    datasource_name="data__dir",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="yellow_trip_data_sample_2019-01.csv",
    limit=1000,
)
context.create_expectation_suite(expectation_suite_name="taxi.demo")
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="taxi.demo",
)
# NOTE: The following assertion is only for testing and can be ignored by users.
assert isinstance(validator, Validator)

# Profile the data with the UserConfigurableProfiler and save resulting ExpectationSuite
ignored_columns = [
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    # "passenger_count",
    "trip_distance",
    "rate_code_id",
    "store_and_fwd_flag",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
]

profiler = UserConfigurableProfiler(
    profile_dataset=validator,
    excluded_expectations=None,
    ignored_columns=ignored_columns,
    not_null_only=False,
    primary_or_compound_key=False,
    semantic_types_dict=None,
    table_expectations_only=False,
    value_set_threshold="MANY",
)
suite = profiler.build_suite()
validator.save_expectation_suite(discard_failed_expectations=False)

# Create first checkpoint on yellow_trip_data_sample_2019-01.csv
my_checkpoint_config = f"""
name: my_checkpoint
config_version: 1.0
class_name: SimpleCheckpoint
run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
validations:
  - batch_request:
      datasource_name: data__dir
      data_connector_name: default_inferred_data_connector_name
      data_asset_name: yellow_trip_data_sample_2019-01.csv
      data_connector_query:
        index: -1
    expectation_suite_name: taxi.demo
"""
my_checkpoint_config = yaml.load(my_checkpoint_config)

# NOTE: The following code (up to and including the assert) is only for testing and can be ignored by users.
# In the current test, site_names are set to None because we do not want to update and build data_docs
# If you would like to build data_docs then either remove `site_names=None` or pass in a list of site_names you would like to build the docs on.
checkpoint = SimpleCheckpoint(
    **my_checkpoint_config, data_context=context, site_names=None
)
checkpoint_result = checkpoint.run(site_names=None)
assert checkpoint_result.run_results


# Create second checkpoint on yellow_trip_data_sample_2019-02.csv
my_new_checkpoint_config = f"""
name: my_new_checkpoint
config_version: 1.0
class_name: SimpleCheckpoint
run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
validations:
  - batch_request:
      datasource_name: data__dir
      data_connector_name: default_inferred_data_connector_name
      data_asset_name: yellow_trip_data_sample_2019-02.csv
      data_connector_query:
        index: -1
    expectation_suite_name: taxi.demo
"""


my_new_checkpoint_config = yaml.load(my_new_checkpoint_config)

# NOTE: The following code (up to and including the assert) is only for testing and can be ignored by users.
# In the current test, site_names are set to None because we do not want to update and build data_docs
# If you would like to build data_docs then either remove `site_names=None` or pass in a list of site_names you would like to build the docs on.
new_checkpoint = SimpleCheckpoint(
    **my_new_checkpoint_config, data_context=context, site_names=None
)
new_checkpoint_result = new_checkpoint.run(site_names=None)
assert new_checkpoint_result.run_results
