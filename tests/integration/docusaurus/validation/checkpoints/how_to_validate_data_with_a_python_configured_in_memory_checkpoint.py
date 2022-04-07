# Required imports for this script's purpose:
# <snippet>
import great_expectations as ge
from great_expectations.checkpoint import Checkpoint

# </snippet>

# Imports used for testing purposes (and can be left out of typical scripts):
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_context.types.base import CheckpointConfig
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)

# Import and setup for working with YAML strings:
# <snippet>
from ruamel import yaml

yaml = yaml.YAML(typ="safe")
# </snippet>

# Initialize your data context.
# <snippet>
context = ge.get_context()
# </snippet>

# Add datasource for all tests
datasource_yaml = """
name: taxi_datasource
class_name: Datasource
module_name: great_expectations.datasource
execution_engine:
  module_name: great_expectations.execution_engine
  class_name: PandasExecutionEngine
data_connectors:
  default_inferred_data_connector_name:
    class_name: InferredAssetFilesystemDataConnector
    base_directory: ../data/
    default_regex:
      group_names:
        - data_asset_name
      pattern: (.*)\\.csv
  default_runtime_data_connector_name:
    class_name: RuntimeDataConnector
    batch_identifiers:
      - default_identifier_name
"""
context.test_yaml_config(datasource_yaml)
context.add_datasource(**yaml.load(datasource_yaml))
assert [ds["name"] for ds in context.list_datasources()] == ["taxi_datasource"]
# Add Expectation Suite for use in Checkpoint config
context.create_expectation_suite("my_expectation_suite")

# Define your checkpoint's configuration.
# NOTE: Because we are directly using the Checkpoint class, we do not need to
# specify the parameters `module_name` and `class_name`.
# <snippet>
my_checkpoint_name = "in_memory_checkpoint"
python_config = {
    "name": my_checkpoint_name,
    "config_version": 1,
    "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
    "action_list": [
        {
            "name": "store_validation_result",
            "action": {"class_name": "StoreValidationResultAction"},
        },
        {
            "name": "store_evaluation_params",
            "action": {"class_name": "StoreEvaluationParametersAction"},
        },
        {
            "name": "update_data_docs",
            "action": {"class_name": "UpdateDataDocsAction", "site_names": []},
        },
    ],
    "validations": [
        {
            "batch_request": {
                "datasource_name": "taxi_datasource",
                "data_connector_name": "default_inferred_data_connector_name",
                "data_asset_name": "yellow_tripdata_sample_2019-01",
                "data_connector_query": {"index": -1},
            },
            "expectation_suite_name": "my_expectation_suite",
        }
    ],
}
# </snippet>

# Initialize your checkpoint with the Data Context and configuration
# from before.
# <snippet>
my_checkpoint = Checkpoint(data_context=context, **python_config)
# </snippet>

# Run your Checkpoint.
# <snippet>
results = my_checkpoint.run()
# </snippet>

# The following asserts are for testing purposes and do not need to be included in typical scripts.
assert results.success is True
run_id_type = type(results.run_id)
assert run_id_type == RunIdentifier
validation_result_id_type_set = {type(k) for k in results.run_results.keys()}
assert len(validation_result_id_type_set) == 1
validation_result_id_type = next(iter(validation_result_id_type_set))
assert validation_result_id_type == ValidationResultIdentifier
validation_result_id = results.run_results[[k for k in results.run_results.keys()][0]]
assert (
    type(validation_result_id["validation_result"]) == ExpectationSuiteValidationResult
)
assert isinstance(results.checkpoint_config, CheckpointConfig)

# <snippet>
# context.open_data_docs()
# </snippet>
