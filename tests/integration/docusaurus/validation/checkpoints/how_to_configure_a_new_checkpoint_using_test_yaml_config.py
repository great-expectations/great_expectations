import pathlib
from pprint import pprint

from ruamel.yaml import YAML

import great_expectations as gx

yaml = YAML()
context = gx.get_context()

data_directory = pathlib.Path(
    gx.__file__,
    "..",
    "..",
    "tests",
    "test_sets",
    "taxi_yellow_tripdata_samples",
).resolve(strict=True)

context.add_expectation_suite(expectation_suite_name="taxi_data")

pprint(context.get_available_data_asset_names())


context.list_expectation_suite_names()

my_checkpoint_name = "my_checkpoint"

yaml_config = f"""
name: {my_checkpoint_name}
config_version: 1.0
class_name: Checkpoint
run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
validations:
  - batch_request:
      datasource_name: taxi_source
      data_asset_name: yellow_tripdata
    expectation_suite_name: taxi_data
"""

my_checkpoint = context.test_yaml_config(yaml_config=yaml_config)

context.add_checkpoint(checkpoint=my_checkpoint)

assert context.get_checkpoint("my_checkpoint")
