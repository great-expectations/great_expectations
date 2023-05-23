import pathlib

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config.py setup">
from ruamel.yaml import YAML
import great_expectations as gx
from pprint import pprint

yaml = YAML()
context = gx.get_context()
# </snippet>

data_directory = pathlib.Path(
    gx.__file__,
    "..",
    "..",
    "tests",
    "test_sets",
    "taxi_yellow_tripdata_samples",
).resolve(strict=True)

context.add_expectation_suite(expectation_suite_name="taxi_data")

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config.py asset_names">
pprint(context.get_available_data_asset_names())
# </snippet>

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config.py suite_names">
context.list_expectation_suite_names()
# </snippet>

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config.py create_checkpoint">
my_checkpoint_name = "my_checkpoint"

yaml_config = f"""
name: {my_checkpoint_name}
config_version: 1.0
class_name: SimpleCheckpoint
run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
validations:
  - batch_request:
      datasource_name: taxi_source
      data_asset_name: yellow_tripdata
    expectation_suite_name: taxi_data
"""
# </snippet>

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config.py test_checkpoint">
my_checkpoint = context.test_yaml_config(yaml_config=yaml_config)
# </snippet>

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config.py save_checkpoint">
context.add_checkpoint(checkpoint=my_checkpoint)
# </snippet>

assert context.get_checkpoint("my_checkpoint")
