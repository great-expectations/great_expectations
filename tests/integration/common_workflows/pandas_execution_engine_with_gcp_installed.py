import os

import great_expectations as ge

# What does this test and why?
# A common initial use of GE is locally, with the PandasExecutionEngine
# A user of GCP could also have the GOOGLE_APPLICATION_CREDENTIALS set
# This workflow was broken for a short time by PR # 3679 and then reverted with PR # 3689 and fixed with PR #3694
# The following test ensures that this simple workflow still works

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ".gcs/my_example_creds.json"
os.environ["GCLOUD_PROJECT"] = "some_project_id"

context = ge.get_context()

example_yaml = """
name: my_datasource
class_name: Datasource
execution_engine:
  class_name: PandasExecutionEngine

data_connectors:
  default_inferred_data_connector_name:
    class_name: InferredAssetFilesystemDataConnector
    base_directory: ../data/
    default_regex:
      group_names:
        - data_asset_name
      pattern: (.*)
"""

context.test_yaml_config(yaml_config=example_yaml)
