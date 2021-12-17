import pandas as pd
import pytest

from great_expectations import DataContext
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.usage_statistics.anonymizers.checkpoint_run_anonymizer import (
    CheckpointRunAnonymizer,
)
from great_expectations.data_context.types.base import CheckpointConfig

DATA_CONTEXT_ID = "00000000-0000-0000-0000-000000000001"


@pytest.fixture
def checkpoint(
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    return Checkpoint(
        data_context=context,
        **{
            "name": "my_checkpoint",
            "config_version": 1.0,
            "template_name": None,
            "module_name": "great_expectations.checkpoint",
            "class_name": "Checkpoint",
            "run_name_template": None,
            "expectation_suite_name": None,
            "batch_request": None,
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
            "evaluation_parameters": {},
            "runtime_configuration": {},
            "validations": [
                {
                    "batch_request": {
                        "datasource_name": "example_datasource",
                        "data_connector_name": "default_runtime_data_connector_name",
                        "data_asset_name": "my_data_asset",
                    },
                    "expectation_suite_name": "test_suite",
                }
            ],
            "profilers": [],
            "ge_cloud_id": None,
            "expectation_suite_ge_cloud_id": None,
        }
    )


def test_checkpoint_config_repr(checkpoint):

    checkpoint_config_repr = checkpoint.config.__repr__()

    print(checkpoint.config)

    assert (
        checkpoint_config_repr
        == """{
  "name": "my_checkpoint",
  "config_version": 1.0,
  "template_name": null,
  "module_name": "great_expectations.checkpoint",
  "class_name": "Checkpoint",
  "run_name_template": null,
  "expectation_suite_name": null,
  "batch_request": null,
  "action_list": [
    {
      "name": "store_validation_result",
      "action": {
        "class_name": "StoreValidationResultAction"
      }
    },
    {
      "name": "store_evaluation_params",
      "action": {
        "class_name": "StoreEvaluationParametersAction"
      }
    },
    {
      "name": "update_data_docs",
      "action": {
        "class_name": "UpdateDataDocsAction",
        "site_names": []
      }
    }
  ],
  "evaluation_parameters": {},
  "runtime_configuration": {},
  "validations": [
    {
      "batch_request": {
        "datasource_name": "example_datasource",
        "data_connector_name": "default_runtime_data_connector_name",
        "data_asset_name": "my_data_asset"
      },
      "expectation_suite_name": "test_suite"
    }
  ],
  "profilers": [],
  "ge_cloud_id": null,
  "expectation_suite_ge_cloud_id": null
}"""
    )
