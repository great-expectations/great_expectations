import json

import pandas as pd
import pytest

from great_expectations import DataContext
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.usage_statistics.anonymizers.checkpoint_run_anonymizer import (
    CheckpointRunAnonymizer,
)
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.util import deep_filter_properties_iterable

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
            "ge_cloud_id": None,
            "expectation_suite_ge_cloud_id": None,
        }
    )


def test_checkpoint_config_repr(checkpoint):
    checkpoint_config_repr: str = str(checkpoint)

    assert (
        checkpoint_config_repr
        == """{
  "name": "my_checkpoint",
  "config_version": 1.0,
  "batch_request": {},
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
  "profilers": [],
  "validations": [
    {
      "batch_request": {
        "datasource_name": "example_datasource",
        "data_connector_name": "default_runtime_data_connector_name",
        "data_asset_name": "my_data_asset"
      },
      "expectation_suite_name": "test_suite"
    }
  ]
}"""
    )


def test_checkpoint_config_repr_after_substitution(checkpoint):

    checkpoint_run_anonymizer: CheckpointRunAnonymizer = CheckpointRunAnonymizer(
        salt=DATA_CONTEXT_ID
    )

    df: pd.DataFrame = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    batch_request_param: dict = {
        "runtime_parameters": {"batch_data": df},
        "batch_identifiers": {"default_identifier_name": "my_simple_df"},
    }

    result_format_param: dict = {"result_format": "SUMMARY"}

    kwargs: dict = {
        "batch_request": batch_request_param,
        "result_format": result_format_param,
    }

    # Matching how this is called in usage_statistics.py (parameter style)
    resolved_runtime_kwargs: dict = (
        checkpoint_run_anonymizer.resolve_config_using_acceptable_arguments(
            *(checkpoint,), **kwargs
        )
    )

    json_dict: dict = convert_to_json_serializable(data=resolved_runtime_kwargs)
    deep_filter_properties_iterable(
        properties=json_dict,
        inplace=True,
    )
    checkpoint_config_repr: str = json.dumps(json_dict, indent=2)

    assert (
        checkpoint_config_repr
        == """{
  "name": "my_checkpoint",
  "config_version": 1.0,
  "batch_request": {
    "runtime_parameters": {
      "batch_data": [
        {
          "a": 1,
          "b": 3
        },
        {
          "a": 2,
          "b": 4
        }
      ]
    },
    "batch_identifiers": {
      "default_identifier_name": "my_simple_df"
    }
  },
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
  "profilers": [],
  "validations": [
    {
      "batch_request": {
        "datasource_name": "example_datasource",
        "data_connector_name": "default_runtime_data_connector_name",
        "data_asset_name": "my_data_asset",
        "runtime_parameters": {
          "batch_data": [
            {
              "a": 1,
              "b": 3
            },
            {
              "a": 2,
              "b": 4
            }
          ]
        },
        "batch_identifiers": {
          "default_identifier_name": "my_simple_df"
        }
      },
      "expectation_suite_name": "test_suite",
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
      ]
    }
  ]
}"""
    )
