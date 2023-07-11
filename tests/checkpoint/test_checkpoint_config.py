import json
from typing import List

import pandas as pd
import pytest

from great_expectations import DataContext
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context.types.base import CheckpointConfig
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
        },
    )


@pytest.mark.filesystem
def test_checkpoint_config_repr(checkpoint):
    checkpoint_config_repr: str = str(checkpoint)

    for key in (
        "action_list",
        "batch_request",
        "class_name",
        "config_version",
        "evaluation_parameters",
        "module_name",
        "name",
        "profilers",
        "runtime_configuration",
        "validations",
    ):
        assert key in checkpoint_config_repr


@pytest.mark.filesystem
def test_checkpoint_config_repr_after_substitution(checkpoint):
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
        CheckpointConfig.resolve_config_using_acceptable_arguments(
            *(checkpoint,), **kwargs
        )
    )

    json_dict: dict = convert_to_json_serializable(data=resolved_runtime_kwargs)
    deep_filter_properties_iterable(
        properties=json_dict,
        inplace=True,
    )

    keys: List[str] = sorted(list(json_dict.keys()))

    key: str
    sorted_json_dict: dict = {key: json_dict[key] for key in keys}

    checkpoint_config_repr: str = json.dumps(sorted_json_dict, indent=2)

    for key in (
        "action_list",
        "batch_request",
        "class_name",
        "config_version",
        "evaluation_parameters",
        "module_name",
        "name",
        "profilers",
        "runtime_configuration",
        "validations",
    ):
        assert key in checkpoint_config_repr
