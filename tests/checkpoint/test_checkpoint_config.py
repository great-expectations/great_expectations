import pytest

from great_expectations.checkpoint import Checkpoint

DATA_CONTEXT_ID = "00000000-0000-0000-0000-000000000001"


@pytest.fixture
def checkpoint(
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    return Checkpoint(
        data_context=context,
        **{
            "name": "my_checkpoint",
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
        "evaluation_parameters",
        "name",
        "runtime_configuration",
        "validations",
    ):
        assert key in checkpoint_config_repr
