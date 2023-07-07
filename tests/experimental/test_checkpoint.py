from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from great_expectations.data_context.types.base import CheckpointValidationConfig
from great_expectations.experimental.checkpoint import Checkpoint

if TYPE_CHECKING:
    from great_expectations.checkpoint.configurator import ActionDict
    from great_expectations.data_context.data_context.ephemeral_data_context import (
        EphemeralDataContext,
    )


@pytest.fixture
def action_list() -> list[ActionDict]:
    return [
        {
            "name": "store_validation_result",
            "action": {
                "class_name": "StoreValidationResultAction",
            },
        },
        {
            "name": "store_evaluation_params",
            "action": {
                "class_name": "StoreEvaluationParametersAction",
            },
        },
    ]


@pytest.fixture
def validations() -> list[CheckpointValidationConfig]:
    return [
        CheckpointValidationConfig(
            batch_request={
                "datasource_name": "my_pandas_filesystem_datasource",
                "data_asset_name": "users",
            },
        ),
    ]


@pytest.fixture
def experimental_checkpoint(
    in_memory_runtime_context: EphemeralDataContext,
    action_list: list[ActionDict],
    validations: list[CheckpointValidationConfig],
) -> Checkpoint:
    return Checkpoint(
        name="my_experimental_checkpoint",
        data_context=in_memory_runtime_context,
        action_list=action_list,
        validations=validations,
    )


def test_add_action(experimental_checkpoint: Checkpoint):
    before = len(experimental_checkpoint.action_list)
    action = {
        "name": "update_data_docs",
        "action": {
            "class_name": "UpdateDataDocsAction",
        },
    }
    experimental_checkpoint.add_action(action)
    after = len(experimental_checkpoint.action_list)

    assert after - before == 1


def test_add_action_collision_raises_error(
    experimental_checkpoint: Checkpoint, action_list: list[ActionDict]
):
    action = action_list[0]
    with pytest.raises(ValueError):
        experimental_checkpoint.add_action(action)


@pytest.mark.parametrize(
    "validation",
    [
        pytest.param(
            CheckpointValidationConfig(
                expectation_suite_name="my_suite",
                batch_request={
                    "datasource_name": "my_ds",
                    "data_asset_name": "my_asset",
                },
            ),
            id="CheckpointValidationConfig",
        ),
        pytest.param(
            {
                "expectation_suite_name": "my_suite",
                "batch_request": {
                    "datasource_name": "my_ds",
                    "data_asset_name": "my_asset",
                },
            },
            id="dict",
        ),
    ],
)
def test_add_validation(
    experimental_checkpoint: Checkpoint,
    validation: CheckpointValidationConfig | dict,
):
    before = len(experimental_checkpoint.validations)
    experimental_checkpoint.add_validation(validation)
    after = len(experimental_checkpoint.validations)

    assert after - before == 1
    assert all(
        isinstance(v, CheckpointValidationConfig)
        for v in experimental_checkpoint.validations
    )


@pytest.mark.parametrize(
    "validation",
    [
        # Same as what is defined in experimental_checkpoint
        pytest.param(
            CheckpointValidationConfig(
                batch_request={
                    "datasource_name": "my_pandas_filesystem_datasource",
                    "data_asset_name": "users",
                },
            ),
            id="CheckpointValidationConfig",
        ),
        # Same as what is defined in experimental_checkpoint (but as a dict)
        pytest.param(
            {
                "batch_request": {
                    "datasource_name": "my_pandas_filesystem_datasource",
                    "data_asset_name": "users",
                },
            },
            id="dict",
        ),
    ],
)
def test_add_validation_collision_raises_error(
    experimental_checkpoint: Checkpoint,
    validation: CheckpointValidationConfig | dict,
):
    with pytest.raises(ValueError):
        # TODO: currently doesn't work because CheckpointValidationConfig __eq__ isn't implemented
        experimental_checkpoint.add_validation(validation)


def test_run():
    pass  # TODO: ensure our limited constructor args result in a proper run
