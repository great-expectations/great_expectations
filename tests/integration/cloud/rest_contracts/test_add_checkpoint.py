from __future__ import annotations

import pathlib
from collections import OrderedDict
from typing import TYPE_CHECKING, Callable, Final

import pact
import pytest

from tests.integration.cloud.rest_contracts.conftest import (
    EXISTING_ORGANIZATION_ID,
    ContractInteraction,
)

if TYPE_CHECKING:
    from tests.integration.cloud.rest_contracts.conftest import PactBody

NON_EXISTING_CHECKPOINT_ID: Final[str] = "68058949-f9c2-47b1-922a-a89c23ffad99"
EXISTING_CHECKPOINT_ID: Final[str] = "68058949-f9c2-47b1-922a-a89c23ffad99"
PUT_CHECKPOINT_MIN_RESPONSE_BODY: Final[PactBody] = pact.Like("204 string")
PUT_CHECKPOINTS_MIN_REQUEST_BODY = {
    "data": {
        "attributes": {
            "checkpoint_config": OrderedDict(
                [
                    ("name", "my_checkpoint"),
                    ("config_version", 1.0),
                    ("template_name", None),
                    ("module_name", "great_expectations.checkpoint"),
                    ("class_name", "Checkpoint"),
                    ("run_name_template", None),
                    ("expectation_suite_name", None),
                    (
                        "action_list",
                        [
                            {
                                "name": "store_validation_result",
                                "action": {"class_name": "StoreValidationResultAction"},
                            },
                            {
                                "name": "store_evaluation_params",
                                "action": {
                                    "class_name": "StoreEvaluationParametersAction"
                                },
                            },
                        ],
                    ),
                    ("evaluation_parameters", {}),
                    ("runtime_configuration", {}),
                    (
                        "validations",
                        [
                            {
                                "batch_request": {
                                    "datasource_name": "My_Datasource",
                                    "data_connector_name": "default_runtime_data_connector",
                                    "data_asset_name": "My_data_asset",
                                    "data_connector_query": {"index": 0},
                                },
                                "expectation_suite_name": "My_Expectation_Suite",
                                "id": "1d8bc783-6444-437c-b008-ccfb7c8ce964",
                            },
                        ],
                    ),
                    ("profilers", []),
                    ("ge_cloud_id", "68058949-f9c2-47b1-922a-a89c23ffad99"),
                    (
                        "expectation_suite_ge_cloud_id",
                        "3705d38a-0eec-4bd8-9956-fdb34df924b6",
                    ),
                ]
            )
        },
        "type": "checkpoint",
    }
}

GET_CHECKPOINTS_MIN_RESPONSE_BODY: Final[dict] = {
    "data": {
        "id": pact.Format().uuid,
        "type": "checkpoint",
        "attributes": {
            "checkpoint_config": {
                "validations": [
                    {
                        "id": pact.Format().uuid,
                    }
                ]
            },
        },
    },
}


@pytest.mark.cloud
@pytest.mark.parametrize(
    "contract_interaction",
    [
        ContractInteraction(
            method="POST",
            request_path=pathlib.Path(
                "/",
                "organizations",
                EXISTING_ORGANIZATION_ID,
                "checkpoints",
            ),
            request_body=PUT_CHECKPOINTS_MIN_REQUEST_BODY,
            upon_receiving="a request to create a Checkpoint",
            given="the Checkpoint is created",
            response_status=201,
            response_body="",
        ),
        ContractInteraction(
            method="GET",
            request_path=pathlib.Path(
                "/",
                "organizations",
                EXISTING_ORGANIZATION_ID,
                "checkpoints",
                EXISTING_CHECKPOINT_ID,
            ),
            upon_receiving="a request to get a Checkpoint",
            given="the Checkpoint exists",
            response_status=200,
            response_body=GET_CHECKPOINTS_MIN_RESPONSE_BODY,
        ),
    ],
)
def test_put_checkpoint(
    contract_interaction: ContractInteraction,
    run_pact_test: Callable[[ContractInteraction], None],
) -> None:
    run_pact_test(contract_interaction)
