import copy
from typing import Callable, Dict, Tuple, Type
from unittest import mock

import pytest

from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.data_context.base_data_context import (
    BaseDataContext,
)
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.data_context.data_context.data_context import DataContext
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    checkpointConfigSchema,
)
from tests.data_context.cloud_data_context.conftest import MockResponse


@pytest.fixture
def checkpoint_config() -> dict:
    checkpoint_config = {
        "name": "oss_test_checkpoint",
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "expectation_suite_name": "oss_test_expectation_suite",
        "validations": [
            {
                "expectation_suite_name": "taxi.demo_pass",
            },
            {
                "batch_request": {
                    "datasource_name": "oss_test_datasource",
                    "data_connector_name": "oss_test_data_connector",
                    "data_asset_name": "users",
                },
            },
        ],
    }
    return checkpoint_config


@pytest.fixture
def checkpoint_id() -> str:
    return "c83e4299-6188-48c6-83b7-f6dce8ad4ab5"


@pytest.fixture
def validation_ids() -> Tuple[str, str]:
    validation_id_1 = "v8764797-c486-4104-a764-1f2bf9630ee1"
    validation_id_2 = "vd0185a8-11c2-11ed-861d-0242ac120002"
    return validation_id_1, validation_id_2


@pytest.fixture
def checkpoint_config_with_ids(
    checkpoint_config: dict, checkpoint_id: str, validation_ids: Tuple[str, str]
) -> dict:
    validation_id_1, validation_id_2 = validation_ids

    updated_checkpoint_config = copy.deepcopy(checkpoint_config)
    updated_checkpoint_config["id"] = checkpoint_id
    updated_checkpoint_config["validations"][0]["id"] = validation_id_1
    updated_checkpoint_config["validations"][1]["id"] = validation_id_2

    return updated_checkpoint_config


@pytest.fixture
def mocked_post_response(
    mock_response_factory: Callable, checkpoint_id: str, validation_ids: Tuple[str, str]
) -> Callable[[], MockResponse]:
    validation_id_1, validation_id_2 = validation_ids

    def _mocked_post_response(*args, **kwargs):
        return mock_response_factory(
            {
                "data": {
                    "id": checkpoint_id,
                    "validations": [
                        {"id": validation_id_1},
                        {"id": validation_id_2},
                    ],
                }
            },
            201,
        )

    return _mocked_post_response


@pytest.fixture
def mocked_get_response(
    mock_response_factory: Callable,
    checkpoint_config_with_ids: dict,
    checkpoint_id: str,
) -> Callable[[], MockResponse]:
    def _mocked_get_response(*args, **kwargs):
        created_by_id = "c06ac6a2-52e0-431e-b878-9df624edc8b8"
        organization_id = "046fe9bc-c85b-4e95-b1af-e4ce36ba5384"

        return mock_response_factory(
            {
                "data": {
                    "attributes": {
                        "checkpoint_config": checkpoint_config_with_ids,
                        "created_at": "2022-08-02T17:55:45.107550",
                        "created_by_id": created_by_id,
                        "deleted": False,
                        "deleted_at": None,
                        "desc": None,
                        "name": "oss_test_checkpoint",
                        "organization_id": f"{organization_id}",
                        "updated_at": "2022-08-02T17:55:45.107550",
                    },
                    "id": checkpoint_id,
                    "links": {
                        "self": f"/organizations/{organization_id}/checkpoints/{checkpoint_id}"
                    },
                    "type": "checkpoint",
                },
            },
            200,
        )

    return _mocked_get_response


@pytest.mark.cloud
@pytest.mark.integration
@pytest.mark.parametrize(
    "data_context_fixture_name,data_context_type",
    [
        # In order to leverage existing fixtures in parametrization, we provide
        # their string names and dynamically retrieve them using pytest's built-in
        # `request` fixture.
        # Source: https://stackoverflow.com/a/64348247
        pytest.param(
            "empty_base_data_context_in_cloud_mode",
            BaseDataContext,
            id="BaseDataContext",
        ),
        pytest.param("empty_data_context_in_cloud_mode", DataContext, id="DataContext"),
        pytest.param(
            "empty_cloud_data_context", CloudDataContext, id="CloudDataContext"
        ),
    ],
)
def test_cloud_backed_data_context_add_checkpoint(
    data_context_fixture_name: str,
    data_context_type: Type[AbstractDataContext],
    checkpoint_id: str,
    validation_ids: Tuple[str, str],
    checkpoint_config: dict,
    mocked_post_response: Callable[[], MockResponse],
    mocked_get_response: Callable[[], MockResponse],
    shared_called_with_request_kwargs: dict,
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    request,
) -> None:
    """
    All Cloud-backed contexts (DataContext, BaseDataContext, and CloudDataContext) should save to a Cloud-backed CheckpointStore when calling `add_checkpoint`.
    When saving, it should use the id from the response to create the checkpoint.
    """
    context = request.getfixturevalue(data_context_fixture_name)

    # Make sure the fixture has the right configuration
    assert isinstance(context, data_context_type)
    assert context.ge_cloud_mode

    validation_id_1, validation_id_2 = validation_ids

    with mock.patch(
        "requests.post", autospec=True, side_effect=mocked_post_response
    ) as mock_post, mock.patch(
        "requests.get", autospec=True, side_effect=mocked_get_response
    ) as mock_get:
        checkpoint = context.add_checkpoint(**checkpoint_config)

        # Round trip through schema to mimic updates made during store serialization process
        expected_checkpoint_config = checkpointConfigSchema.dump(
            CheckpointConfig(**checkpoint_config)
        )

        mock_post.assert_called_with(
            f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/checkpoints",
            json={
                "data": {
                    "type": "checkpoint",
                    "attributes": {
                        "checkpoint_config": expected_checkpoint_config,
                        "organization_id": ge_cloud_organization_id,
                    },
                },
            },
            **shared_called_with_request_kwargs,
        )

        mock_get.assert_called_with(
            f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/checkpoints/{checkpoint_id}",
            params={"name": checkpoint_config["name"]},
            **shared_called_with_request_kwargs,
        )

    assert checkpoint.ge_cloud_id == checkpoint_id
    assert checkpoint.config.ge_cloud_id == checkpoint_id

    assert checkpoint.config.validations[0]["id_"] == validation_id_1
    assert checkpoint.validations[0]["id_"] == validation_id_1

    assert checkpoint.config.validations[1]["id_"] == validation_id_2
    assert checkpoint.validations[1]["id_"] == validation_id_2


@pytest.mark.xfail(
    reason="Environment variable issues with Azure; temporary xfail to unblock release 0.15.19"
)
@pytest.mark.e2e
@pytest.mark.cloud
@mock.patch("great_expectations.data_context.DataContext._save_project_config")
def test_cloud_backed_data_context_add_checkpoint_e2e(
    mock_save_project_config: mock.MagicMock,
    checkpoint_config: dict,
) -> None:
    context = DataContext(ge_cloud_mode=True)

    checkpoint = context.add_checkpoint(**checkpoint_config)

    ge_cloud_id = checkpoint.ge_cloud_id

    checkpoint_stored_in_cloud = context.get_checkpoint(ge_cloud_id=ge_cloud_id)

    assert checkpoint.ge_cloud_id == checkpoint_stored_in_cloud.ge_cloud_id
    assert (
        checkpoint.config.to_json_dict()
        == checkpoint_stored_in_cloud.config.to_json_dict()
    )
