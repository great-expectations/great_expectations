import copy
from typing import Callable, Optional, Tuple
from unittest import mock

import pandas as pd
import pytest

from great_expectations.checkpoint import Checkpoint
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.data_context.data_context.data_context import DataContext
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    DataContextConfig,
    GXCloudConfig,
    checkpointConfigSchema,
)
from great_expectations.data_context.types.resource_identifiers import GXCloudIdentifier
from great_expectations.exceptions import CheckpointNotFoundError, StoreBackendError
from great_expectations.util import get_context
from tests.data_context.conftest import MockResponse


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
    mock_response_factory: Callable,
    checkpoint_id: str,
    validation_ids: Tuple[str, str],
    checkpoint_config_with_ids: dict,
) -> Callable[[], MockResponse]:
    validation_id_1, validation_id_2 = validation_ids

    def _mocked_post_response(*args, **kwargs):
        created_by_id = "c06ac6a2-52e0-431e-b878-9df624edc8b8"
        organization_id = "046fe9bc-c85b-4e95-b1af-e4ce36ba5384"

        return mock_response_factory(
            {
                "data": {
                    "id": checkpoint_id,
                    "attributes": {
                        "checkpoint_config": copy.deepcopy(checkpoint_config_with_ids),
                        "class_name": "Checkpoint",
                        "created_by_id": created_by_id,
                        "default_validation_id": "4cb29141-db66-4dac-a74b-8360779e3da3",
                        "description": "My First checkpoint.",
                        "id": checkpoint_id,
                        "name": "oss_test_checkpoint",
                        "organization_id": f"{organization_id}",
                    },
                    "type": "checkpoint",
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
def mocked_put_response(
    mock_response_factory: Callable, checkpoint_id: str, validation_ids: Tuple[str, str]
) -> Callable[[], MockResponse]:
    def _mocked_put_response(*args, **kwargs):
        return mock_response_factory(
            {},
            204,
        )

    return _mocked_put_response


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
                        "checkpoint_config": copy.deepcopy(checkpoint_config_with_ids),
                        "class_name": "Checkpoint",
                        "created_by_id": created_by_id,
                        "default_validation_id": "4cb29141-db66-4dac-a74b-8360779e3da3",
                        "description": "My First checkpoint.",
                        "id": checkpoint_id,
                        "name": "oss_test_checkpoint",
                        "organization_id": f"{organization_id}",
                    },
                    "id": checkpoint_id,
                    "type": "checkpoint",
                },
            },
            200,
        )

    return _mocked_get_response


@pytest.fixture
def mocked_get_by_name_response_1_result(
    mock_response_factory: Callable,
    checkpoint_config_with_ids: dict,
    checkpoint_id: str,
) -> Callable[[], MockResponse]:
    def _mocked_get_by_name_response(*args, **kwargs):
        created_by_id = "c06ac6a2-52e0-431e-b878-9df624edc8b8"
        organization_id = "046fe9bc-c85b-4e95-b1af-e4ce36ba5384"

        return mock_response_factory(
            {
                "data": [
                    {
                        "attributes": {
                            "checkpoint_config": copy.deepcopy(
                                checkpoint_config_with_ids
                            ),
                            "class_name": "Checkpoint",
                            "created_by_id": created_by_id,
                            "default_validation_id": "4cb29141-db66-4dac-a74b-8360779e3da3",
                            "description": "My First checkpoint.",
                            "id": checkpoint_id,
                            "name": "oss_test_checkpoint",
                            "organization_id": f"{organization_id}",
                        },
                        "id": checkpoint_id,
                        "type": "checkpoint",
                    }
                ],
            },
            200,
        )

    return _mocked_get_by_name_response


@pytest.fixture
def mocked_get_by_name_response_0_results(
    mock_response_factory: Callable,
) -> Callable[[], MockResponse]:
    def _mocked_get_by_name_response(*args, **kwargs):
        return mock_response_factory(
            {
                "data": [],
            },
            200,
        )

    return _mocked_get_by_name_response


@pytest.mark.cloud
@pytest.mark.integration
def test_cloud_backed_data_context_get_checkpoint_by_name(
    empty_cloud_data_context: CloudDataContext,
    checkpoint_id: str,
    validation_ids: Tuple[str, str],
    checkpoint_config: dict,
    mocked_get_by_name_response_1_result: Callable[[], MockResponse],
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
) -> None:
    """
    A Cloud-backed context should get from a Cloud-backed CheckpointStore when calling `get_checkpoint`.
    When provided only a name, it should hit ".../checkpoints?name=my-checkpoint-name"
    """
    context = empty_cloud_data_context

    validation_id_1, validation_id_2 = validation_ids

    with mock.patch(
        "requests.Session.get",
        autospec=True,
        side_effect=mocked_get_by_name_response_1_result,
    ) as mock_get:
        checkpoint = context.get_checkpoint(name=checkpoint_config["name"])

        mock_get.assert_called_with(
            mock.ANY,  # requests.Session object
            f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/checkpoints",
            params={"name": checkpoint_config["name"]},
        )

    assert checkpoint.ge_cloud_id == checkpoint_id
    assert checkpoint.config.ge_cloud_id == checkpoint_id

    assert checkpoint.config.validations[0]["id"] == validation_id_1
    assert checkpoint.validations[0]["id"] == validation_id_1

    assert checkpoint.config.validations[1]["id"] == validation_id_2
    assert checkpoint.validations[1]["id"] == validation_id_2


@pytest.mark.cloud
@pytest.mark.unit
def test_get_checkpoint_no_identifier_raises_error(
    empty_cloud_data_context: CloudDataContext,
) -> None:
    context = empty_cloud_data_context

    with pytest.raises(ValueError):
        context.get_checkpoint()


@pytest.mark.cloud
@pytest.mark.integration
def test_cloud_backed_data_context_add_checkpoint(
    empty_cloud_data_context: CloudDataContext,
    checkpoint_id: str,
    validation_ids: Tuple[str, str],
    checkpoint_config: dict,
    mocked_post_response: Callable[[], MockResponse],
    mocked_get_response: Callable[[], MockResponse],
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
) -> None:
    """
    A Cloud-backed context should save to a Cloud-backed CheckpointStore when calling `add_checkpoint`.
    When saving, it should use the id from the response to create the checkpoint.
    """
    context = empty_cloud_data_context

    validation_id_1, validation_id_2 = validation_ids

    with mock.patch(
        "requests.Session.post", autospec=True, side_effect=mocked_post_response
    ) as mock_post, mock.patch(
        "great_expectations.data_context.store.GXCloudStoreBackend._has_key",
        autospec=True,
        return_value=False,
    ):
        checkpoint = context.add_checkpoint(**checkpoint_config)

        # Round trip through schema to mimic updates made during store serialization process
        expected_checkpoint_config = checkpointConfigSchema.dump(
            CheckpointConfig(**checkpoint_config)
        )

        mock_post.assert_called_with(
            mock.ANY,  # requests.Session object
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
        )

    assert checkpoint.ge_cloud_id == checkpoint_id
    assert checkpoint.config.ge_cloud_id == checkpoint_id

    assert checkpoint.config.validations[0]["id"] == validation_id_1
    assert checkpoint.validations[0]["id"] == validation_id_1

    assert checkpoint.config.validations[1]["id"] == validation_id_2
    assert checkpoint.validations[1]["id"] == validation_id_2


@pytest.mark.cloud
@pytest.mark.integration
def test_add_checkpoint_updates_existing_checkpoint_in_cloud_backend(
    empty_cloud_data_context: CloudDataContext,
    checkpoint_config: dict,
    checkpoint_id: str,
    mocked_post_response: Callable[[], MockResponse],
    mocked_put_response: Callable[[], MockResponse],
    mocked_get_response: Callable[[], MockResponse],
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
) -> None:
    context = empty_cloud_data_context

    with mock.patch(
        "requests.Session.put", autospec=True, side_effect=mocked_put_response
    ) as mock_put, mock.patch(
        "requests.Session.get", autospec=True, side_effect=mocked_get_response
    ) as mock_get:
        checkpoint = context.add_checkpoint(
            ge_cloud_id=checkpoint_id, **checkpoint_config
        )

        # Round trip through schema to mimic updates made during store serialization process
        expected_checkpoint_config = checkpointConfigSchema.dump(
            CheckpointConfig(**checkpoint_config)
        )

        # Always called by store after POST and PATCH calls
        assert mock_get.call_count == 3
        mock_get.assert_called_with(
            mock.ANY,  # requests.Session object
            f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/checkpoints",
            params={"name": "oss_test_checkpoint"},
        )

        expected_checkpoint_config["ge_cloud_id"] = checkpoint_id

        # Called during creation of `checkpoint` (which is `checkpoint_1` but updated)
        mock_put.assert_called_once_with(
            mock.ANY,  # requests.Session object
            f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/checkpoints/{checkpoint_id}",
            json={
                "data": {
                    "type": "checkpoint",
                    "attributes": {
                        "checkpoint_config": expected_checkpoint_config,
                        "organization_id": ge_cloud_organization_id,
                    },
                    "id": checkpoint_id,
                },
            },
        )

    assert checkpoint.ge_cloud_id == checkpoint_id


@pytest.mark.cloud
@pytest.mark.integration
def test_cloud_backed_data_context_add_or_update_checkpoint_adds(
    empty_cloud_data_context: CloudDataContext,
    checkpoint_id: str,
    validation_ids: Tuple[str, str],
    checkpoint_config: dict,
    mocked_post_response: Callable[[], MockResponse],
    mocked_get_response: Callable[[], MockResponse],
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
) -> None:
    """
    A Cloud-backed context should save to a Cloud-backed CheckpointStore when calling `add_checkpoint`.
    When saving, it should use the id from the response to create the checkpoint.
    """
    context = empty_cloud_data_context

    validation_id_1, validation_id_2 = validation_ids

    with mock.patch(
        "requests.Session.post", autospec=True, side_effect=mocked_post_response
    ) as mock_post, mock.patch(
        "great_expectations.data_context.store.GXCloudStoreBackend._get",
        autospec=True,
        side_effect=StoreBackendError("Does not exist."),
    ):
        checkpoint = context.add_or_update_checkpoint(**checkpoint_config)

        # Round trip through schema to mimic updates made during store serialization process
        expected_checkpoint_config = checkpointConfigSchema.dump(
            CheckpointConfig(**checkpoint_config)
        )

        mock_post.assert_called_with(
            mock.ANY,  # requests.Session object
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
        )

    assert checkpoint.ge_cloud_id == checkpoint_id
    assert checkpoint.config.ge_cloud_id == checkpoint_id

    assert checkpoint.config.validations[0]["id"] == validation_id_1
    assert checkpoint.validations[0]["id"] == validation_id_1

    assert checkpoint.config.validations[1]["id"] == validation_id_2
    assert checkpoint.validations[1]["id"] == validation_id_2


@pytest.mark.cloud
@pytest.mark.integration
def test_cloud_backed_data_context_add_or_update_checkpoint_adds_when_id_not_present(
    empty_cloud_data_context: CloudDataContext,
    checkpoint_id: str,
    validation_ids: Tuple[str, str],
    checkpoint_config: dict,
    mocked_post_response: Callable[[], MockResponse],
    mocked_get_by_name_response_0_results: Callable[[], MockResponse],
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
) -> None:
    """
    A Cloud-backed context should save to a Cloud-backed CheckpointStore when calling `add_checkpoint`.
    When saving, it should use the id from the response to create the checkpoint.
    """
    context = empty_cloud_data_context

    validation_id_1, validation_id_2 = validation_ids

    with mock.patch(
        "requests.Session.post", autospec=True, side_effect=mocked_post_response
    ) as mock_post, mock.patch(
        "requests.Session.get",
        autospec=True,
        side_effect=mocked_get_by_name_response_0_results,
    ):
        checkpoint = context.add_or_update_checkpoint(**checkpoint_config)

        # Round trip through schema to mimic updates made during store serialization process
        expected_checkpoint_config = checkpointConfigSchema.dump(
            CheckpointConfig(**checkpoint_config)
        )

        mock_post.assert_called_with(
            mock.ANY,  # requests.Session object
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
        )

    assert checkpoint.ge_cloud_id == checkpoint_id
    assert checkpoint.config.ge_cloud_id == checkpoint_id

    assert checkpoint.config.validations[0]["id"] == validation_id_1
    assert checkpoint.validations[0]["id"] == validation_id_1

    assert checkpoint.config.validations[1]["id"] == validation_id_2
    assert checkpoint.validations[1]["id"] == validation_id_2


@pytest.mark.cloud
@pytest.mark.integration
def test_cloud_backed_data_context_add_or_update_checkpoint_updates_when_id_present(
    empty_cloud_data_context: CloudDataContext,
    checkpoint_id: str,
    validation_ids: Tuple[str, str],
    checkpoint_config_with_ids: dict,
    mocked_put_response: Callable[[], MockResponse],
    mocked_get_response: Callable[[], MockResponse],
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
) -> None:
    """
    A Cloud-backed context should save to a Cloud-backed CheckpointStore when calling `add_checkpoint`.
    When saving, it should use the id from the response to create the checkpoint.
    """
    context = empty_cloud_data_context

    validation_id_1, validation_id_2 = validation_ids

    with mock.patch(
        "requests.Session.put", autospec=True, side_effect=mocked_put_response
    ) as mock_put, mock.patch(
        "requests.Session.get",
        autospec=True,
        side_effect=mocked_get_response,
    ):
        checkpoint = context.add_or_update_checkpoint(**checkpoint_config_with_ids)

        # Round trip through schema to mimic updates made during store serialization process
        ge_cloud_id = checkpoint_config_with_ids.pop("id")
        expected_checkpoint_config = checkpointConfigSchema.dump(
            CheckpointConfig(ge_cloud_id=ge_cloud_id, **checkpoint_config_with_ids)
        )
        expected_checkpoint_config["ge_cloud_id"] = checkpoint_id

        mock_put.assert_called_once_with(
            mock.ANY,  # requests.Session object
            f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/checkpoints/{checkpoint_id}",
            json={
                "data": {
                    "type": "checkpoint",
                    "attributes": {
                        "checkpoint_config": expected_checkpoint_config,
                        "organization_id": ge_cloud_organization_id,
                    },
                    "id": checkpoint_id,
                },
            },
        )

    assert checkpoint.ge_cloud_id == checkpoint_id
    assert checkpoint.config.ge_cloud_id == checkpoint_id

    assert checkpoint.config.validations[0]["id"] == validation_id_1
    assert checkpoint.validations[0]["id"] == validation_id_1

    assert checkpoint.config.validations[1]["id"] == validation_id_2
    assert checkpoint.validations[1]["id"] == validation_id_2


@pytest.mark.cloud
@pytest.mark.integration
def test_cloud_backed_data_context_add_or_update_checkpoint_updates_when_id_not_present(
    empty_cloud_data_context: CloudDataContext,
    checkpoint_id: str,
    validation_ids: Tuple[str, str],
    checkpoint_config_with_ids: dict,
    mocked_put_response: Callable[[], MockResponse],
    mocked_get_by_name_response_1_result: Callable[[], MockResponse],
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
) -> None:
    """
    A Cloud-backed context should save to a Cloud-backed CheckpointStore when calling `add_checkpoint`.
    When saving, it should use the id from the response to create the checkpoint.
    """
    context = empty_cloud_data_context

    validation_id_1, validation_id_2 = validation_ids

    with mock.patch(
        "requests.Session.put", autospec=True, side_effect=mocked_put_response
    ) as mock_put, mock.patch(
        "requests.Session.get",
        autospec=True,
        side_effect=mocked_get_by_name_response_1_result,
    ):
        checkpoint_config = copy.deepcopy(checkpoint_config_with_ids)
        checkpoint_config.pop("id")
        checkpoint = context.add_or_update_checkpoint(**checkpoint_config)

        # Round trip through schema to mimic updates made during store serialization process
        expected_checkpoint_config = checkpointConfigSchema.dump(
            CheckpointConfig(**checkpoint_config)
        )

        mock_put.assert_called_once_with(
            mock.ANY,  # requests.Session object
            f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/checkpoints/{checkpoint_id}",
            json={
                "data": {
                    "type": "checkpoint",
                    "attributes": {
                        "checkpoint_config": expected_checkpoint_config,
                        "organization_id": ge_cloud_organization_id,
                    },
                    "id": checkpoint_id,
                },
            },
        )

    assert checkpoint.config.validations[0]["id"] == validation_id_1
    assert checkpoint.validations[0]["id"] == validation_id_1

    assert checkpoint.config.validations[1]["id"] == validation_id_2
    assert checkpoint.validations[1]["id"] == validation_id_2


@pytest.mark.cloud
@pytest.mark.integration
def test_cloud_backed_data_context_update_checkpoint_updates_when_id_present(
    empty_cloud_data_context: CloudDataContext,
    checkpoint_id: str,
    validation_ids: Tuple[str, str],
    checkpoint_config_with_ids: dict,
    mocked_put_response: Callable[[], MockResponse],
    mocked_get_response: Callable[[], MockResponse],
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
) -> None:
    """
    A Cloud-backed context should update a Cloud-backed CheckpointStore when calling `update_checkpoint`.
    """
    context = empty_cloud_data_context

    validation_id_1, validation_id_2 = validation_ids

    with mock.patch(
        "requests.Session.put", autospec=True, side_effect=mocked_put_response
    ) as mock_put, mock.patch(
        "requests.Session.get",
        autospec=True,
        side_effect=mocked_get_response,
    ):
        ge_cloud_id = checkpoint_config_with_ids.pop("id")
        checkpoint = context.update_checkpoint(
            Checkpoint.instantiate_from_config_with_runtime_args(
                checkpoint_config=CheckpointConfig(
                    ge_cloud_id=ge_cloud_id,
                    **checkpoint_config_with_ids,
                ),
                data_context=context,
                name=checkpoint_config_with_ids["name"],
            )
        )

        # Round trip through schema to mimic updates made during store serialization process
        expected_checkpoint_config = checkpointConfigSchema.dump(
            CheckpointConfig(ge_cloud_id=ge_cloud_id, **checkpoint_config_with_ids)
        )
        expected_checkpoint_config["ge_cloud_id"] = checkpoint_id

        mock_put.assert_called_once_with(
            mock.ANY,  # requests.Session object
            f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/checkpoints/{checkpoint_id}",
            json={
                "data": {
                    "type": "checkpoint",
                    "attributes": {
                        "checkpoint_config": expected_checkpoint_config,
                        "organization_id": ge_cloud_organization_id,
                    },
                    "id": checkpoint_id,
                },
            },
        )

    assert checkpoint.ge_cloud_id == checkpoint_id
    assert checkpoint.config.ge_cloud_id == checkpoint_id

    assert checkpoint.config.validations[0]["id"] == validation_id_1
    assert checkpoint.validations[0]["id"] == validation_id_1

    assert checkpoint.config.validations[1]["id"] == validation_id_2
    assert checkpoint.validations[1]["id"] == validation_id_2


@pytest.mark.cloud
@pytest.mark.integration
def test_cloud_backed_data_context_update_non_existent_checkpoint_when_id_not_present(
    empty_cloud_data_context: CloudDataContext,
    checkpoint_config_with_ids: dict,
    mocked_get_by_name_response_0_results: Callable[[], MockResponse],
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
) -> None:
    """
    A Cloud-backed context should raise StoreBackendError when calling `update_checkpoint` and the
    referenced Checkpoint does not exist.
    """
    context = empty_cloud_data_context

    with mock.patch(
        "requests.Session.get",
        autospec=True,
        side_effect=mocked_get_by_name_response_0_results,
    ):
        checkpoint_config = copy.deepcopy(checkpoint_config_with_ids)
        checkpoint_config.pop("id")
        with pytest.raises(CheckpointNotFoundError):
            context.update_checkpoint(
                Checkpoint.instantiate_from_config_with_runtime_args(
                    checkpoint_config=CheckpointConfig(**checkpoint_config),
                    data_context=context,
                    name=checkpoint_config["name"],
                )
            )


@pytest.mark.cloud
@pytest.mark.integration
def test_cloud_backed_data_context_update_checkpoint_updates_when_id_not_present(
    empty_cloud_data_context: CloudDataContext,
    checkpoint_id: str,
    validation_ids: Tuple[str, str],
    checkpoint_config_with_ids: dict,
    mocked_put_response: Callable[[], MockResponse],
    mocked_get_by_name_response_1_result: Callable[[], MockResponse],
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
) -> None:
    """
    A Cloud-backed context should update a Cloud-backed CheckpointStore when calling `update_checkpoint`.
    """
    context = empty_cloud_data_context

    validation_id_1, validation_id_2 = validation_ids

    with mock.patch(
        "requests.Session.put", autospec=True, side_effect=mocked_put_response
    ) as mock_put, mock.patch(
        "requests.Session.get",
        autospec=True,
        side_effect=mocked_get_by_name_response_1_result,
    ):
        checkpoint_config = copy.deepcopy(checkpoint_config_with_ids)
        checkpoint_config.pop("id")

        checkpoint = context.update_checkpoint(
            Checkpoint.instantiate_from_config_with_runtime_args(
                checkpoint_config=CheckpointConfig(**checkpoint_config),
                data_context=context,
                name=checkpoint_config["name"],
            )
        )

        # Round trip through schema to mimic updates made during store serialization process
        expected_checkpoint_config = checkpointConfigSchema.dump(
            CheckpointConfig(**checkpoint_config)
        )

        mock_put.assert_called_once_with(
            mock.ANY,  # requests.Session object
            f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/checkpoints/{checkpoint_id}",
            json={
                "data": {
                    "type": "checkpoint",
                    "attributes": {
                        "checkpoint_config": expected_checkpoint_config,
                        "organization_id": ge_cloud_organization_id,
                    },
                    "id": checkpoint_id,
                },
            },
        )

    assert checkpoint.config.validations[0]["id"] == validation_id_1
    assert checkpoint.validations[0]["id"] == validation_id_1

    assert checkpoint.config.validations[1]["id"] == validation_id_2
    assert checkpoint.validations[1]["id"] == validation_id_2


@pytest.mark.xfail(
    reason="GX Cloud E2E tests are currently failing due to a schema issue with DataContextVariables; xfailing for purposes of the 0.15.20 release",
    run=True,
    strict=True,
)
@pytest.mark.e2e
@pytest.mark.cloud
@mock.patch("great_expectations.data_context.DataContext._save_project_config")
def test_cloud_backed_data_context_add_checkpoint_e2e(
    mock_save_project_config: mock.MagicMock,
    checkpoint_config: dict,
) -> None:
    context = DataContext(cloud_mode=True)

    checkpoint = context.add_checkpoint(**checkpoint_config)

    ge_cloud_id = checkpoint.ge_cloud_id

    checkpoint_stored_in_cloud = context.get_checkpoint(ge_cloud_id=ge_cloud_id)

    assert checkpoint.ge_cloud_id == checkpoint_stored_in_cloud.ge_cloud_id
    assert (
        checkpoint.config.to_json_dict()
        == checkpoint_stored_in_cloud.config.to_json_dict()
    )


@pytest.mark.xfail(
    reason="GX Cloud E2E tests are currently failing due to a migration to a new CI environment",
)
@pytest.mark.e2e
@pytest.mark.cloud
def test_cloud_data_context_run_checkpoint_e2e():
    """
    What does this test do and why?

    Tests the `run_checkpoint` method against the GX Cloud dev environment.
    Note that this test relies on some prerequisite state (which has be configured
    externally of this test).

    For reference, the following scripts were run to:

    === Set up the prerequisite Datasource ===
    ```
    datasource_yaml = '''
    name: nathan_test_pandas_datasource
    class_name: Datasource
    execution_engine:
        class_name: PandasExecutionEngine
    data_connectors:
        runtime_data_connector:
            class_name: RuntimeDataConnector
            batch_identifiers: ["test_identifier"]
    '''

    datasource = context.test_yaml_config(datasource_yaml)
    datasource = context.add_datasource(datasource=datasource)
    ```

    === Set up the prerequisite Checkpoint ===
    ```
    batch_request = {
        "datasource_name": "oss_test_pandas_datasource",
        "data_connector_name": "runtime_data_connector",
        "data_asset_name": "test_df",
    }

    config = {
        "action_list": [
            {
                "action": {"class_name": "StoreValidationResultAction"},
                "name": "store_validation_result",
            },
            {
                "action": {"class_name": "StoreEvaluationParametersAction"},
                "name": "store_evaluation_params",
            },
        ],
        "batch_request": batch_request,
        "class_name": "Checkpoint",
        "config_version": 1.0,
        "evaluation_parameters": {"table_row_count": 3},
        "expectation_suite_name": "evaluation_parameters_rendering",
        "module_name": "great_expectations.checkpoint",
        "name": "OSS_E2E_run_checkpoint",
    }

    checkpoint = context.add_checkpoint(**config)
    ```
    """
    context = get_context(cloud_mode=True)

    checkpoint_name = "OSS_E2E_run_checkpoint"

    cloud_id: Optional[str] = None
    checkpoint_identifiers = context.list_checkpoints()
    for identifier in checkpoint_identifiers:
        if identifier.resource_name == checkpoint_name:
            cloud_id = identifier.id
            break

    assert cloud_id, f"Could not find checkpoint '{checkpoint_name}' in Cloud"

    test_df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        }
    )
    # The datasource/data_connector/data_asset are all preconfigured in Cloud
    batch_request = RuntimeBatchRequest(
        datasource_name="oss_test_pandas_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name="test_df",
        runtime_parameters={"batch_data": test_df},
        batch_identifiers={"test_identifier": "my_id"},
    )

    result = context.run_checkpoint(ge_cloud_id=cloud_id, batch_request=batch_request)
    assert result.success


@pytest.fixture
def checkpoint_names_and_ids() -> Tuple[Tuple[str, str], Tuple[str, str]]:
    checkpoint_name_1 = "Test Checkpoint 1"
    checkpoint_id_1 = "9db8721d-52e3-4263-90b3-ddb83a7aca04"
    checkpoint_name_2 = "Test Checkpoint 2"
    checkpoint_id_2 = "88972771-1774-4e7c-b76a-0c30063bea55"

    checkpoint_1 = (checkpoint_name_1, checkpoint_id_1)
    checkpoint_2 = (checkpoint_name_2, checkpoint_id_2)
    return checkpoint_1, checkpoint_2


@pytest.fixture
def mock_get_all_checkpoints_json(
    checkpoint_names_and_ids: Tuple[Tuple[str, str], Tuple[str, str]]
) -> dict:
    checkpoint_1, checkpoint_2 = checkpoint_names_and_ids
    checkpoint_name_1, checkpoint_id_1 = checkpoint_1
    checkpoint_name_2, checkpoint_id_2 = checkpoint_2

    mock_json = {
        "data": [
            {
                "attributes": {
                    "checkpoint_config": {
                        "action_list": [],
                        "batch_request": {},
                        "class_name": "Checkpoint",
                        "config_version": 1.0,
                        "evaluation_parameters": {},
                        "module_name": "great_expectations.checkpoint",
                        "name": checkpoint_name_1,
                        "profilers": [],
                        "run_name_template": None,
                        "runtime_configuration": {},
                        "template_name": None,
                        "validations": [
                            {
                                "batch_request": {
                                    "data_asset_name": "my_data_asset",
                                    "data_connector_name": "my_data_connector",
                                    "data_connector_query": {"index": 0},
                                    "datasource_name": "data__dir",
                                },
                                "expectation_suite_name": "raw_health.critical",
                            }
                        ],
                    },
                    "class_name": "Checkpoint",
                    "created_by_id": "329eb0a6-6559-4221-8b27-131a9185118d",
                    "default_validation_id": None,
                    "id": checkpoint_id_1,
                    "name": checkpoint_name_1,
                    "organization_id": "77eb8b08-f2f4-40b1-8b41-50e7fbedcda3",
                },
                "id": checkpoint_id_1,
                "type": "checkpoint",
            },
            {
                "attributes": {
                    "checkpoint_config": {
                        "action_list": [],
                        "batch_request": {},
                        "class_name": "Checkpoint",
                        "config_version": 1.0,
                        "evaluation_parameters": {},
                        "module_name": "great_expectations.checkpoint",
                        "name": checkpoint_name_2,
                        "profilers": [],
                        "run_name_template": None,
                        "runtime_configuration": {},
                        "template_name": None,
                        "validations": [
                            {
                                "batch_request": {
                                    "data_asset_name": "my_data_asset",
                                    "data_connector_name": "my_data_connector",
                                    "data_connector_query": {"index": 0},
                                    "datasource_name": "data__dir",
                                },
                                "expectation_suite_name": "raw_health.critical",
                            }
                        ],
                    },
                    "class_name": "Checkpoint",
                    "created_by_id": "329eb0a6-6559-4221-8b27-131a9185118d",
                    "default_validation_id": None,
                    "id": checkpoint_id_2,
                    "name": checkpoint_name_2,
                    "organization_id": "77eb8b08-f2f4-40b1-8b41-50e7fbedcda3",
                },
                "id": checkpoint_id_2,
                "type": "checkpoint",
            },
        ]
    }
    return mock_json


@pytest.mark.unit
@pytest.mark.cloud
def test_list_checkpoints(
    empty_ge_cloud_data_context_config: DataContextConfig,
    ge_cloud_config: GXCloudConfig,
    checkpoint_names_and_ids: Tuple[Tuple[str, str], Tuple[str, str]],
    mock_get_all_checkpoints_json: dict,
) -> None:
    project_path_name = "foo/bar/baz"

    context = get_context(
        project_config=empty_ge_cloud_data_context_config,
        context_root_dir=project_path_name,
        cloud_base_url=ge_cloud_config.base_url,
        cloud_access_token=ge_cloud_config.access_token,
        cloud_organization_id=ge_cloud_config.organization_id,
        cloud_mode=True,
    )

    checkpoint_1, checkpoint_2 = checkpoint_names_and_ids
    checkpoint_name_1, checkpoint_id_1 = checkpoint_1
    checkpoint_name_2, checkpoint_id_2 = checkpoint_2

    with mock.patch("requests.Session.get", autospec=True) as mock_get:
        mock_get.return_value = mock.Mock(
            status_code=200, json=lambda: mock_get_all_checkpoints_json
        )
        checkpoints = context.list_checkpoints()

    assert checkpoints == [
        GXCloudIdentifier(
            resource_type=GXCloudRESTResource.CHECKPOINT,
            id=checkpoint_id_1,
            resource_name=checkpoint_name_1,
        ),
        GXCloudIdentifier(
            resource_type=GXCloudRESTResource.CHECKPOINT,
            id=checkpoint_id_2,
            resource_name=checkpoint_name_2,
        ),
    ]
