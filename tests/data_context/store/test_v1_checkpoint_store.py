from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from unittest import mock

import pytest

from great_expectations.checkpoint.actions import SlackNotificationAction
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.data_context_key import StringKey
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context import AbstractDataContext
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.data_context.context_factory import set_context
from great_expectations.data_context.store.checkpoint_store import CheckpointStore
from great_expectations.data_context.types.resource_identifiers import GXCloudIdentifier

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

    from tests.datasource.fluent._fake_cloud_api import CloudDetails


@pytest.fixture
def ephemeral_store():
    return CheckpointStore(store_name="ephemeral_checkpoint_store")


@pytest.fixture
def file_backed_store(tmp_path):
    base_directory = tmp_path / "base_dir"
    return CheckpointStore(
        store_name="file_backed_checkpoint_store",
        store_backend={
            "class_name": "TupleFilesystemStoreBackend",
            "base_directory": base_directory,
        },
    )


@pytest.fixture
def cloud_backed_store(cloud_details: CloudDetails):
    return CheckpointStore(
        store_name="cloud_backed_checkpoint_store",
        store_backend={
            "class_name": "GXCloudStoreBackend",
            "ge_cloud_resource_type": GXCloudRESTResource.CHECKPOINT,
            "ge_cloud_credentials": {
                "access_token": cloud_details.access_token,
                "organization_id": cloud_details.org_id,
            },
        },
    )


@pytest.fixture
def mock_checkpoint_json() -> dict:
    return {
        "name": "my_checkpoint",
        "validation_definitions": [
            {"name": "my_first_validation", "id": "a58816-64c8-46cb-8f7e-03c12cea1d67"},
            {"name": "my_second_validation", "id": "139ab16-64c8-46cb-8f7e-03c12cea1d67"},
        ],
        "actions": [
            {
                "name": "my_slack_action",
                "slack_webhook": "https://hooks.slack.com/services/ABC123/DEF456/XYZ789",
                "notify_on": "all",
                "notify_with": ["my_data_docs_site"],
                "renderer": {
                    "class_name": "SlackRenderer",
                },
            }
        ],
        "result_format": "SUMMARY",
        "id": None,
    }


@pytest.fixture
def mock_checkpoint_dict(mocker, mock_checkpoint_json: dict) -> dict:
    context = mocker.Mock(spec=AbstractDataContext)
    set_context(context)

    data = BatchDefinition[None](name="my_batch_config")
    suite = ExpectationSuite(name="my_suite")

    return {
        "name": mock_checkpoint_json["name"],
        "validation_definitions": [
            ValidationDefinition(
                **mock_checkpoint_json["validation_definitions"][0], data=data, suite=suite
            ),
            ValidationDefinition(
                **mock_checkpoint_json["validation_definitions"][1], data=data, suite=suite
            ),
        ],
        "actions": [
            SlackNotificationAction(**mock_checkpoint_json["actions"][0]),
        ],
        "result_format": mock_checkpoint_json["result_format"],
        "id": mock_checkpoint_json["id"],
    }


@pytest.fixture
def checkpoint(
    mocker: MockerFixture, mock_checkpoint_json: dict, mock_checkpoint_dict: dict
) -> CheckpointStore:
    cp = mocker.Mock(spec=Checkpoint, name="my_checkpoint", id=None)
    cp.json.return_value = json.dumps(mock_checkpoint_json)
    cp.dict.return_value = mock_checkpoint_dict
    return cp


@pytest.mark.parametrize("store_fixture", ["ephemeral_store", "file_backed_store"])
@pytest.mark.unit
def test_add(request, store_fixture: str, checkpoint: Checkpoint):
    store: CheckpointStore = request.getfixturevalue(store_fixture)
    key = store.get_key(name="my_checkpoint")

    assert not checkpoint.id
    store.add(key=key, value=checkpoint)

    assert checkpoint.id


@pytest.mark.cloud
def test_add_cloud(cloud_backed_store: CheckpointStore, checkpoint: Checkpoint):
    store = cloud_backed_store

    id = "5a8ada9f-5b71-461b-b1af-f1d93602a156"

    name = "my_checkpoint"
    key = GXCloudIdentifier(
        resource_type=GXCloudRESTResource.CHECKPOINT,
        id=id,
        resource_name=name,
    )

    with mock.patch("requests.Session.put", autospec=True) as mock_put:
        store.add(key=key, value=checkpoint)

    mock_put.assert_called_once_with(
        mock.ANY,  # requests Session
        f"https://api.greatexpectations.io/api/v1/organizations/12345678-1234-5678-1234-567812345678/checkpoints/{id}",
        json={
            "data": {
                "name": name,
                "id": None,
                "result_format": "SUMMARY",
                "validation_definitions": [
                    {
                        "id": "a58816-64c8-46cb-8f7e-03c12cea1d67",
                        "name": "my_first_validation",
                    },
                    {
                        "id": "139ab16-64c8-46cb-8f7e-03c12cea1d67",
                        "name": "my_second_validation",
                    },
                ],
                "actions": [
                    {
                        "name": "my_slack_action",
                        "notify_on": "all",
                        "notify_with": ["my_data_docs_site"],
                        "renderer": {"class_name": "SlackRenderer"},
                        "slack_webhook": "https://hooks.slack.com/services/ABC123/DEF456/XYZ789",
                    },
                ],
            }
        },
    )


@pytest.mark.parametrize("store_fixture", ["ephemeral_store", "file_backed_store"])
@pytest.mark.unit
def test_get_key(request, store_fixture: str):
    store: CheckpointStore = request.getfixturevalue(store_fixture)

    name = "my_checkpoint"
    assert store.get_key(name=name) == StringKey(key=name)


@pytest.mark.cloud
def test_get_key_cloud(cloud_backed_store: CheckpointStore):
    key = cloud_backed_store.get_key(name="my_checkpoint")
    assert key.resource_type == GXCloudRESTResource.CHECKPOINT  # type: ignore[union-attr]
    assert key.resource_name == "my_checkpoint"  # type: ignore[union-attr]


def _create_checkpoint_config(name: str, id: str) -> dict[str, Any]:
    return {
        "name": name,
        "validation_definitions": [
            {"name": "my_first_validation", "id": "a58816-64c8-46cb-8f7e-03c12cea1d67"},
            {"name": "my_second_validation", "id": "139ab16-64c8-46cb-8f7e-03c12cea1d67"},
        ],
        "actions": [
            {
                "name": "my_slack_action",
                "slack_webhook": "https://hooks.slack.com/services/ABC123/DEF456/XYZ789",
                "notify_on": "all",
                "notify_with": ["my_data_docs_site"],
                "renderer": {
                    "class_name": "SlackRenderer",
                },
            }
        ],
        "result_format": "SUMMARY",
        "id": id,
    }


_CHECKPOINT_ID = "a4sdfd-64c8-46cb-8f7e-03c12cea1d67"
_CHECKPOINT_CONFIG = _create_checkpoint_config("my_checkpoint", _CHECKPOINT_ID)


@pytest.mark.unit
@pytest.mark.parametrize(
    "response_json",
    [
        pytest.param(
            {
                "data": {
                    **_CHECKPOINT_CONFIG,
                    "id": _CHECKPOINT_ID,
                }
            },
            id="single_checkpoint_config",
        ),
        pytest.param(
            {
                "data": [
                    {
                        **_CHECKPOINT_CONFIG,
                        "id": _CHECKPOINT_ID,
                    }
                ]
            },
            id="list_with_single_checkpoint_config",
        ),
    ],
)
def test_gx_cloud_response_json_to_object_dict_success(response_json: dict):
    actual = CheckpointStore.gx_cloud_response_json_to_object_dict(response_json)
    expected = {**_CHECKPOINT_CONFIG, "id": _CHECKPOINT_ID}
    assert actual == expected


@pytest.mark.unit
def test_gx_cloud_response_json_to_object_collection():
    id_a = "a4sdfd-64c8-46cb-8f7e-03c12cea1d67"
    id_b = "a4sdfd-64c8-46cb-8f7e-03c12cea1d68"
    config_a = _create_checkpoint_config("my_checkpoint_a", "something else?")
    config_b = _create_checkpoint_config("my_checkpoint_b", id_b)
    response_json = {
        "data": [{**config_a, "id": id_a}, {**config_b, "id": id_b}],
    }

    result = CheckpointStore.gx_cloud_response_json_to_object_collection(response_json)

    expected = [{**config_a, "id": id_a}, {**config_b, "id": id_b}]
    assert result == expected


@pytest.mark.cloud
@pytest.mark.parametrize(
    "response_json, error_substring",
    [
        pytest.param(
            {
                "data": [],
            },
            "Cannot parse empty data",
            id="empty_list",
        ),
        pytest.param(
            {
                "data": [
                    {
                        "id": _CHECKPOINT_ID,
                        "attributes": {
                            "checkpoint_config": _CHECKPOINT_CONFIG,
                        },
                    },
                    {
                        "id": _CHECKPOINT_ID,
                        "attributes": {
                            "checkpoint_config": _CHECKPOINT_CONFIG,
                        },
                    },
                ],
            },
            "Cannot parse multiple items",
            id="list_with_multiple_checkpoint_configs",
        ),
    ],
)
def test_gx_cloud_response_json_to_object_dict_failure(response_json: dict, error_substring: str):
    with pytest.raises(ValueError, match=f"{error_substring}*."):
        CheckpointStore.gx_cloud_response_json_to_object_dict(response_json)


@pytest.mark.unit
def test_update_failure_wraps_store_backend_error(
    ephemeral_store: CheckpointStore, checkpoint: Checkpoint
):
    key = ephemeral_store.get_key(name="my_nonexistant_checkpoint")

    with pytest.raises(ValueError) as e:
        ephemeral_store.update(key=key, value=checkpoint)

    assert "Could not update Checkpoint" in str(e.value)
