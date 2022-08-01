import copy
from typing import Callable, Tuple
from unittest import mock

import pytest

from great_expectations.data_context.data_context.base_data_context import (
    BaseDataContext,
)
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.data_context.data_context.data_context import DataContext
from tests.data_context.cloud_data_context.conftest import MockResponse


@pytest.fixture
def checkpoint_config() -> dict:
    checkpoint_config = {
        "name": "my_simple_checkpoint",
        "config_version": 1,
        "class_name": "SimpleCheckpoint",
        "expectation_suite_name": "my_expectation_suite",
        "validations": [
            {
                "expectation_suite_name": "taxi.demo_pass",
            },
            {
                "batch_request": {},
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
    checkpoint_id: str,
    checkpoint_config_with_ids: dict,
) -> Callable[[], MockResponse]:
    def _mocked_get_response(*args, **kwargs):
        return mock_response_factory(
            {
                "data": {
                    "id": checkpoint_id,
                    "attributes": {
                        "checkpoint_config": checkpoint_config_with_ids,
                    },
                }
            },
            200,
        )

    return _mocked_get_response


def test_base_data_context_in_cloud_mode_add_checkpoint(
    empty_base_data_context_in_cloud_mode: BaseDataContext,
    checkpoint_id: str,
    validation_ids: Tuple[str, str],
    checkpoint_config: dict,
    mocked_post_response: Callable[[], MockResponse],
    mocked_get_response: Callable[[], MockResponse],
) -> None:
    """
    A BaseDataContext in cloud mode should save to the Cloud backed CheckpointStore when calling `add_checkpoint`.
    When saving, it should use the id from the response to create the checkpoint.
    """
    context: BaseDataContext = empty_base_data_context_in_cloud_mode

    # Make sure the fixture has the right configuration
    assert isinstance(context, BaseDataContext)
    assert context.ge_cloud_mode

    validation_id_1, validation_id_2 = validation_ids

    with mock.patch(
        "requests.post", autospec=True, side_effect=mocked_post_response
    ) as mock_post, mock.patch(
        "requests.get", autospec=True, side_effect=mocked_get_response
    ) as mock_get:

        checkpoint = context.add_checkpoint(**checkpoint_config)

        assert mock_post.call_count == 1
        assert mock_get.call_count == 1

    assert checkpoint.ge_cloud_id == checkpoint_id
    assert checkpoint.config.ge_cloud_id == checkpoint_id

    assert checkpoint.config.validations[0]["id"] == validation_id_1
    assert checkpoint.validations[0]["id"] == validation_id_1

    assert checkpoint.config.validations[1]["id"] == validation_id_2
    assert checkpoint.validations[1]["id"] == validation_id_2


def test_data_context_in_cloud_mode_add_checkpoint(
    empty_data_context_in_cloud_mode: DataContext,
    checkpoint_id: str,
    validation_ids: Tuple[str, str],
    checkpoint_config: dict,
    mocked_post_response: Callable[[], MockResponse],
    mocked_get_response: Callable[[], MockResponse],
) -> None:
    """ """
    context: DataContext = empty_data_context_in_cloud_mode

    # Make sure the fixture has the right configuration
    assert isinstance(context, DataContext)
    assert context.ge_cloud_mode

    validation_id_1, validation_id_2 = validation_ids

    with mock.patch(
        "requests.post", autospec=True, side_effect=mocked_post_response
    ) as mock_post, mock.patch(
        "requests.get", autospec=True, side_effect=mocked_get_response
    ) as mock_get:

        checkpoint = context.add_checkpoint(**checkpoint_config)

        assert mock_post.call_count == 1
        assert mock_get.call_count == 1

    assert checkpoint.ge_cloud_id == checkpoint_id
    assert checkpoint.config.ge_cloud_id == checkpoint_id

    assert checkpoint.config.validations[0]["id"] == validation_id_1
    assert checkpoint.validations[0]["id"] == validation_id_1

    assert checkpoint.config.validations[1]["id"] == validation_id_2
    assert checkpoint.validations[1]["id"] == validation_id_2


def test_cloud_data_context(
    empty_cloud_data_context: CloudDataContext,
    checkpoint_id: str,
    validation_ids: Tuple[str, str],
    checkpoint_config: dict,
    mocked_post_response: Callable[[], MockResponse],
    mocked_get_response: Callable[[], MockResponse],
) -> None:
    context: CloudDataContext = empty_cloud_data_context

    # Make sure the fixture has the right configuration
    assert isinstance(context, CloudDataContext)
    assert context.ge_cloud_mode

    validation_id_1, validation_id_2 = validation_ids

    with mock.patch(
        "requests.post", autospec=True, side_effect=mocked_post_response
    ) as mock_post, mock.patch(
        "requests.get", autospec=True, side_effect=mocked_get_response
    ) as mock_get:

        checkpoint = context.add_checkpoint(**checkpoint_config)

        assert mock_post.call_count == 1
        assert mock_get.call_count == 1

    assert checkpoint.ge_cloud_id == checkpoint_id
    assert checkpoint.config.ge_cloud_id == checkpoint_id

    assert checkpoint.config.validations[0]["id"] == validation_id_1
    assert checkpoint.validations[0]["id"] == validation_id_1

    assert checkpoint.config.validations[1]["id"] == validation_id_2
    assert checkpoint.validations[1]["id"] == validation_id_2
