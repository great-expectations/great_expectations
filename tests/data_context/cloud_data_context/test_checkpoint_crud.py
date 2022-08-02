import copy
from typing import Callable, Tuple, Type
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
from great_expectations.data_context.store.ge_cloud_store_backend import (
    GeCloudRESTResource,
)
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

        assert mock_post.assert_called_with(
            "https://app.test.greatexpectations.io/organizations/bd20fead-2c31-4392-bcd1-f1e87ad5a79c/contracts",
            json={
                "data": {
                    "attributes": {
                        "checkpoint_config": {
                            "action_list": [
                                {
                                    "action": {
                                        "class_name": "StoreValidationResultAction"
                                    },
                                    "name": "store_validation_result",
                                },
                                {
                                    "action": {
                                        "class_name": "StoreEvaluationParametersAction"
                                    },
                                    "name": "store_evaluation_params",
                                },
                                {
                                    "action": {
                                        "class_name": "UpdateDataDocsAction",
                                        "site_names": [],
                                    },
                                    "name": "update_data_docs",
                                },
                            ],
                            "batch_request": {},
                            "class_name": "Checkpoint",
                            "config_version": 1.0,
                            "evaluation_parameters": {},
                            "expectation_suite_ge_cloud_id": None,
                            "expectation_suite_name": "my_expectation_suite",
                            "ge_cloud_id": None,
                            "module_name": "great_expectations.checkpoint",
                            "name": "my_simple_checkpoint",
                            "profilers": [],
                            "run_name_template": None,
                            "runtime_configuration": {},
                            "template_name": None,
                            "validations": [
                                {"expectation_suite_name": "taxi.demo_pass"},
                                {},
                            ],
                        },
                        "organization_id": "bd20fead-2c31-4392-bcd1-f1e87ad5a79c",
                    },
                },
                "type": GeCloudRESTResource.CONTRACT,
            },
            headers={
                "Content-Type": "application/vnd.api+json",
                "Authorization": "Bearer 6bb5b6f5c7794892a4ca168c65c2603e",
            },
        )

        assert mock_get.call_count == 1

    assert checkpoint.ge_cloud_id == checkpoint_id
    assert checkpoint.config.ge_cloud_id == checkpoint_id

    assert checkpoint.config.validations[0]["id"] == validation_id_1
    assert checkpoint.validations[0]["id"] == validation_id_1

    assert checkpoint.config.validations[1]["id"] == validation_id_2
    assert checkpoint.validations[1]["id"] == validation_id_2
