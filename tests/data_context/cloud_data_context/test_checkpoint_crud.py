import copy
from typing import Callable
from unittest import mock

from great_expectations.data_context.data_context.base_data_context import (
    BaseDataContext,
)


def test_base_data_context_in_cloud_mode_add_checkpoint(
    empty_base_data_context_in_cloud_mode: BaseDataContext,
    mock_response_factory: Callable,
):
    """
    A BaseDataContext in cloud mode should save to the cloud backed Datasource store when calling add_datasource
    with save_changes=True and not save when save_changes=False. When saving, it should use the id from the response
    to create the datasource.
    """
    context: BaseDataContext = empty_base_data_context_in_cloud_mode

    # Make sure the fixture has the right configuration
    assert isinstance(context, BaseDataContext)
    assert context.ge_cloud_mode

    checkpoint_id = "c83e4299-6188-48c6-83b7-f6dce8ad4ab5"
    validation_id_1 = "v8764797-c486-4104-a764-1f2bf9630ee1"
    validation_id_2 = "vd0185a8-11c2-11ed-861d-0242ac120002"

    checkpoint_config = {
        "name": "my_missing_batch_request_checkpoint",
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

    checkpoint_config_with_ids = copy.deepcopy(checkpoint_config)
    checkpoint_config_with_ids["id"] = checkpoint_id
    checkpoint_config_with_ids["validations"][0]["id"] = validation_id_1
    checkpoint_config_with_ids["validations"][1]["id"] = validation_id_2

    def mocked_post_response(*args, **kwargs):
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

    def mocked_get_response(*args, **kwargs):
        return mock_response_factory(
            {
                "data": {
                    "id": checkpoint_id,
                    "attributes": {
                        "checkpoint_config": checkpoint_config_with_ids,
                    },
                }
            },
            201,
        )

    with mock.patch(
        "requests.post", autospec=True, side_effect=mocked_post_response
    ) as mock_post, mock.patch(
        "requests.get", autospec=True, side_effect=mocked_get_response
    ) as mock_get:
        # Call add_datasource with and without the name field included in the datasource config
        checkpoint = context.add_checkpoint(**checkpoint_config)

        assert mock_post.call_count == 1
        assert mock_get.call_count == 1

    assert checkpoint.ge_cloud_id == checkpoint_id
    assert checkpoint.validations[0]["id"] == validation_id_1
    assert checkpoint.validations[1]["id"] == validation_id_2
