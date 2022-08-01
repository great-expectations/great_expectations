from unittest import mock

from great_expectations.data_context.data_context.base_data_context import (
    BaseDataContext,
)
from great_expectations.datasource.new_datasource import BaseDatasource


def test_base_data_context_in_cloud_mode_add_checkpoint(
    empty_base_data_context_in_cloud_mode: BaseDataContext,
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    request_headers: dict,
):
    """A BaseDataContext in cloud mode should save to the cloud backed Datasource store when calling add_datasource
    with save_changes=True and not save when save_changes=False. When saving, it should use the id from the response
    to create the datasource."""

    context: BaseDataContext = empty_base_data_context_in_cloud_mode
    # Make sure the fixture has the right configuration
    assert isinstance(context, BaseDataContext)
    assert context.ge_cloud_mode
    assert len(context.list_checkpoints()) == 0

    validation_id_1 = "some_uuid_1"
    validation_id_2 = "some_uuid_2"
    validation_id_3 = "some_uuid_3"

    def mocked_post_response(*args, **kwargs):
        class MockResponse:
            def __init__(self, json_data: dict, status_code: int) -> None:
                self._json_data = json_data
                self._status_code = status_code

            def json(self):
                return self._json_data

        return MockResponse(
            {
                "data": {
                    "validations": [
                        {"id": validation_id_1},
                        {"id": validation_id_2},
                        {"id": validation_id_3},
                    ]
                }
            },
            201,
        )

    with mock.patch(
        "requests.post", autospec=True, side_effect=mocked_post_response
    ) as mock_post:

        # Call add_datasource with and without the name field included in the datasource config
        context.add_checkpoint()

    # Make sure we have stored our datasource in the context
    assert len(context.list_checkpoints()) == 1

    retrieved_checkpoint: Checkpoint = context.get_checkpoint(datasource_name)
