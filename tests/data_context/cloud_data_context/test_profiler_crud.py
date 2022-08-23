from typing import Callable
from unittest import mock

import pytest

from great_expectations.data_context.data_context.base_data_context import (
    BaseDataContext,
)
from great_expectations.data_context.data_context.data_context import DataContext
from great_expectations.rule_based_profiler.config.base import (
    ruleBasedProfilerConfigSchema,
)
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler
from tests.data_context.cloud_data_context.conftest import MockResponse


@pytest.fixture
def profiler_id() -> str:
    return "45f8a554-c90e-464e-8dff-f4008eb2cbb8"


@pytest.fixture
def profiler_without_id(profiler_rules: dict) -> RuleBasedProfiler:
    return RuleBasedProfiler(
        "my_profiler",
        config_version=1.0,
        rules=profiler_rules,
    )


@pytest.fixture
def profiler_with_id(profiler_id: str, profiler_rules: dict) -> RuleBasedProfiler:
    return RuleBasedProfiler(
        "my_profiler",
        config_version=1.0,
        id_=profiler_id,
        rules=profiler_rules,
    )


@pytest.fixture
def mocked_get_response(
    mock_response_factory: Callable,
    profiler_id: str,
    profiler_with_id: RuleBasedProfiler,
) -> Callable[[], MockResponse]:
    profiler_config_dict = profiler_with_id.config.to_json_dict()
    created_by_id = "c06ac6a2-52e0-431e-b878-9df624edc8b8"
    organization_id = "046fe9bc-c85b-4e95-b1af-e4ce36ba5384"

    def _mocked_get_response(*args, **kwargs):
        return mock_response_factory(
            {
                "data": {
                    "id": profiler_id,
                    "attributes": {
                        "profiler": profiler_config_dict,
                        "created_at": "2022-08-02T17:55:45.107550",
                        "created_by_id": created_by_id,
                        "deleted": False,
                        "deleted_at": None,
                        "desc": None,
                        "organization_id": f"{organization_id}",
                        "updated_at": "2022-08-02T17:55:45.107550",
                    },
                    "links": {
                        "self": f"/organizations/{organization_id}/profilers/{profiler_id}"
                    },
                    "type": "profiler",
                },
            },
            200,
        )

    return _mocked_get_response


@pytest.fixture
def mocked_post_response(
    mock_response_factory: Callable, profiler_id: str
) -> Callable[[], MockResponse]:
    def _mocked_post_response(*args, **kwargs):
        return mock_response_factory(
            {
                "data": {
                    "id": profiler_id,
                }
            },
            201,
        )

    return _mocked_post_response


@pytest.mark.cloud
@pytest.mark.integration
def test_profiler_save_with_existing_profiler_retrieves_obj_with_id_from_store(
    empty_base_data_context_in_cloud_mode: BaseDataContext,
    profiler_with_id: RuleBasedProfiler,
    mocked_get_response: Callable[[], MockResponse],
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    shared_called_with_request_kwargs: dict,
) -> None:
    """
    What does this test do and why?

    `DataContext.save_profiler()` should take in an input profiler with an id, save it to the GX Cloud backend,
    and return the same profiler (id and all).
    """
    context = empty_base_data_context_in_cloud_mode

    with mock.patch("requests.put", autospec=True) as mock_put, mock.patch(
        "requests.get", autospec=True, side_effect=mocked_get_response
    ) as mock_get:
        return_profiler = context.save_profiler(profiler=profiler_with_id)

    profiler_id = profiler_with_id.ge_cloud_id
    expected_profiler_config = ruleBasedProfilerConfigSchema.dump(
        profiler_with_id.config
    )

    mock_put.assert_called_once_with(
        f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/profilers/{profiler_id}",
        json={
            "data": {
                "attributes": {
                    "organization_id": ge_cloud_organization_id,
                    "profiler": expected_profiler_config,
                },
                "id": profiler_id,
                "type": "profiler",
            },
        },
        **shared_called_with_request_kwargs,
    )

    mock_get.assert_called_once_with(
        f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/profilers/{profiler_id}",
        params=None,
        **shared_called_with_request_kwargs,
    )

    assert return_profiler.ge_cloud_id == profiler_with_id.ge_cloud_id


@pytest.mark.cloud
@pytest.mark.integration
def test_profiler_save_with_new_profiler_retrieves_obj_with_id_from_store(
    empty_base_data_context_in_cloud_mode: BaseDataContext,
    profiler_without_id: RuleBasedProfiler,
    profiler_id: str,
    mocked_get_response: Callable[[], MockResponse],
    mocked_post_response: Callable[[], MockResponse],
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    shared_called_with_request_kwargs: dict,
) -> None:
    """
    What does this test do and why?

    `DataContext.save_profiler()` should take in an input profiler without an id, save it to the GX Cloud backend,
    and return the same profiler with the Cloud generated id.
    """
    context = empty_base_data_context_in_cloud_mode

    with mock.patch(
        "requests.post", autospec=True, side_effect=mocked_post_response
    ) as mock_post, mock.patch(
        "requests.get", autospec=True, side_effect=mocked_get_response
    ) as mock_get:
        return_profiler = context.save_profiler(profiler=profiler_without_id)

    expected_profiler_config = ruleBasedProfilerConfigSchema.dump(
        profiler_without_id.config
    )

    mock_post.assert_called_once_with(
        f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/profilers",
        json={
            "data": {
                "type": "profiler",
                "attributes": {
                    "organization_id": ge_cloud_organization_id,
                    "profiler": expected_profiler_config,
                },
            },
        },
        **shared_called_with_request_kwargs,
    )

    mock_get.assert_called_once_with(
        f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/profilers/{profiler_id}",
        params=None,
        **shared_called_with_request_kwargs,
    )

    assert return_profiler.ge_cloud_id == profiler_id


@pytest.mark.e2e
@pytest.mark.cloud
@mock.patch("great_expectations.data_context.DataContext._save_project_config")
def test_cloud_backed_data_context_add_profiler_e2e(
    mock_save_project_config: mock.MagicMock,
    profiler_rules: dict,
) -> None:
    context = DataContext(ge_cloud_mode=True)

    name = "oss_test_profiler"
    config_version = 1.0
    rules = profiler_rules

    profiler = context.add_profiler(
        name=name, config_version=config_version, rules=rules
    )

    ge_cloud_id = profiler.ge_cloud_id

    profiler_stored_in_cloud = context.get_profiler(ge_cloud_id=ge_cloud_id)

    assert profiler.ge_cloud_id == profiler_stored_in_cloud.ge_cloud_id
    assert profiler.to_json_dict() == profiler_stored_in_cloud.to_json_dict()
