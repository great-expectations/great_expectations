from typing import Callable, Tuple
from unittest import mock

import pytest

from great_expectations.data_context.data_context.base_data_context import (
    BaseDataContext,
)
from great_expectations.data_context.store.ge_cloud_store_backend import (
    GeCloudRESTResource,
)
from great_expectations.data_context.types.base import DataContextConfig, GeCloudConfig
from great_expectations.data_context.types.resource_identifiers import GeCloudIdentifier
from great_expectations.exceptions.exceptions import DataContextError
from tests.data_context.conftest import MockResponse


@pytest.fixture
def suite_names_and_ids() -> Tuple[Tuple[str, str], Tuple[str, str]]:
    suite_name_1 = "Test Suite 1"
    suite_id_1 = "9db8721d-52e3-4263-90b3-ddb83a7aca04"
    suite_name_2 = "Test Suite 2"
    suite_id_2 = "88972771-1774-4e7c-b76a-0c30063bea55"

    suite_1 = (suite_name_1, suite_id_1)
    suite_2 = (suite_name_2, suite_id_2)
    return suite_1, suite_2


@pytest.fixture
def mock_get_all_suites_json(
    suite_names_and_ids: Tuple[Tuple[str, str], Tuple[str, str]]
) -> dict:
    suite_1, suite_2 = suite_names_and_ids
    suite_name_1, suite_id_1 = suite_1
    suite_name_2, suite_id_2 = suite_2
    mock_json = {
        "data": [
            {
                "attributes": {
                    "clause_id": None,
                    "created_at": "2022-03-02T19:34:00.687921",
                    "created_by_id": "934e0898-6a5c-4ffd-9125-89381a46d191",
                    "deleted": False,
                    "deleted_at": None,
                    "organization_id": "77eb8b08-f2f4-40b1-8b41-50e7fbedcda3",
                    "rendered_data_doc_id": None,
                    "suite": {
                        "data_asset_type": None,
                        "expectation_suite_name": suite_name_1,
                        "expectations": [
                            {
                                "expectation_type": "expect_column_to_exist",
                                "ge_cloud_id": "c8a239a6-fb80-4f51-a90e-40c38dffdf91",
                                "kwargs": {"column": "infinities"},
                                "meta": {},
                            },
                        ],
                        "ge_cloud_id": suite_id_1,
                        "meta": {"great_expectations_version": "0.15.19"},
                    },
                    "updated_at": "2022-08-18T18:34:17.561984",
                },
                "id": suite_id_1,
                "type": "expectation_suite",
            },
            {
                "attributes": {
                    "clause_id": None,
                    "created_at": "2022-03-02T19:34:00.687921",
                    "created_by_id": "934e0898-6a5c-4ffd-9125-89381a46d191",
                    "deleted": False,
                    "deleted_at": None,
                    "organization_id": "77eb8b08-f2f4-40b1-8b41-50e7fbedcda3",
                    "rendered_data_doc_id": None,
                    "suite": {
                        "data_asset_type": None,
                        "expectation_suite_name": suite_name_2,
                        "expectations": [
                            {
                                "expectation_type": "expect_column_to_exist",
                                "ge_cloud_id": "c8a239a6-fb80-4f51-a90e-40c38dffdf91",
                                "kwargs": {"column": "infinities"},
                                "meta": {},
                            },
                        ],
                        "ge_cloud_id": suite_id_2,
                        "meta": {"great_expectations_version": "0.15.19"},
                    },
                    "updated_at": "2022-08-18T18:34:17.561984",
                },
                "id": suite_id_2,
                "type": "expectation_suite",
            },
        ]
    }
    return mock_json


@pytest.fixture
def mocked_post_response(
    mock_response_factory: Callable,
    suite_names_and_ids: Tuple[Tuple[str, str], Tuple[str, str]],
) -> Callable[[], MockResponse]:
    suite_id = suite_names_and_ids[0][1]

    def _mocked_post_response(*args, **kwargs):
        return mock_response_factory(
            {
                "data": {
                    "id": suite_id,
                }
            },
            201,
        )

    return _mocked_post_response


@pytest.mark.unit
@pytest.mark.cloud
def test_list_expectation_suites(
    empty_ge_cloud_data_context_config: DataContextConfig,
    ge_cloud_config: GeCloudConfig,
    suite_names_and_ids: Tuple[Tuple[str, str], Tuple[str, str]],
    mock_get_all_suites_json: dict,
) -> None:
    project_path_name = "foo/bar/baz"

    context = BaseDataContext(
        project_config=empty_ge_cloud_data_context_config,
        context_root_dir=project_path_name,
        ge_cloud_config=ge_cloud_config,
        ge_cloud_mode=True,
    )

    suite_1, suite_2 = suite_names_and_ids
    suite_name_1, suite_id_1 = suite_1
    suite_name_2, suite_id_2 = suite_2

    with mock.patch("requests.get", autospec=True) as mock_get:
        mock_get.return_value = mock.Mock(
            status_code=200, json=lambda: mock_get_all_suites_json
        )
        suites = context.list_expectation_suites()

    assert suites == [
        GeCloudIdentifier(
            resource_type=GeCloudRESTResource.EXPECTATION_SUITE,
            ge_cloud_id=suite_id_1,
            resource_name=suite_name_1,
        ),
        GeCloudIdentifier(
            resource_type=GeCloudRESTResource.EXPECTATION_SUITE,
            ge_cloud_id=suite_id_2,
            resource_name=suite_name_2,
        ),
    ]


@pytest.mark.unit
@pytest.mark.cloud
def test_create_expectation_suite_saves_suite_to_cloud(
    empty_base_data_context_in_cloud_mode: BaseDataContext,
    mocked_post_response: Callable[[], MockResponse],
) -> None:
    context = empty_base_data_context_in_cloud_mode

    suite_name = "my_suite"
    existing_suite_names = []

    with mock.patch(
        "great_expectations.data_context.data_context.cloud_data_context.CloudDataContext.list_expectation_suite_names",
        return_value=existing_suite_names,
    ), mock.patch("requests.post", autospec=True, side_effect=mocked_post_response):
        suite = context.create_expectation_suite(suite_name)

    assert suite.ge_cloud_id is not None


@pytest.mark.unit
@pytest.mark.cloud
def test_create_expectation_suite_overwrites_existing_suite(
    empty_base_data_context_in_cloud_mode: BaseDataContext,
    mocked_post_response: Callable[[], MockResponse],
) -> None:
    context = empty_base_data_context_in_cloud_mode

    suite_name = "my_suite"
    existing_suite_names = [suite_name]

    with mock.patch(
        "great_expectations.data_context.data_context.cloud_data_context.CloudDataContext.list_expectation_suite_names",
        return_value=existing_suite_names,
    ), mock.patch("requests.post", autospec=True, side_effect=mocked_post_response):
        suite = context.create_expectation_suite(suite_name, overwrite_existing=True)

    assert suite.ge_cloud_id is not None


@pytest.mark.unit
@pytest.mark.cloud
def test_create_expectation_suite_namespace_collision_raises_error(
    empty_base_data_context_in_cloud_mode: BaseDataContext,
) -> None:
    context = empty_base_data_context_in_cloud_mode

    suite_name = "my_suite"
    existing_suite_names = [suite_name]
    with mock.patch(
        "great_expectations.data_context.data_context.cloud_data_context.CloudDataContext.list_expectation_suite_names",
        return_value=existing_suite_names,
    ), pytest.raises(DataContextError) as e:
        context.create_expectation_suite(suite_name)

    assert f"expectation_suite '{suite_name}' already exists" in str(e.value)


@pytest.mark.unit
@pytest.mark.cloud
def test_delete_expectation_suite_deletes_suite_in_cloud(
    empty_base_data_context_in_cloud_mode: BaseDataContext,
    suite_names_and_ids: Tuple[Tuple[str, str], Tuple[str, str]],
) -> None:
    context = empty_base_data_context_in_cloud_mode

    suite_id = suite_names_and_ids[0][1]

    with mock.patch(
        "great_expectations.data_context.store.expectations_store.ExpectationsStore.has_key",
        return_value=True,
    ), mock.patch("requests.delete", autospec=True) as mock_delete:
        context.delete_expectation_suite(ge_cloud_id=suite_id)

    assert mock_delete.call_args[1]["json"] == {
        "data": {
            "type": GeCloudRESTResource.EXPECTATION_SUITE,
            "id": suite_id,
            "attributes": {"deleted": True},
        }
    }


@pytest.mark.unit
@pytest.mark.cloud
def test_delete_expectation_suite_nonexistent_suite_raises_error(
    empty_base_data_context_in_cloud_mode: BaseDataContext,
) -> None:
    context = empty_base_data_context_in_cloud_mode

    suite_id = "abc123"

    with mock.patch(
        "great_expectations.data_context.store.expectations_store.ExpectationsStore.has_key",
        return_value=False,
    ), pytest.raises(DataContextError) as e:
        context.delete_expectation_suite(ge_cloud_id=suite_id)

    assert f"expectation_suite with id {suite_id} does not exist" in str(e.value)


@pytest.mark.unit
@pytest.mark.cloud
def test_get_expectation_suite_retrieves_suite_from_cloud():
    pass


@pytest.mark.unit
@pytest.mark.cloud
def test_get_expectation_suite_nonexistent_suite_raises_error():
    pass


@pytest.mark.unit
@pytest.mark.cloud
def test_save_expectation_suite_saves_suite_to_cloud():
    pass


@pytest.mark.unit
@pytest.mark.cloud
def test_save_expectation_suite_overwrites_existing_suite():
    pass


@pytest.mark.unit
@pytest.mark.cloud
def test_save_expectation_suite_namespace_collision_raises_error():
    pass
