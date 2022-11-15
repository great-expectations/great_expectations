from typing import Callable, NamedTuple
from unittest import mock

import pytest

from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.data_context.base_data_context import (
    BaseDataContext,
)
from great_expectations.data_context.types.base import DataContextConfig, GXCloudConfig
from great_expectations.data_context.types.resource_identifiers import GXCloudIdentifier
from great_expectations.exceptions.exceptions import DataContextError
from tests.data_context.conftest import MockResponse


class SuiteIdentifierTuple(NamedTuple):
    id: str
    name: str


@pytest.fixture
def suite_1() -> SuiteIdentifierTuple:
    id = "9db8721d-52e3-4263-90b3-ddb83a7aca04"
    name = "Test Suite 1"
    return SuiteIdentifierTuple(id=id, name=name)


@pytest.fixture
def suite_2() -> SuiteIdentifierTuple:
    id = "88972771-1774-4e7c-b76a-0c30063bea55"
    name = "Test Suite 2"
    return SuiteIdentifierTuple(id=id, name=name)


@pytest.fixture
def mock_get_all_suites_json(
    suite_1: SuiteIdentifierTuple,
    suite_2: SuiteIdentifierTuple,
) -> dict:
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
                        "expectation_suite_name": suite_1.name,
                        "expectations": [
                            {
                                "expectation_type": "expect_column_to_exist",
                                "ge_cloud_id": "c8a239a6-fb80-4f51-a90e-40c38dffdf91",
                                "kwargs": {"column": "infinities"},
                                "meta": {},
                            },
                        ],
                        "ge_cloud_id": suite_1.id,
                        "meta": {"great_expectations_version": "0.15.19"},
                    },
                    "updated_at": "2022-08-18T18:34:17.561984",
                },
                "id": suite_1.id,
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
                        "expectation_suite_name": suite_2.name,
                        "expectations": [
                            {
                                "expectation_type": "expect_column_to_exist",
                                "ge_cloud_id": "c8a239a6-fb80-4f51-a90e-40c38dffdf91",
                                "kwargs": {"column": "infinities"},
                                "meta": {},
                            },
                        ],
                        "ge_cloud_id": suite_2.id,
                        "meta": {"great_expectations_version": "0.15.19"},
                    },
                    "updated_at": "2022-08-18T18:34:17.561984",
                },
                "id": suite_2.id,
                "type": "expectation_suite",
            },
        ]
    }
    return mock_json


@pytest.fixture
def mocked_post_response(
    mock_response_factory: Callable,
    suite_1: SuiteIdentifierTuple,
) -> Callable[[], MockResponse]:
    suite_id = suite_1.id

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


@pytest.fixture
def mocked_get_response(
    mock_response_factory: Callable,
    suite_1: SuiteIdentifierTuple,
) -> Callable[[], MockResponse]:
    suite_id = suite_1.id

    def _mocked_get_response(*args, **kwargs):
        return mock_response_factory(
            {
                "data": {
                    "attributes": {
                        "clause_id": "3199e1eb-3f68-473a-aca5-5e12324c3b92",
                        "created_at": "2021-12-02T16:53:31.015139",
                        "created_by_id": "67dce9ed-9c41-4607-9f22-15c14cc82ac0",
                        "deleted": False,
                        "deleted_at": None,
                        "organization_id": "c8f9f2d0-fb5c-464b-bcc9-8a45b8144f44",
                        "rendered_data_doc_id": None,
                        "suite": {
                            "data_asset_type": None,
                            "expectation_suite_name": "my_mock_suite",
                            "expectations": [
                                {
                                    "expectation_type": "expect_column_to_exist",
                                    "ge_cloud_id": "869771ee-a728-413d-96a6-8efc4dc70318",
                                    "kwargs": {"column": "infinities"},
                                    "meta": {},
                                },
                            ],
                            "ge_cloud_id": suite_id,
                        },
                    },
                    "id": suite_id,
                }
            },
            200,
        )

    return _mocked_get_response


@pytest.fixture
def mock_list_expectation_suite_names() -> mock.MagicMock:
    """
    Expects a return value to be set within the test function.
    """
    with mock.patch(
        "great_expectations.data_context.data_context.cloud_data_context.CloudDataContext.list_expectation_suite_names",
    ) as mock_method:
        yield mock_method


@pytest.fixture
def mock_list_expectation_suites() -> mock.MagicMock:
    """
    Expects a return value to be set within the test function.
    """
    with mock.patch(
        "great_expectations.data_context.data_context.cloud_data_context.CloudDataContext.list_expectation_suites",
    ) as mock_method:
        yield mock_method


@pytest.fixture
def mock_expectations_store_has_key() -> mock.MagicMock:
    """
    Expects a return value to be set within the test function.
    """
    with mock.patch(
        "great_expectations.data_context.store.expectations_store.ExpectationsStore.has_key",
    ) as mock_method:
        yield mock_method


@pytest.mark.unit
@pytest.mark.cloud
def test_list_expectation_suites(
    empty_ge_cloud_data_context_config: DataContextConfig,
    ge_cloud_config: GXCloudConfig,
    suite_1: SuiteIdentifierTuple,
    suite_2: SuiteIdentifierTuple,
    mock_get_all_suites_json: dict,
) -> None:
    project_path_name = "foo/bar/baz"

    context = BaseDataContext(
        project_config=empty_ge_cloud_data_context_config,
        context_root_dir=project_path_name,
        ge_cloud_config=ge_cloud_config,
        ge_cloud_mode=True,
    )

    with mock.patch("requests.Session.get", autospec=True) as mock_get:
        mock_get.return_value = mock.Mock(
            status_code=200, json=lambda: mock_get_all_suites_json
        )
        suites = context.list_expectation_suites()

    assert suites == [
        GXCloudIdentifier(
            resource_type=GXCloudRESTResource.EXPECTATION_SUITE,
            ge_cloud_id=suite_1.id,
            resource_name=suite_1.name,
        ),
        GXCloudIdentifier(
            resource_type=GXCloudRESTResource.EXPECTATION_SUITE,
            ge_cloud_id=suite_2.id,
            resource_name=suite_2.name,
        ),
    ]


@pytest.mark.unit
@pytest.mark.cloud
def test_create_expectation_suite_saves_suite_to_cloud(
    empty_base_data_context_in_cloud_mode: BaseDataContext,
    mocked_post_response: Callable[[], MockResponse],
    mock_list_expectation_suite_names: mock.MagicMock,
) -> None:
    context = empty_base_data_context_in_cloud_mode

    suite_name = "my_suite"
    existing_suite_names = []

    with mock.patch(
        "requests.Session.post", autospec=True, side_effect=mocked_post_response
    ):
        mock_list_expectation_suite_names.return_value = existing_suite_names
        suite = context.create_expectation_suite(suite_name)

    assert suite.ge_cloud_id is not None


@pytest.mark.unit
@pytest.mark.cloud
def test_create_expectation_suite_overwrites_existing_suite(
    empty_base_data_context_in_cloud_mode: BaseDataContext,
    mock_list_expectation_suite_names: mock.MagicMock,
    mock_list_expectation_suites: mock.MagicMock,
    suite_1: SuiteIdentifierTuple,
) -> None:
    context = empty_base_data_context_in_cloud_mode

    suite_name = suite_1.name
    existing_suite_names = [suite_name]
    suite_id = suite_1.id

    with mock.patch(
        "great_expectations.data_context.data_context.cloud_data_context.CloudDataContext.expectations_store"
    ):
        mock_list_expectation_suite_names.return_value = existing_suite_names
        mock_list_expectation_suites.return_value = [
            GXCloudIdentifier(
                resource_type=GXCloudRESTResource.EXPECTATION,
                ge_cloud_id=suite_id,
                resource_name=suite_name,
            )
        ]
        suite = context.create_expectation_suite(
            expectation_suite_name=suite_name, overwrite_existing=True
        )

    assert suite.ge_cloud_id == suite_id


@pytest.mark.unit
@pytest.mark.cloud
def test_create_expectation_suite_namespace_collision_raises_error(
    empty_base_data_context_in_cloud_mode: BaseDataContext,
    mock_list_expectation_suite_names: mock.MagicMock,
) -> None:
    context = empty_base_data_context_in_cloud_mode

    suite_name = "my_suite"
    existing_suite_names = [suite_name]

    with pytest.raises(DataContextError) as e:
        mock_list_expectation_suite_names.return_value = existing_suite_names
        context.create_expectation_suite(suite_name)

    assert f"expectation_suite '{suite_name}' already exists" in str(e.value)


@pytest.mark.unit
@pytest.mark.cloud
def test_delete_expectation_suite_deletes_suite_in_cloud(
    empty_base_data_context_in_cloud_mode: BaseDataContext,
    suite_1: SuiteIdentifierTuple,
    mock_expectations_store_has_key: mock.MagicMock,
) -> None:
    context = empty_base_data_context_in_cloud_mode
    suite_id = suite_1.id

    with mock.patch("requests.Session.delete", autospec=True) as mock_delete:
        mock_expectations_store_has_key.return_value = True
        context.delete_expectation_suite(ge_cloud_id=suite_id)

    mock_expectations_store_has_key.assert_called_once_with(
        GXCloudIdentifier(GXCloudRESTResource.EXPECTATION_SUITE, ge_cloud_id=suite_id)
    )
    assert mock_delete.call_args[1]["json"] == {
        "data": {
            "type": GXCloudRESTResource.EXPECTATION_SUITE,
            "id": suite_id,
            "attributes": {"deleted": True},
        }
    }


@pytest.mark.unit
@pytest.mark.cloud
def test_delete_expectation_suite_nonexistent_suite_raises_error(
    empty_base_data_context_in_cloud_mode: BaseDataContext,
    suite_1: SuiteIdentifierTuple,
    mock_expectations_store_has_key: mock.MagicMock,
) -> None:
    context = empty_base_data_context_in_cloud_mode
    suite_id = suite_1.id

    with pytest.raises(DataContextError) as e:
        mock_expectations_store_has_key.return_value = False
        context.delete_expectation_suite(ge_cloud_id=suite_id)

    mock_expectations_store_has_key.assert_called_once_with(
        GXCloudIdentifier(GXCloudRESTResource.EXPECTATION_SUITE, ge_cloud_id=suite_id)
    )
    assert f"expectation_suite with id {suite_id} does not exist" in str(e.value)


@pytest.mark.unit
@pytest.mark.cloud
def test_get_expectation_suite_retrieves_suite_from_cloud(
    empty_base_data_context_in_cloud_mode: BaseDataContext,
    suite_1: SuiteIdentifierTuple,
    mocked_get_response: Callable[[], MockResponse],
    mock_expectations_store_has_key: mock.MagicMock,
) -> None:
    context = empty_base_data_context_in_cloud_mode
    suite_id = suite_1.id

    with mock.patch(
        "requests.Session.get", autospec=True, side_effect=mocked_get_response
    ):
        mock_expectations_store_has_key.return_value = True
        suite = context.get_expectation_suite(ge_cloud_id=suite_id)

    mock_expectations_store_has_key.assert_called_once_with(
        GXCloudIdentifier(GXCloudRESTResource.EXPECTATION_SUITE, ge_cloud_id=suite_id)
    )
    assert str(suite.ge_cloud_id) == str(suite_id)


@pytest.mark.unit
@pytest.mark.cloud
def test_get_expectation_suite_nonexistent_suite_raises_error(
    empty_base_data_context_in_cloud_mode: BaseDataContext,
    mock_expectations_store_has_key: mock.MagicMock,
) -> None:
    context = empty_base_data_context_in_cloud_mode

    suite_id = "abc123"

    with pytest.raises(DataContextError) as e:
        mock_expectations_store_has_key.return_value = False
        context.get_expectation_suite(ge_cloud_id=suite_id)

    mock_expectations_store_has_key.assert_called_once_with(
        GXCloudIdentifier(GXCloudRESTResource.EXPECTATION_SUITE, ge_cloud_id=suite_id)
    )
    assert f"expectation_suite with id {suite_id} not found" in str(e.value)


@pytest.mark.unit
@pytest.mark.cloud
def test_save_expectation_suite_saves_suite_to_cloud(
    empty_base_data_context_in_cloud_mode: BaseDataContext,
    mocked_post_response: Callable[[], MockResponse],
) -> None:
    context = empty_base_data_context_in_cloud_mode

    suite_name = "my_suite"
    suite_id = None
    suite = ExpectationSuite(suite_name, ge_cloud_id=suite_id)

    assert suite.ge_cloud_id is None

    with mock.patch(
        "requests.Session.post", autospec=True, side_effect=mocked_post_response
    ):
        context.save_expectation_suite(suite)

    assert suite.ge_cloud_id is not None


@pytest.mark.unit
@pytest.mark.cloud
def test_save_expectation_suite_overwrites_existing_suite(
    empty_base_data_context_in_cloud_mode: BaseDataContext,
    suite_1: SuiteIdentifierTuple,
) -> None:
    context = empty_base_data_context_in_cloud_mode
    suite_name = suite_1.name
    suite_id = suite_1.id

    suite = ExpectationSuite(suite_name, ge_cloud_id=suite_id)

    with mock.patch(
        "requests.Session.put", autospec=True, return_value=mock.Mock(status_code=405)
    ) as mock_put, mock.patch("requests.Session.patch", autospec=True) as mock_patch:
        context.save_expectation_suite(suite)

    expected_suite_json = {
        "data_asset_type": None,
        "expectation_suite_name": suite_name,
        "expectations": [],
        "ge_cloud_id": suite_id,
    }

    actual_put_suite_json = mock_put.call_args[1]["json"]["data"]["attributes"]["suite"]
    actual_put_suite_json.pop("meta")
    assert actual_put_suite_json == expected_suite_json

    actual_patch_suite_json = mock_patch.call_args[1]["json"]["data"]["attributes"][
        "suite"
    ]
    assert actual_patch_suite_json == expected_suite_json


@pytest.mark.unit
@pytest.mark.cloud
def test_save_expectation_suite_no_overwrite_namespace_collision_raises_error(
    empty_base_data_context_in_cloud_mode: BaseDataContext,
    mock_expectations_store_has_key: mock.MagicMock,
    mock_list_expectation_suite_names: mock.MagicMock,
) -> None:
    context = empty_base_data_context_in_cloud_mode

    suite_name = "my_suite"
    suite_id = None
    suite = ExpectationSuite(suite_name, ge_cloud_id=suite_id)

    existing_suite_names = [suite_name]

    with pytest.raises(DataContextError) as e:
        mock_expectations_store_has_key.return_value = False
        mock_list_expectation_suite_names.return_value = existing_suite_names
        context.save_expectation_suite(
            expectation_suite=suite, overwrite_existing=False
        )

    assert f"expectation_suite '{suite_name}' already exists" in str(e.value)


@pytest.mark.unit
@pytest.mark.cloud
def test_save_expectation_suite_no_overwrite_id_collision_raises_error(
    empty_base_data_context_in_cloud_mode: BaseDataContext,
    suite_1: SuiteIdentifierTuple,
    mock_expectations_store_has_key: mock.MagicMock,
) -> None:
    context = empty_base_data_context_in_cloud_mode

    suite_name = "my_suite"
    suite_id = suite_1.id
    suite = ExpectationSuite(suite_name, ge_cloud_id=suite_id)

    with pytest.raises(DataContextError) as e:
        mock_expectations_store_has_key.return_value = True
        context.save_expectation_suite(
            expectation_suite=suite, overwrite_existing=False
        )

    mock_expectations_store_has_key.assert_called_once_with(
        GXCloudIdentifier(
            GXCloudRESTResource.EXPECTATION_SUITE,
            ge_cloud_id=suite_id,
            resource_name=suite_name,
        )
    )
    assert f"expectation_suite with GE Cloud ID {suite_id} already exists" in str(
        e.value
    )
