from typing import Callable, NamedTuple
from unittest import mock

import pytest

from great_expectations.core.expectation_suite import (
    ExpectationConfiguration,
    ExpectationSuite,
)
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.data_context.store.gx_cloud_store_backend import (
    GXCloudStoreBackend,
)
from great_expectations.data_context.types.base import DataContextConfig, GXCloudConfig
from great_expectations.data_context.types.resource_identifiers import GXCloudIdentifier
from great_expectations.exceptions.exceptions import DataContextError, StoreBackendError
from great_expectations.render import RenderedAtomicContent, RenderedAtomicValue
from great_expectations.util import get_context
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
                        "created_by_id": "67dce9ed-9c41-4607-9f22-15c14cc82ac0",
                        "organization_id": "c8f9f2d0-fb5c-464b-bcc9-8a45b8144f44",
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
                    "type": "expectation_suite",
                }
            },
            200,
        )

    return _mocked_get_response


@pytest.fixture
def mocked_404_response(
    mock_response_factory: Callable,
) -> Callable[[], MockResponse]:
    def _mocked_get_response(*args, **kwargs):
        return mock_response_factory(
            {},
            404,
        )

    return _mocked_get_response


@pytest.fixture
def mocked_get_by_name_response(
    mock_response_factory: Callable,
    suite_1: SuiteIdentifierTuple,
) -> Callable[[], MockResponse]:
    suite_id = suite_1.id

    def _mocked_get_by_name_response(*args, **kwargs):
        return mock_response_factory(
            {
                "data": [
                    {
                        "attributes": {
                            "created_by_id": "67dce9ed-9c41-4607-9f22-15c14cc82ac0",
                            "organization_id": "c8f9f2d0-fb5c-464b-bcc9-8a45b8144f44",
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
                        "type": "expectation_suite",
                    }
                ]
            },
            200,
        )

    return _mocked_get_by_name_response


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

    context = get_context(
        project_config=empty_ge_cloud_data_context_config,
        context_root_dir=project_path_name,
        cloud_base_url=ge_cloud_config.base_url,
        cloud_access_token=ge_cloud_config.access_token,
        cloud_organization_id=ge_cloud_config.organization_id,
        cloud_mode=True,
    )

    with mock.patch("requests.Session.get", autospec=True) as mock_get:
        mock_get.return_value = mock.Mock(
            status_code=200, json=lambda: mock_get_all_suites_json
        )
        suites = context.list_expectation_suites()

    assert suites == [
        GXCloudIdentifier(
            resource_type=GXCloudRESTResource.EXPECTATION_SUITE,
            id=suite_1.id,
            resource_name=suite_1.name,
        ),
        GXCloudIdentifier(
            resource_type=GXCloudRESTResource.EXPECTATION_SUITE,
            id=suite_2.id,
            resource_name=suite_2.name,
        ),
    ]


@pytest.mark.unit
@pytest.mark.cloud
def test_create_expectation_suite_saves_suite_to_cloud(
    empty_base_data_context_in_cloud_mode: CloudDataContext,
    mocked_post_response: Callable[[], MockResponse],
    mock_list_expectation_suite_names: mock.MagicMock,
) -> None:
    context = empty_base_data_context_in_cloud_mode

    suite_name = "my_suite"
    existing_suite_names = []

    with mock.patch(
        "requests.Session.post", autospec=True, side_effect=mocked_post_response
    ), pytest.deprecated_call():
        mock_list_expectation_suite_names.return_value = existing_suite_names
        suite = context.create_expectation_suite(suite_name)

    assert suite.ge_cloud_id is not None


@pytest.mark.unit
@pytest.mark.cloud
def test_create_expectation_suite_overwrites_existing_suite(
    empty_base_data_context_in_cloud_mode: CloudDataContext,
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
    ), pytest.deprecated_call():
        mock_list_expectation_suite_names.return_value = existing_suite_names
        mock_list_expectation_suites.return_value = [
            GXCloudIdentifier(
                resource_type=GXCloudRESTResource.EXPECTATION,
                id=suite_id,
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
    empty_base_data_context_in_cloud_mode: CloudDataContext,
    mock_list_expectation_suite_names: mock.MagicMock,
) -> None:
    context = empty_base_data_context_in_cloud_mode

    suite_name = "my_suite"
    existing_suite_names = [suite_name]

    with pytest.raises(DataContextError) as e, pytest.deprecated_call():
        mock_list_expectation_suite_names.return_value = existing_suite_names
        context.create_expectation_suite(suite_name)

    assert f"expectation_suite '{suite_name}' already exists" in str(e.value)


@pytest.mark.unit
@pytest.mark.cloud
def test_delete_expectation_suite_by_id_deletes_suite_in_cloud(
    empty_base_data_context_in_cloud_mode: CloudDataContext,
    suite_1: SuiteIdentifierTuple,
) -> None:
    context = empty_base_data_context_in_cloud_mode
    suite_id = suite_1.id

    with mock.patch("requests.Session.delete", autospec=True) as mock_delete:
        context.delete_expectation_suite(ge_cloud_id=suite_id)

    assert mock_delete.call_args[1]["json"] == {
        "data": {
            "type": GXCloudRESTResource.EXPECTATION_SUITE,
            "id": suite_id,
            "attributes": {"deleted": True},
        }
    }


@pytest.mark.unit
@pytest.mark.cloud
def test_delete_expectation_suite_by_name_deletes_suite_in_cloud(
    empty_base_data_context_in_cloud_mode: CloudDataContext,
    suite_1: SuiteIdentifierTuple,
) -> None:
    context = empty_base_data_context_in_cloud_mode
    suite_name = suite_1.name

    with mock.patch("requests.Session.delete", autospec=True) as mock_delete:
        context.delete_expectation_suite(expectation_suite_name=suite_name)

    assert (
        mock_delete.call_args[0][1]
        == "https://app.test.greatexpectations.io/organizations/bd20fead-2c31-4392"
        "-bcd1-f1e87ad5a79c/expectation-suites"
    )
    assert mock_delete.call_args[1]["params"] == {"name": suite_name}


@pytest.mark.unit
@pytest.mark.cloud
def test_delete_expectation_suite_nonexistent_suite_raises_error(
    empty_base_data_context_in_cloud_mode: CloudDataContext,
    suite_1: SuiteIdentifierTuple,
    mocked_404_response,
) -> None:
    context = empty_base_data_context_in_cloud_mode
    suite_id = suite_1.id

    with pytest.raises(StoreBackendError):
        with mock.patch(
            "requests.Session.delete", autospec=True, side_effect=mocked_404_response
        ):
            context.delete_expectation_suite(ge_cloud_id=suite_id)


@pytest.mark.unit
@pytest.mark.cloud
def test_get_expectation_suite_by_name_retrieves_suite_from_cloud(
    empty_base_data_context_in_cloud_mode: CloudDataContext,
    suite_1: SuiteIdentifierTuple,
    mocked_get_by_name_response: Callable[[], MockResponse],
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
) -> None:
    context = empty_base_data_context_in_cloud_mode
    suite_id = suite_1.id

    with mock.patch(
        "requests.Session.get", autospec=True, side_effect=mocked_get_by_name_response
    ) as mock_get:
        suite = context.get_expectation_suite(expectation_suite_name=suite_1.name)
        mock_get.assert_called_with(
            mock.ANY,
            f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/expectation-suites",
            params={"name": suite_1.name},
        )

    assert suite.ge_cloud_id == suite_id


@pytest.mark.unit
@pytest.mark.cloud
def test_get_expectation_suite_nonexistent_suite_raises_error(
    empty_base_data_context_in_cloud_mode: CloudDataContext, mocked_404_response
) -> None:
    context = empty_base_data_context_in_cloud_mode

    suite_id = "abc123"

    with pytest.raises(ValueError) as e:
        with mock.patch(
            "requests.Session.get", autospec=True, side_effect=mocked_404_response
        ):
            context.get_expectation_suite(ge_cloud_id=suite_id)

    assert "abc123" in str(e.value)


@pytest.mark.unit
@pytest.mark.cloud
def test_get_expectation_suite_no_identifier_raises_error(
    empty_base_data_context_in_cloud_mode: CloudDataContext,
) -> None:
    context = empty_base_data_context_in_cloud_mode

    with pytest.raises(ValueError):
        context.get_expectation_suite()


@pytest.mark.unit
@pytest.mark.cloud
def test_save_expectation_suite_saves_suite_to_cloud(
    empty_base_data_context_in_cloud_mode: CloudDataContext,
    mocked_post_response: Callable[[], MockResponse],
) -> None:
    context = empty_base_data_context_in_cloud_mode

    suite_name = "my_suite"
    suite_id = None
    suite = ExpectationSuite(suite_name, ge_cloud_id=suite_id)

    assert suite.ge_cloud_id is None

    with mock.patch(
        "requests.Session.post", autospec=True, side_effect=mocked_post_response
    ), pytest.deprecated_call():
        context.save_expectation_suite(suite)

    assert suite.ge_cloud_id is not None


@pytest.mark.unit
@pytest.mark.cloud
def test_save_expectation_suite_overwrites_existing_suite(
    empty_base_data_context_in_cloud_mode: CloudDataContext,
    suite_1: SuiteIdentifierTuple,
) -> None:
    context = empty_base_data_context_in_cloud_mode
    suite_name = suite_1.name
    suite_id = suite_1.id

    suite = ExpectationSuite(suite_name, ge_cloud_id=suite_id)

    with mock.patch(
        "requests.Session.put", autospec=True, return_value=mock.Mock(status_code=405)
    ) as mock_put, mock.patch(
        "requests.Session.patch", autospec=True
    ) as mock_patch, pytest.deprecated_call():
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
    empty_base_data_context_in_cloud_mode: CloudDataContext,
    mock_expectations_store_has_key: mock.MagicMock,
    mock_list_expectation_suite_names: mock.MagicMock,
) -> None:
    context = empty_base_data_context_in_cloud_mode

    suite_name = "my_suite"
    suite_id = None
    suite = ExpectationSuite(suite_name, ge_cloud_id=suite_id)

    existing_suite_names = [suite_name]

    with pytest.raises(DataContextError) as e, pytest.deprecated_call():
        mock_expectations_store_has_key.return_value = False
        mock_list_expectation_suite_names.return_value = existing_suite_names
        context.save_expectation_suite(
            expectation_suite=suite, overwrite_existing=False
        )

    assert f"expectation_suite '{suite_name}' already exists" in str(e.value)


@pytest.mark.unit
@pytest.mark.cloud
def test_save_expectation_suite_no_overwrite_id_collision_raises_error(
    empty_base_data_context_in_cloud_mode: CloudDataContext,
    suite_1: SuiteIdentifierTuple,
    mock_expectations_store_has_key: mock.MagicMock,
) -> None:
    context = empty_base_data_context_in_cloud_mode

    suite_name = "my_suite"
    suite_id = suite_1.id
    suite = ExpectationSuite(suite_name, ge_cloud_id=suite_id)

    with pytest.raises(DataContextError) as e, pytest.deprecated_call():
        mock_expectations_store_has_key.return_value = True
        context.save_expectation_suite(
            expectation_suite=suite, overwrite_existing=False
        )

    mock_expectations_store_has_key.assert_called_once_with(
        GXCloudIdentifier(
            GXCloudRESTResource.EXPECTATION_SUITE,
            id=suite_id,
            resource_name=suite_name,
        )
    )
    assert f"expectation_suite with GX Cloud ID {suite_id} already exists" in str(
        e.value
    )


@pytest.mark.unit
@pytest.mark.cloud
def test_add_or_update_expectation_suite_adds_new_obj(
    empty_base_data_context_in_cloud_mode: CloudDataContext,
):
    context = empty_base_data_context_in_cloud_mode
    mock_expectations_store_has_key.return_value = True

    name = "my_suite"
    suite = ExpectationSuite(expectation_suite_name=name)

    with mock.patch(
        f"{GXCloudStoreBackend.__module__}.{GXCloudStoreBackend.__name__}.has_key",
        return_value=False,
    ), mock.patch(
        "requests.Session.get", autospec=True, side_effect=DataContextError("not found")
    ) as mock_get, mock.patch(
        "requests.Session.post",
        autospec=True,
    ) as mock_post:
        context.add_or_update_expectation_suite(expectation_suite=suite)

    mock_get.assert_called_once()  # check if resource exists
    mock_post.assert_called_once()  # persist resource


@pytest.mark.unit
@pytest.mark.cloud
def test_add_expectation_suite_without_name_raises_error(
    empty_base_data_context_in_cloud_mode: CloudDataContext,
):
    context = empty_base_data_context_in_cloud_mode

    with pytest.raises(ValueError):
        context.add_expectation_suite(expectation_suite_name=None)


@pytest.mark.unit
@pytest.mark.cloud
def test_expectation_suite_gx_cloud_identifier_requires_id_or_resource_name(
    empty_base_data_context_in_cloud_mode: CloudDataContext,
):
    context = empty_base_data_context_in_cloud_mode

    key = GXCloudIdentifier(resource_type=GXCloudRESTResource.EXPECTATION_SUITE)

    with pytest.raises(ValueError):
        context.expectations_store._validate_key(key=key)


@pytest.mark.unit
@pytest.mark.cloud
def test_add_or_update_expectation_suite_updates_existing_obj(
    empty_base_data_context_in_cloud_mode: CloudDataContext, mocked_get_by_name_response
):
    context = empty_base_data_context_in_cloud_mode
    mock_expectations_store_has_key.return_value = True

    name = "my_suite"
    id = "861955f0-121e-40ea-9f4f-7b4fc78d9225"
    suite = ExpectationSuite(expectation_suite_name=name, ge_cloud_id=id)

    with mock.patch(
        f"{GXCloudStoreBackend.__module__}.{GXCloudStoreBackend.__name__}.has_key",
        return_value=True,
    ), mock.patch(
        "requests.Session.get", autospec=True, side_effect=mocked_get_by_name_response
    ) as mock_get, mock.patch(
        "requests.Session.put", autospec=True
    ) as mock_put:
        context.add_or_update_expectation_suite(expectation_suite=suite)

    assert mock_get.call_count == 2  # check if resource exists, get updated resource
    mock_put.assert_called_once()  # persist resource


@pytest.mark.integration
def test_get_expectation_suite_include_rendered_content_prescriptive(
    empty_data_context,
):
    context = empty_data_context

    expectation_suite_name = "validating_taxi_data"

    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_max_to_be_between",
        kwargs={
            "column": "passenger_count",
            "min_value": {"$PARAMETER": "upstream_column_min"},
            "max_value": {"$PARAMETER": "upstream_column_max"},
        },
    )

    context.add_expectation_suite(
        expectation_suite_name=expectation_suite_name,
        expectations=[expectation_configuration],
    )

    expectation_suite_exclude_rendered_content: ExpectationSuite = (
        context.get_expectation_suite(
            expectation_suite_name=expectation_suite_name,
        )
    )
    assert (
        expectation_suite_exclude_rendered_content.expectations[0].rendered_content
        is None
    )

    expected_expectation_configuration_prescriptive_rendered_content = [
        RenderedAtomicContent(
            value_type="StringValueType",
            value=RenderedAtomicValue(
                schema={"type": "com.superconductive.rendered.string"},
                template="$column maximum value must be greater than or equal to $min_value and less than or equal to $max_value.",
                params={
                    "column": {
                        "schema": {"type": "string"},
                        "value": "passenger_count",
                    },
                    "min_value": {
                        "schema": {"type": "object"},
                        "value": {"$PARAMETER": "upstream_column_min"},
                    },
                    "max_value": {
                        "schema": {"type": "object"},
                        "value": {"$PARAMETER": "upstream_column_max"},
                    },
                },
            ),
            name="atomic.prescriptive.summary",
        )
    ]

    expectation_suite_include_rendered_content: ExpectationSuite = (
        context.get_expectation_suite(
            expectation_suite_name=expectation_suite_name,
            include_rendered_content=True,
        )
    )
    assert (
        expectation_suite_include_rendered_content.expectations[0].rendered_content
        == expected_expectation_configuration_prescriptive_rendered_content
    )
