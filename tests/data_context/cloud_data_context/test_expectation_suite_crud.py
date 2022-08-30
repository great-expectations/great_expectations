from typing import Tuple
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
