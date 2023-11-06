from typing import Callable, Tuple
from unittest import mock

import pytest

from great_expectations.data_context import get_context
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.data_context.data_context import DataContext
from great_expectations.data_context.types.base import DataContextConfig, GXCloudConfig
from great_expectations.data_context.types.resource_identifiers import GXCloudIdentifier
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler
from tests.data_context.conftest import MockResponse


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
        id=profiler_id,
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


@pytest.mark.e2e
@pytest.mark.cloud
@mock.patch("great_expectations.data_context.DataContext._save_project_config")
@pytest.mark.xfail(
    strict=False,
    reason="GX Cloud E2E tests are failing due to env vars not being consistently recognized by Docker; x-failing for purposes of 0.15.22 release",
)
def test_cloud_backed_data_context_add_profiler_e2e(
    mock_save_project_config: mock.MagicMock,
    profiler_rules: dict,
) -> None:
    context = DataContext(cloud_mode=True)

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


@pytest.fixture
def profiler_names_and_ids() -> Tuple[Tuple[str, str], Tuple[str, str]]:
    profiler_name_1 = "Test Profiler 1"
    profiler_id_1 = "43996c1d-ac32-493d-8e68-eb41e37ac039"
    profiler_name_2 = "Test Profiler 2"
    profiler_id_2 = "94604e27-22d6-4264-89fe-a52c9e4b654d"

    profiler_1 = (profiler_name_1, profiler_id_1)
    profiler_2 = (profiler_name_2, profiler_id_2)
    return profiler_1, profiler_2


@pytest.fixture
def mock_get_all_profilers_json(
    profiler_names_and_ids: Tuple[Tuple[str, str], Tuple[str, str]]
) -> dict:
    profiler_1, profiler_2 = profiler_names_and_ids
    profiler_name_1, profiler_id_1 = profiler_1
    profiler_name_2, profiler_id_2 = profiler_2
    mock_json = {
        "data": [
            {
                "attributes": {
                    "created_by_id": "df665fc4-1891-4ef7-9a12-a0c46015c92c",
                    "organization_id": "841d7bd5-e321-4951-9b60-deb086203c7d",
                    "profiler": {
                        "name": profiler_name_1,
                        "rules": {
                            "my_rule_for_numeric_columns": {
                                "domain_builder": {
                                    "batch_request": "$variables.my_last_month_sales_batch_request",
                                    "class_name": "SemanticTypeColumnDomainBuilder",
                                    "semantic_types": ["numeric"],
                                },
                                "expectation_configuration_builders": [
                                    {
                                        "class_name": "DefaultExpectationConfigurationBuilder",
                                        "column": "$domain.domain_kwargs.column",
                                        "expectation_type": "expect_column_values_to_be_between",
                                        "max_value": "$parameter.my_column_max.value",
                                        "min_value": "$parameter.my_column_min.value",
                                        "mostly": "$variables.mostly_default",
                                    }
                                ],
                                "parameter_builders": [
                                    {
                                        "batch_request": "$variables.my_last_month_sales_batch_request",
                                        "class_name": "MetricParameterBuilder",
                                        "metric_domain_kwargs": "$domain.domain_kwargs",
                                        "metric_name": "column.min",
                                        "parameter_name": "my_column_min",
                                    },
                                ],
                            }
                        },
                        "variables": {},
                    },
                },
                "id": profiler_id_1,
                "type": "profiler",
            },
            {
                "attributes": {
                    "created_by_id": "df665fc4-1891-4ef7-9a12-a0c46015c92c",
                    "organization_id": "841d7bd5-e321-4951-9b60-deb086203c7d",
                    "profiler": {
                        "name": profiler_name_2,
                        "rules": {
                            "my_rule_for_numeric_columns": {
                                "domain_builder": {
                                    "batch_request": "$variables.my_last_month_sales_batch_request",
                                    "class_name": "SemanticTypeColumnDomainBuilder",
                                    "semantic_types": ["numeric"],
                                },
                                "expectation_configuration_builders": [
                                    {
                                        "class_name": "DefaultExpectationConfigurationBuilder",
                                        "column": "$domain.domain_kwargs.column",
                                        "expectation_type": "expect_column_values_to_be_between",
                                        "max_value": "$parameter.my_column_max.value",
                                        "min_value": "$parameter.my_column_min.value",
                                        "mostly": "$variables.mostly_default",
                                    }
                                ],
                                "parameter_builders": [
                                    {
                                        "batch_request": "$variables.my_last_month_sales_batch_request",
                                        "class_name": "MetricParameterBuilder",
                                        "metric_domain_kwargs": "$domain.domain_kwargs",
                                        "metric_name": "column.min",
                                        "parameter_name": "my_column_min",
                                    },
                                ],
                            }
                        },
                        "variables": {},
                    },
                },
                "id": profiler_id_2,
                "type": "profiler",
            },
        ]
    }
    return mock_json


@pytest.mark.cloud
def test_list_profilers(
    empty_ge_cloud_data_context_config: DataContextConfig,
    ge_cloud_config: GXCloudConfig,
    profiler_names_and_ids: Tuple[Tuple[str, str], Tuple[str, str]],
    mock_get_all_profilers_json: dict,
) -> None:
    project_path_name = "foo/bar/baz"

    context = get_context(
        project_config=empty_ge_cloud_data_context_config,
        context_root_dir=project_path_name,
        cloud_base_url=ge_cloud_config.base_url,
        cloud_organization_id=ge_cloud_config.organization_id,
        cloud_access_token=ge_cloud_config.access_token,
        cloud_mode=True,
    )

    profiler_1, profiler_2 = profiler_names_and_ids
    profiler_name_1, profiler_id_1 = profiler_1
    profiler_name_2, profiler_id_2 = profiler_2

    with mock.patch("requests.Session.get", autospec=True) as mock_get:
        mock_get.return_value = mock.Mock(
            status_code=200, json=lambda: mock_get_all_profilers_json
        )
        profilers = context.list_profilers()

    assert profilers == [
        GXCloudIdentifier(
            resource_type=GXCloudRESTResource.PROFILER,
            id=profiler_id_1,
            resource_name=profiler_name_1,
        ),
        GXCloudIdentifier(
            resource_type=GXCloudRESTResource.PROFILER,
            id=profiler_id_2,
            resource_name=profiler_name_2,
        ),
    ]
