from typing import Callable, Tuple

import pytest

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
                    "links": {"self": f"/organizations/{organization_id}/profilers/{profiler_id}"},
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
    profiler_names_and_ids: Tuple[Tuple[str, str], Tuple[str, str]],
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
                                        "batch_request": "$variables.my_last_month_sales_batch_request",  # noqa: E501
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
                                        "batch_request": "$variables.my_last_month_sales_batch_request",  # noqa: E501
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
