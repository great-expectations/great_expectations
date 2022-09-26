"""TODO: Add docstring"""
from unittest import mock

import pytest

import great_expectations as gx
from great_expectations import DataContext


def test_cloud_migrator_test_migrate_true(empty_data_context: DataContext):
    """TODO: Test is a placeholder."""

    with pytest.raises(NotImplementedError):
        gx.CloudMigrator.migrate(test_migrate=True, context=empty_data_context)


@pytest.fixture
def serialized_configuration_bundle() -> dict:
    return {
        "checkpoints": [
            {
                "action_list": [
                    {
                        "action": {"class_name": "StoreValidationResultAction"},
                        "name": "store_validation_result",
                    },
                    {
                        "action": {"class_name": "StoreEvaluationParametersAction"},
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
                "expectation_suite_name": None,
                "ge_cloud_id": None,
                "module_name": "great_expectations.checkpoint",
                "name": "my_checkpoint",
                "profilers": [],
                "run_name_template": None,
                "runtime_configuration": {},
                "template_name": None,
                "validations": [],
            }
        ],
        "data_context_variables": {
            "checkpoint_store_name": "checkpoint_store",
            "config_variables_file_path": None,
            "config_version": 3.0,
            "data_docs_sites": {},
            "evaluation_parameter_store_name": "evaluation_parameter_store",
            "expectations_store_name": "expectations_store",
            "include_rendered_content": {
                "expectation_suite": False,
                "expectation_validation_result": False,
                "globally": False,
            },
            "notebooks": None,
            "plugins_directory": None,
            "profiler_store_name": "profiler_store",
            "stores": {
                "checkpoint_store": {
                    "class_name": "CheckpointStore",
                    "store_backend": {"class_name": "InMemoryStoreBackend"},
                },
                "evaluation_parameter_store": {
                    "class_name": "EvaluationParameterStore"
                },
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {"class_name": "InMemoryStoreBackend"},
                },
                "profiler_store": {
                    "class_name": "ProfilerStore",
                    "store_backend": {"class_name": "InMemoryStoreBackend"},
                },
                "validations_store": {
                    "class_name": "ValidationsStore",
                    "store_backend": {"class_name": "InMemoryStoreBackend"},
                },
            },
            "validations_store_name": "validations_store",
        },
        "expectation_suites": [
            {
                "data_asset_type": None,
                "expectation_suite_name": "my_suite",
                "expectations": [],
                "ge_cloud_id": None,
                "meta": {"great_expectations_version": "0.15.24+14.g6eff2678d.dirty"},
            }
        ],
        "profilers": [
            {
                "class_name": "RuleBasedProfiler",
                "config_version": 1.0,
                "module_name": "great_expectations.rule_based_profiler",
                "name": "my_profiler",
                "rules": {
                    "rule_1": {
                        "domain_builder": {
                            "class_name": "TableDomainBuilder",
                            "module_name": "great_expectations.rule_based_profiler.domain_builder",
                        },
                        "expectation_configuration_builders": [
                            {
                                "class_name": "DefaultExpectationConfigurationBuilder",
                                "column_A": "$domain.domain_kwargs.column_A",
                                "column_B": "$domain.domain_kwargs.column_B",
                                "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                                "meta": {
                                    "profiler_details": {
                                        "my_parameter_estimator": "$parameter.my_parameter.details",
                                        "note": "Important "
                                        "remarks "
                                        "about "
                                        "estimation "
                                        "algorithm.",
                                    }
                                },
                                "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                                "my_arg": "$parameter.my_parameter.value[0]",
                                "my_other_arg": "$parameter.my_parameter.value[1]",
                            }
                        ],
                        "parameter_builders": [
                            {
                                "class_name": "MetricMultiBatchParameterBuilder",
                                "metric_name": "my_metric",
                                "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                                "name": "my_parameter",
                            }
                        ],
                        "variables": {},
                    }
                },
                "variables": {},
            }
        ],
        "validation_results": [
            {
                "evaluation_parameters": {},
                "meta": {},
                "results": [],
                "statistics": {},
                "success": True,
            }
        ],
    }


@pytest.mark.unit
@pytest.mark.cloud
def test__send_configuration_bundle_sends_valid_http_request(
    serialized_configuration_bundle: dict,
):
    ge_cloud_base_url = "https://app.test.greatexpectations.io"
    ge_cloud_organization_id = "229616e2-1bbc-4849-8161-4be89b79bd36"
    ge_cloud_access_token = "d7asdh2efads9afah2e0fadf8eh20da8"

    mock_context = mock.MagicMock()
    migrator = gx.CloudMigrator(
        context=mock_context,
        ge_cloud_base_url=ge_cloud_base_url,
        ge_cloud_organization_id=ge_cloud_organization_id,
        ge_cloud_access_token=ge_cloud_access_token,
    )

    configuration_bundle = mock.MagicMock()
    serializer = mock.MagicMock()
    serializer.serialize.return_value = serialized_configuration_bundle

    with mock.patch("requests.Session.post", autospec=True) as mock_post:
        migrator._send_configuration_bundle(
            configuration_bundle=configuration_bundle, serializer=serializer
        )

    mock_post.assert_called_once_with(
        mock.ANY,
        f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/migration",
        json={
            "data": {
                "type": "migration",
                "attributes": {
                    "organization_id": ge_cloud_organization_id,
                    "bundle": serialized_configuration_bundle,
                },
            }
        },
    )
