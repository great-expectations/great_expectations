import uuid
from typing import Any, Dict

import pytest
import responses

from great_expectations.data_context.data_context.cloud_data_context import CloudDataContext

CLOUD_BASE_URL = "https://greatexpectations.io/fake-url"
ACCESS_TOKEN = "my-secret-access-token"
ORG_ID = str(uuid.uuid4())
CONTEXT_CONFIGURATION_URL = f"{CLOUD_BASE_URL}/organizations/{ORG_ID}/data-context-configuration"


def _create_cloud_config_response(
    expectation_suite_store_name_key: str,
    validation_results_store_name_key: str,
    suite_parameter_store_class_name: str,
    validation_results_store_class_name: str,
) -> Dict[str, Any]:
    return {
        "anonymous_usage_statistics": {
            "data_context_id": "6a52bdfa-e182-455b-a825-e69f076e67d6",
            "enabled": True,
        },
        "checkpoint_store_name": "default_checkpoint_store",
        "config_variables_file_path": "uncommitted/config_variables.yml",
        "config_version": 3.0,
        "data_docs_sites": {},
        "datasources": {
            "taxi_datasource": {
                "class_name": "Datasource",
                "data_connectors": {
                    "default_runtime_data_connector_name": {
                        "assets": {
                            "taxi_data": {
                                "batch_identifiers": ["runtime_batch_identifier_name"],
                                "class_name": "Asset",
                                "module_name": "great_expectations.datasource.data_connector.asset",
                            }
                        },
                        "class_name": "RuntimeDataConnector",
                        "id": "e0af346c-32ea-44e6-8908-b559c4162a70",
                        "module_name": "great_expectations.datasource.data_connector",
                        "name": "default_runtime_data_connector_name",
                    },
                    "taxi_data_connector": {
                        "base_directory": ",",
                        "class_name": "InferredAssetFilesystemDataConnector",
                        "default_regex": {
                            "group_names": ["data_asset_name"],
                            "pattern": "(.*)",
                        },
                        "id": "997a7842-195b-4374-a71b-e52f192068d1",
                        "module_name": "great_expectations.datasource.data_connector",
                        "name": "taxi_data_connector",
                    },
                },
                "execution_engine": {
                    "class_name": "PandasExecutionEngine",
                    "module_name": "great_expectations.execution_engine",
                },
                "id": "eb0c729d-9457-43a0-8b40-6ec6c79c0fef",
                "module_name": "great_expectations.datasource",
                "name": "taxi_datasource",
            }
        },
        expectation_suite_store_name_key: "suite_parameter_store",
        "expectations_store_name": "default_expectations_store",
        "include_rendered_content": {
            "expectation_suite": True,
            "expectation_validation_result": True,
            "globally": True,
        },
        "plugins_directory": "plugins/",
        "profiler_store_name": "default_profiler_store",
        "progress_bars": {
            "globally": False,
            "metric_calculations": False,
            "profilers": False,
        },
        "stores": {
            "default_checkpoint_store": {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "GXCloudStoreBackend",
                    "ge_cloud_base_url": CLOUD_BASE_URL,
                    "ge_cloud_credentials": {
                        "access_token": ACCESS_TOKEN,
                        "organization_id": ORG_ID,
                    },
                    "ge_cloud_resource_type": "checkpoint",
                    "suppress_store_backend_id": True,
                },
            },
            "default_suite_parameter_store": {"class_name": suite_parameter_store_class_name},
            "default_expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "GXCloudStoreBackend",
                    "ge_cloud_base_url": CLOUD_BASE_URL,
                    "ge_cloud_credentials": {
                        "access_token": ORG_ID,
                        "organization_id": ORG_ID,
                    },
                    "ge_cloud_resource_type": "expectation_suite",
                    "suppress_store_backend_id": True,
                },
            },
            "default_profiler_store": {
                "class_name": "ProfilerStore",
                "store_backend": {
                    "class_name": "GXCloudStoreBackend",
                    "ge_cloud_base_url": CLOUD_BASE_URL,
                    "ge_cloud_credentials": {
                        "access_token": ACCESS_TOKEN,
                        "organization_id": ORG_ID,
                    },
                    "ge_cloud_resource_name": "profilers",
                    "ge_cloud_resource_type": "profiler",
                    "suppress_store_backend_id": True,
                },
            },
            "default_validation_results_store": {
                "class_name": validation_results_store_class_name,
                "store_backend": {
                    "class_name": "GXCloudStoreBackend",
                    "ge_cloud_base_url": CLOUD_BASE_URL,
                    "ge_cloud_credentials": {
                        "access_token": ACCESS_TOKEN,
                        "organization_id": ORG_ID,
                    },
                    "ge_cloud_resource_type": "validation_result",
                    "suppress_store_backend_id": True,
                },
            },
            "suite_parameter_store": {
                "class_name": suite_parameter_store_class_name,
                "module_name": "great_expectations.data_context.store",
            },
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "base_directory": "expectations/",
                    "class_name": "TupleFilesystemStoreBackend",
                },
            },
        },
        validation_results_store_name_key: "default_validation_results_store",
    }


V0_CONFIG = _create_cloud_config_response(
    expectation_suite_store_name_key="evaluation_parameter_store_name",
    validation_results_store_name_key="validations_store_name",
    suite_parameter_store_class_name="EvaluationParameterStore",
    validation_results_store_class_name="ValidationsStore",
)

V1_CONFIG = _create_cloud_config_response(
    expectation_suite_store_name_key="suite_parameter_store_name",
    validation_results_store_name_key="validation_results_store_name",
    suite_parameter_store_class_name="SuiteParameterStore",
    validation_results_store_class_name="ValidationResultsStore",
)


@pytest.mark.parametrize(
    ("config",),
    [
        (V0_CONFIG,),
        (V1_CONFIG,),
    ],
)
@responses.activate
@pytest.mark.unit
def test_parses_v0_config_from_cloud(config: dict):
    """
    Tests to ensure we can build a cloud data context from both v0 and v1 configurations.

    NOTE: This includes some assertions, but we are also just checking that no exceptions
    are raised when instantiating the CloudDataContext, as would happen if we didn't
    properly map keys from the v0 configuration to the v1 configuration.
    """

    responses.add(
        responses.GET,
        CONTEXT_CONFIGURATION_URL,
        json=config,
        status=200,
    )

    CloudDataContext(
        cloud_base_url=CLOUD_BASE_URL,
        cloud_access_token=ACCESS_TOKEN,
        cloud_organization_id=ORG_ID,
    )

    # if we didn't raise when instantiating the context, we are good!
