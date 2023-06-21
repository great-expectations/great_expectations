import os
from typing import Tuple

from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context.types.refs import GXCloudResourceRef


def make_retrieve_data_context_config_from_cloud(data_dir):
    def retrieve_data_context_config_from_cloud(cls, *args, **kwargs):
        return DataContextConfig(**_cloud_config(data_dir))

    return retrieve_data_context_config_from_cloud


def make_store_get(data_file_name, with_slack):
    def store_get(self, key):
        # key is a 3-tuple with the form
        # (GXCloudRESTResource, cloud_id as a string uuid, name as string)
        # For example:
        # (<GXCloudRESTResource.CHECKPOINT: 'checkpoint'>, '731dc2a5-45d8-4827-9118-39b77c5cd413', 'my_checkpoint')
        if key[0] == GXCloudRESTResource.CHECKPOINT:
            return {"data": _checkpoint_config(data_file_name, with_slack)}
        elif key[0] == GXCloudRESTResource.EXPECTATION_SUITE:
            return {"data": _expectation_suite()}
        return None

    return store_get


def store_set(self, key, value, **kwargs):
    base_url = os.environ["GX_CLOUD_BASE_URL"]
    org_id = os.environ["GX_CLOUD_ORGANIZATION_ID"]
    if not base_url.endswith("/"):
        base_url += "/"
    url = base_url + "organizations/" + org_id + "/validation-results"
    # This is incomplete but has the necessary info for the current tests.
    validation_result_json = {
        "data": {
            "attributes": {
                "created_by_id": "934e0898-6a5c-4ffd-9125-89381a46d191",
                "organization_id": org_id,
                "validation_result": {
                    "display_url": f"{base_url}{org_id}/?validationResultId=2e13ecc3-eaaa-444b-b30d-2f616f80ae35",
                },
            }
        }
    }
    return GXCloudResourceRef(
        resource_type=GXCloudRESTResource.VALIDATION_RESULT,
        id="2e13ecc3-eaaa-444b-b30d-2f616f80ae35",
        url=url,
        response_json=validation_result_json,
    )


def list_keys(self, prefix: Tuple = ()):
    if self.ge_cloud_resource_type == GXCloudRESTResource.EXPECTATION_SUITE:
        return [
            (
                GXCloudRESTResource.EXPECTATION_SUITE,
                "1212e79d-f751-4c6e-921d-26de2b1db174",
                "suite_name",
            ),
        ]


class CallCounter:
    def __init__(self):
        self.count = 0

    def increment(self):
        self.count += 1


def make_send_slack_notifications(counter: CallCounter):
    def send_slack_notification(*args, **kwargs):
        counter.increment()

    return send_slack_notification


def _cloud_config(data_dir):
    return {
        "anonymous_usage_statistics": {
            "data_context_id": "6a52bdfa-e182-455b-a825-e69f076e67d6",
            "enabled": True,
        },
        "checkpoint_store_name": "default_checkpoint_store",
        "concurrency": {"enabled": False},
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
                        "base_directory": str(data_dir),
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
        "evaluation_parameter_store_name": "evaluation_parameter_store",
        "expectations_store_name": "default_expectations_store",
        "include_rendered_content": {
            "expectation_suite": True,
            "expectation_validation_result": True,
            "globally": True,
        },
        "notebooks": None,
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
                    "ge_cloud_base_url": os.environ["GX_CLOUD_BASE_URL"],
                    "ge_cloud_credentials": {
                        "access_token": os.environ["GX_CLOUD_ACCESS_TOKEN"],
                        "organization_id": os.environ["GX_CLOUD_ORGANIZATION_ID"],
                    },
                    "ge_cloud_resource_type": "checkpoint",
                    "suppress_store_backend_id": True,
                },
            },
            "default_evaluation_parameter_store": {
                "class_name": "EvaluationParameterStore"
            },
            "default_expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "GXCloudStoreBackend",
                    "ge_cloud_base_url": os.environ["GX_CLOUD_BASE_URL"],
                    "ge_cloud_credentials": {
                        "access_token": os.environ["GX_CLOUD_ACCESS_TOKEN"],
                        "organization_id": os.environ["GX_CLOUD_ORGANIZATION_ID"],
                    },
                    "ge_cloud_resource_type": "expectation_suite",
                    "suppress_store_backend_id": True,
                },
            },
            "default_profiler_store": {
                "class_name": "ProfilerStore",
                "store_backend": {
                    "class_name": "GXCloudStoreBackend",
                    "ge_cloud_base_url": os.environ["GX_CLOUD_BASE_URL"],
                    "ge_cloud_credentials": {
                        "access_token": os.environ["GX_CLOUD_ACCESS_TOKEN"],
                        "organization_id": os.environ["GX_CLOUD_ORGANIZATION_ID"],
                    },
                    "ge_cloud_resource_name": "profilers",
                    "ge_cloud_resource_type": "profiler",
                    "suppress_store_backend_id": True,
                },
            },
            "default_validations_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "GXCloudStoreBackend",
                    "ge_cloud_base_url": os.environ["GX_CLOUD_BASE_URL"],
                    "ge_cloud_credentials": {
                        "access_token": os.environ["GX_CLOUD_ACCESS_TOKEN"],
                        "organization_id": os.environ["GX_CLOUD_ORGANIZATION_ID"],
                    },
                    "ge_cloud_resource_type": "validation_result",
                    "suppress_store_backend_id": True,
                },
            },
            "evaluation_parameter_store": {
                "class_name": "EvaluationParameterStore",
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
        "validations_store_name": "default_validations_store",
    }


def _checkpoint_config(data_file_name, with_slack):
    action_list = [
        {
            "action": {"class_name": "StoreValidationResultAction"},
            "name": "store_validation_result",
        },
        {
            "action": {"class_name": "StoreEvaluationParametersAction"},
            "name": "store_evaluation_params",
        },
    ]
    if with_slack:
        action_list.append(
            {
                "name": "send_slack_notification_on_validation_result",
                "action": {
                    "class_name": "SlackNotificationAction",
                    "slack_webhook": "https://hooks.slack.com/services/11111111111/22222222222/333333333333333333333333",
                    "notify_on": "all",
                    "renderer": {
                        "module_name": "great_expectations.render.renderer.slack_renderer",
                        "class_name": "SlackRenderer",
                    },
                },
            },
        )

    return [
        {
            "attributes": {
                "checkpoint_config": {
                    "action_list": action_list,
                    "batch_request": {},
                    "class_name": "Checkpoint",
                    "config_version": 1.0,
                    "default_validation_id": "51c303bd-2396-4f04-b567-79f746b09173",
                    "evaluation_parameters": {},
                    "expectation_suite_ge_cloud_id": None,
                    "expectation_suite_name": None,
                    "ge_cloud_id": None,
                    "id": "731dc2a5-45d8-4827-9118-39b77c5cd413",
                    "module_name": "great_expectations.checkpoint",
                    "name": "my_checkpoint",
                    "profilers": [],
                    "run_name_template": None,
                    "runtime_configuration": {},
                    "template_name": None,
                    "validations": [
                        {
                            "batch_request": {
                                "data_asset_name": data_file_name,
                                "data_connector_name": "taxi_data_connector",
                                "data_connector_query": {"index": -1},
                                "datasource_name": "taxi_datasource",
                            },
                            "expectation_suite_ge_cloud_id": "d1ff9854-ac5f-45ae-9c88-c3f4323432c1",
                            "expectation_suite_name": "taxi_demo_suite",
                            "id": "2e13ecc3-eaaa-444b-b30d-2f616f80ae35",
                        }
                    ],
                },
                "class_name": "Checkpoint",
                "created_by_id": "934e0898-6a5c-4ffd-9125-89381a46d191",
                "default_validation_id": "51c303bd-2396-4f04-b567-79f746b09173",
                "id": "731dc2a5-45d8-4827-9118-39b77c5cd413",
                "name": "my_checkpoint",
                "organization_id": os.environ["GX_CLOUD_ORGANIZATION_ID"],
            },
            "id": "731dc2a5-45d8-4827-9118-39b77c5cd413",
            "type": "checkpoint",
        }
    ]


def _expectation_suite():
    return [
        {
            "attributes": {
                "created_by_id": "934e0898-6a5c-4ffd-9125-89381a46d191",
                "organization_id": os.environ["GX_CLOUD_ORGANIZATION_ID"],
                "suite": {
                    "data_asset_type": None,
                    "expectation_suite_name": "single-snippet-suite-2",
                    "expectations": [
                        {
                            "expectation_type": "expect_column_to_exist",
                            "ge_cloud_id": "0571b111-a1d0-4f1a-9c83-6704887de635",
                            "kwargs": {"column": "passenger_count"},
                            "meta": {},
                        }
                    ],
                    "ge_cloud_id": "1212e79d-f751-4c6e-921d-26de2b1db174",
                    "meta": {"great_expectations_version": "0.15.43"},
                },
            },
            "id": "1212e79d-f751-4c6e-921d-26de2b1db174",
            "type": "expectation_suite",
        }
    ]
