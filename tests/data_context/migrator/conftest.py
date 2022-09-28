import pytest


@pytest.fixture
def serialized_configuration_bundle() -> dict:
    return {
        "data_context_id": "877166bd-08f2-4d7b-b473-a2b97ab5e36f",
        "checkpoints": [
            {
                "class_name": "Checkpoint",
                "config_version": None,
                "module_name": "great_expectations.checkpoint",
                "name": "my_checkpoint",
            }
        ],
        "data_context_variables": {
            "config_variables_file_path": None,
            "config_version": 3.0,
            "data_docs_sites": None,
            "evaluation_parameter_store_name": None,
            "expectations_store_name": None,
            "include_rendered_content": {
                "expectation_suite": False,
                "expectation_validation_result": False,
                "globally": False,
            },
            "notebooks": None,
            "plugins_directory": None,
            "stores": None,
            "validations_store_name": None,
        },
        "datasources": [
            {
                "class_name": "Datasource",
                "data_connectors": {},
                "execution_engine": {
                    "class_name": "PandasExecutionEngine",
                    "module_name": "great_expectations.execution_engine",
                },
                "module_name": "great_expectations.datasource",
                "name": "my_datasource",
            }
        ],
        "expectation_suites": [
            {
                "data_asset_type": None,
                "expectation_suite_name": "my_suite",
                "expectations": [],
                "ge_cloud_id": None,
            }
        ],
        "profilers": [
            {
                "class_name": "RuleBasedProfiler",
                "config_version": 1.0,
                "module_name": "great_expectations.rule_based_profiler",
                "name": "my_profiler",
                "rules": {},
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


@pytest.fixture
def empty_serialized_configuration_bundle() -> dict:
    return {
        "data_context_id": "27517569-1500-4127-af68-b5bad960a492",
        "checkpoints": [],
        "data_context_variables": {
            "config_variables_file_path": None,
            "config_version": 3.0,
            "data_docs_sites": None,
            "evaluation_parameter_store_name": None,
            "expectations_store_name": None,
            "include_rendered_content": {
                "expectation_suite": False,
                "expectation_validation_result": False,
                "globally": False,
            },
            "notebooks": None,
            "plugins_directory": None,
            "stores": None,
            "validations_store_name": None,
        },
        "datasources": [],
        "expectation_suites": [],
        "profilers": [],
        "validation_results": [],
    }
