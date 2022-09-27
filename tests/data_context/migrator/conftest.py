import pytest


@pytest.fixture
def serialized_configuration_bundle() -> dict:
    return {
        "data_context_id": "877166bd-08f2-4d7b-b473-a2b97ab5e36f",
        "datasources": [
            {
                "class_name": "Datasource",
                "data_connectors": {
                    "my_default_data_connector": {
                        "assets": {
                            "": {
                                "base_directory": "data",
                                "class_name": "Asset",
                                "glob_directive": "*.csv",
                                "group_names": ["batch_num", "total_batches"],
                                "pattern": "csv_batch_(\\d.+)_of_(\\d.+)\\.csv",
                            }
                        },
                        "base_directory": "data",
                        "class_name": "ConfiguredAssetFilesystemDataConnector",
                    }
                },
                "execution_engine": {
                    "class_name": "PandasExecutionEngine",
                    "module_name": "great_expectations.execution_engine",
                },
                "name": "generic_csv_generator",
            }
        ],
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
