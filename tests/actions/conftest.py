import pytest

from great_expectations.data_context.types.base import DataContextConfig


@pytest.fixture
def basic_data_context_config_for_validation_operator():
    return DataContextConfig(
        config_version=1,
        plugins_directory="plugins/",
        evaluation_parameter_store_name="evaluation_parameter_store",
        expectations_store_name="expectations_store",
        datasources={},
        stores={
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "FixedLengthTupleFilesystemStoreBackend",
                    "base_directory": "expectations/",
                }
            },
            "evaluation_parameter_store": {
                "module_name": "great_expectations.data_context.store",
                "class_name": "InMemoryEvaluationParameterStore",
            },
            "validation_result_store": {
                "module_name": "great_expectations.data_context.store",
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "InMemoryStoreBackend",
                }
            }
        },
        validations_store_name="validation_result_store",
        data_docs_sites={},
        validation_operators={
            "store_val_res_and_extract_eval_params": {
                "class_name": "ActionListValidationOperator",
                "action_list": [{
                    "name": "store_validation_result",
                    "action": {
                        "class_name": "StoreAction",
                        "target_store_name": "validation_result_store",
                    }
                },
                {
                    "name": "extract_and_store_eval_parameters",
                    "action": {
                        "class_name": "ExtractAndStoreEvaluationParamsAction",
                        "target_store_name": "evaluation_parameter_store",
                    }
                }]
            }
        }
    )
