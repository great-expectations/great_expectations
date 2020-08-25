import pytest

from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig


@pytest.fixture(scope="module")
def basic_data_context_config_for_validation_operator():
    return DataContextConfig(
        config_version=2,
        plugins_directory=None,
        evaluation_parameter_store_name="evaluation_parameter_store",
        expectations_store_name="expectations_store",
        datasources={},
        stores={
            "expectations_store": {"class_name": "ExpectationsStore"},
            "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
            "validation_result_store": {"class_name": "ValidationsStore"},
            "metrics_store": {"class_name": "MetricStore"},
        },
        validations_store_name="validation_result_store",
        data_docs_sites={},
        validation_operators={
            "store_val_res_and_extract_eval_params": {
                "class_name": "ActionListValidationOperator",
                "action_list": [
                    {
                        "name": "store_validation_result",
                        "action": {
                            "class_name": "StoreValidationResultAction",
                            "target_store_name": "validation_result_store",
                        },
                    },
                    {
                        "name": "extract_and_store_eval_parameters",
                        "action": {
                            "class_name": "StoreEvaluationParametersAction",
                            "target_store_name": "evaluation_parameter_store",
                        },
                    },
                ],
            },
            "errors_and_warnings_validation_operator": {
                "class_name": "WarningAndFailureExpectationSuitesValidationOperator",
                "action_list": [
                    {
                        "name": "store_validation_result",
                        "action": {
                            "class_name": "StoreValidationResultAction",
                            "target_store_name": "validation_result_store",
                        },
                    },
                    {
                        "name": "extract_and_store_eval_parameters",
                        "action": {
                            "class_name": "StoreEvaluationParametersAction",
                            "target_store_name": "evaluation_parameter_store",
                        },
                    },
                ],
            },
        },
    )


@pytest.fixture(scope="module")
def basic_in_memory_data_context_for_validation_operator(
    basic_data_context_config_for_validation_operator,
):
    return BaseDataContext(basic_data_context_config_for_validation_operator)
