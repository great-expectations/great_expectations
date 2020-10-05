import pytest

from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)


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

@pytest.fixture(scope="module")
def validation_result_suite():
    return ExpectationSuiteValidationResult(
        results=[],
        success=True,
        statistics={
            "evaluated_expectations": 0,
            "successful_expectations": 0,
            "unsuccessful_expectations": 0,
            "success_percent": None,
        },
        meta={
            "great_expectations_version": "v0.8.0__develop",
            "expectation_suite_name": "asset.default",
            "run_id": "test_100",
        },
    )

@pytest.fixture(scope="module")
def validation_result_suite_id():
    return ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier("asset.default"),
        run_id="test_100",
        batch_identifier="1234",
    )
