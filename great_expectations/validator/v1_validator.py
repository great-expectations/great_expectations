from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any, Optional

from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.core.result_format import ResultFormat
from great_expectations.validator.validator import Validator as OldValidator
from great_expectations.validator.validator import calc_validation_statistics

if TYPE_CHECKING:
    from great_expectations.core import ExpectationSuite
    from great_expectations.core.batch_definition import BatchDefinition
    from great_expectations.datasource.fluent.batch_request import BatchParameters
    from great_expectations.expectations.expectation import (
        Expectation,
        ExpectationConfiguration,
    )


class Validator:
    """Validator.

    Responsible for running expectations on a batch definition.
    """

    def __init__(
        self,
        batch_definition: BatchDefinition,
        result_format: ResultFormat = ResultFormat.SUMMARY,
        batch_parameters: Optional[BatchParameters] = None,
    ) -> None:
        self._batch_definition = batch_definition
        self._batch_parameters = batch_parameters
        self.result_format = result_format

        from great_expectations import project_manager

        self._get_validator = project_manager.get_validator

    def validate_expectation(
        self,
        expectation: Expectation,
        suite_parameters: Optional[dict[str, Any]] = None,
    ) -> ExpectationValidationResult:
        """Run a single expectation against the batch definition"""
        results = self._validate_expectation_configs([expectation.configuration])

        assert len(results) == 1
        return results[0]

    def validate_expectation_suite(
        self,
        expectation_suite: ExpectationSuite,
        suite_parameters: Optional[dict[str, Any]] = None,
    ) -> ExpectationSuiteValidationResult:
        """Run an expectation suite against the batch definition"""
        results = self._validate_expectation_configs(
            expectation_suite.expectation_configurations,
            suite_parameters,
        )
        statistics = calc_validation_statistics(results)

        # TODO: This was copy/pasted from Validator, but many fields were removed
        return ExpectationSuiteValidationResult(
            results=results,
            success=statistics.success,
            suite_name=expectation_suite.name,
            statistics={
                "evaluated_expectations": statistics.evaluated_expectations,
                "successful_expectations": statistics.successful_expectations,
                "unsuccessful_expectations": statistics.unsuccessful_expectations,
                "success_percent": statistics.success_percent,
            },
            batch_id=self.active_batch_id,
        )

    @property
    def active_batch_id(self) -> Optional[str]:
        return self._wrapped_validator.active_batch_id

    @cached_property
    def _wrapped_validator(self) -> OldValidator:
        batch_request = self._batch_definition.build_batch_request(
            batch_parameters=self._batch_parameters
        )
        return self._get_validator(batch_request=batch_request)

    def _validate_expectation_configs(
        self,
        expectation_configs: list[ExpectationConfiguration],
        suite_parameters: Optional[dict[str, Any]] = None,
    ) -> list[ExpectationValidationResult]:
        """Run a list of expectation configurations against the batch definition"""
        processed_expectation_configs = self._wrapped_validator.process_expectations_for_validation(
            expectation_configs, suite_parameters
        )

        results = self._wrapped_validator.graph_validate(
            configurations=processed_expectation_configs,
            runtime_configuration={"result_format": self.result_format.value},
        )

        return results
