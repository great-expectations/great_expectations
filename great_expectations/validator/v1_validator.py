from __future__ import annotations

import threading
from copy import copy
from typing import TYPE_CHECKING, Optional

from great_expectations import __version__ as ge_version
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.core.owner_resolution import owner_from_batch_definition
from great_expectations.core.result_format import (
    DEFAULT_RESULT_FORMAT,
    ResultFormat,
)
from great_expectations.data_context.data_context.context_factory import project_manager
from great_expectations.util import convert_to_json_serializable  # noqa: TID251 # FIXME CoP
from great_expectations.validator.validator import Validator as OldValidator
from great_expectations.validator.validator import calc_validation_statistics

if TYPE_CHECKING:
    from great_expectations.core import ExpectationSuite
    from great_expectations.core.batch_definition import BatchDefinition
    from great_expectations.core.result_format import ResultFormatUnion
    from great_expectations.core.suite_parameters import SuiteParameterDict
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
        result_format: ResultFormatUnion = DEFAULT_RESULT_FORMAT,
        batch_parameters: Optional[BatchParameters] = None,
    ) -> None:
        self._batch_definition = batch_definition
        self._batch_parameters = batch_parameters
        self.result_format = result_format

        # A batch definition that belongs to a Data Context builds its validator through that
        # context, so its data source is looked up among that context's own data sources.
        # One that belongs to no context builds it through the current one (GX issue #12209).
        owner = owner_from_batch_definition(batch_definition)
        self._get_validator = (
            owner.get_validator if owner is not None else project_manager.get_validator
        )
        self._wrapped_validator_cache: Optional[OldValidator] = None
        self._wrapped_validator_lock = threading.RLock()

    def validate_expectation(
        self,
        expectation: Expectation,
        expectation_parameters: Optional[SuiteParameterDict] = None,
    ) -> ExpectationValidationResult:
        """Run a single expectation against the batch definition"""
        results = self._validate_expectation_configs(
            expectation_configs=[expectation.configuration],
            expectation_parameters=expectation_parameters,
        )

        assert len(results) == 1
        return results[0]

    def validate_expectation_suite(
        self,
        expectation_suite: ExpectationSuite,
        expectation_parameters: Optional[SuiteParameterDict] = None,
    ) -> ExpectationSuiteValidationResult:
        """Run an expectation suite against the batch definition"""
        results = self._validate_expectation_configs(
            expectation_configs=expectation_suite.expectation_configurations,
            expectation_parameters=expectation_parameters,
        )
        statistics = calc_validation_statistics(results)

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
            meta={
                # run_id, validation_time, and fkeys are added to this dict
                # in ValidationDefinition.run
                "great_expectations_version": ge_version,
                "batch_spec": convert_to_json_serializable(
                    self._wrapped_validator.active_batch_spec
                ),
                "batch_markers": self._wrapped_validator.active_batch_markers,
                "active_batch_definition": convert_to_json_serializable(
                    self._wrapped_validator.active_batch_definition
                ),
            },
            batch_id=self.active_batch_id,
        )

    @property
    def active_batch_id(self) -> Optional[str]:
        return self._wrapped_validator.active_batch_id

    @property
    def _include_rendered_content(self) -> bool:
        return project_manager.is_using_cloud()

    @property
    def _wrapped_validator(self) -> OldValidator:
        """Build the wrapped Validator on first access and reuse it afterwards.

        This is deliberately not a `functools.cached_property`. On Python 3.10 and 3.11 that
        descriptor holds one `RLock` per property per class and computes inside it, so building
        two Validators on two threads serializes even when the two share nothing, and a thread
        that blocks inside the build holds the lock every other Validator needs to start its
        own. CPython removed the lock in 3.12 (python/cpython#87634). Locking per instance
        keeps concurrent builds independent while still computing once per instance.
        """
        with self._wrapped_validator_lock:
            if self._wrapped_validator_cache is None:
                batch_request = self._batch_definition.build_batch_request(
                    batch_parameters=self._batch_parameters
                )
                self._wrapped_validator_cache = self._get_validator(batch_request=batch_request)
            return self._wrapped_validator_cache

    def _validate_expectation_configs(
        self,
        expectation_configs: list[ExpectationConfiguration],
        expectation_parameters: Optional[SuiteParameterDict] = None,
    ) -> list[ExpectationValidationResult]:
        """Run a list of expectation configurations against the batch definition"""
        processed_expectation_configs = self._wrapped_validator.process_expectations_for_validation(
            expectation_configs, expectation_parameters
        )

        runtime_configuration: dict
        if isinstance(self.result_format, ResultFormat):
            runtime_configuration = {"result_format": copy(self.result_format.value)}
        else:
            runtime_configuration = {"result_format": copy(self.result_format)}

        results = self._wrapped_validator.graph_validate(
            configurations=processed_expectation_configs,
            runtime_configuration=runtime_configuration,
        )

        if self._include_rendered_content:
            for result in results:
                result.render()

        return results
