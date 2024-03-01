from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Union

import great_expectations as gx
from great_expectations._docs_decorators import public_api
from great_expectations.compatibility.pydantic import BaseModel, validator
from great_expectations.core.batch_config import BatchConfig  # noqa: TCH001
from great_expectations.core.expectation_suite import (
    ExpectationSuite,
    expectationSuiteSchema,
)
from great_expectations.validator.v1_validator import ResultFormat, Validator

if TYPE_CHECKING:
    from great_expectations.core.expectation_validation_result import (
        ExpectationSuiteValidationResult,
    )
    from great_expectations.datasource.fluent.batch_request import BatchRequestOptions


class ValidationConfig(BaseModel):
    """
    Responsible for running a suite against data and returning a validation result.

    Args:
        name: The name of the validation.
        data: A batch config to validate.
        suite: A grouping of expectations to validate against the data.

    """

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            ExpectationSuite: lambda v: expectationSuiteSchema.dump(v),
        }

    name: str
    data: BatchConfig  # TODO: Should support a union of Asset | BatchConfig
    suite: ExpectationSuite
    id: Union[str, None] = None

    @validator("suite", pre=True)
    def _validate_suite(cls, v):
        if isinstance(v, dict):
            return ExpectationSuite(**expectationSuiteSchema.load(v))
        elif isinstance(v, ExpectationSuite):
            return v
        raise ValueError(
            "Suite must be a dictionary (if being deserialized) or an ExpectationSuite object."
        )

    @public_api
    def run(
        self,
        *,
        batch_config_options: Optional[BatchRequestOptions] = None,
        evaluation_parameters: Optional[dict[str, Any]] = None,
        result_format: ResultFormat = ResultFormat.SUMMARY,
    ) -> ExpectationSuiteValidationResult:
        context = gx.get_context()
        validator = Validator(
            context=context,
            batch_config=self.data,
            batch_request_options=batch_config_options,
            result_format=result_format,
        )
        return validator.validate_expectation_suite(self.suite, evaluation_parameters)
