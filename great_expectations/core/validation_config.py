from __future__ import annotations

from great_expectations._docs_decorators import public_api
from great_expectations.compatibility.pydantic import BaseModel, validator
from great_expectations.core.batch_config import BatchConfig  # noqa: TCH001
from great_expectations.core.expectation_suite import (
    ExpectationSuite,
    expectationSuiteSchema,
)


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

    @validator("suite", pre=True)
    def _validate_suite(cls, v):
        if isinstance(v, dict):
            return ExpectationSuite(**expectationSuiteSchema.load(v))
        return v

    @public_api
    def run(self):
        raise NotImplementedError


ValidationConfig.update_forward_refs()
