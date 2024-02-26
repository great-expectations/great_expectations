from __future__ import annotations

from typing import TYPE_CHECKING

from great_expectations._docs_decorators import public_api
from great_expectations.compatibility.pydantic import BaseModel

if TYPE_CHECKING:
    from great_expectations.core.batch_config import BatchConfig
    from great_expectations.core.expectation_suite import ExpectationSuite

    # from great_expectations.datasource.fluent.interfaces import DataAsset


class ValidationConfig(BaseModel):
    """
    Responsible for running a suite against data and returning a validation result.

    Args:
        name: The name of the validation.
        data: A batch config to validate.
        suite: A grouping of expectations to validate against the data.

    """

    name: str
    data: BatchConfig  # TODO: Should support a union of Asset | BatchConfig
    suite: ExpectationSuite

    @public_api
    def run(self):
        raise NotImplementedError
