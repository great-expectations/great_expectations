from __future__ import annotations

from typing import TYPE_CHECKING

from great_expectations.compatibility.pydantic import BaseModel

if TYPE_CHECKING:
    from great_expectations.core.batch_config import BatchConfig
    from great_expectations.core.expectation_suite import ExpectationSuite
    from great_expectations.datasource.fluent.interfaces import DataAsset


class Validation(BaseModel):
    """
    Responsible for running a suite against data and returning a validation result.

    Args:
        name: The name of the validation.
        data: An asset or batch config to validate.
        suite: A grouping of expectations to validate against the data.

    """

    name: str
    data: DataAsset | BatchConfig
    suite: ExpectationSuite

    def run(self):
        raise NotImplementedError
