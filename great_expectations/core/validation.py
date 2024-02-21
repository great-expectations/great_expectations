from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from great_expectations.core.batch_config import BatchConfig
    from great_expectations.core.expectation_suite import ExpectationSuite
    from great_expectations.datasource.fluent.interfaces import DataAsset


class Validation:
    """
    Responsible for running a suite against data and returning a validation result.

    Args:
        name: The name of the validation.
        data: An asset or batch config to validate.
        suite: A grouping of expectations to validate against the data.

    """

    def __init__(
        self,
        name: str,
        data: DataAsset | BatchConfig,
        suite: ExpectationSuite,
    ) -> None:
        self._name = name
        self._data = data
        self._suite = suite

    @property
    def name(self) -> str:
        return self._name

    @property
    def data(self) -> DataAsset | BatchConfig:
        return self._data

    @property
    def suite(self) -> ExpectationSuite:
        return self._suite

    def run(self):
        raise NotImplementedError
