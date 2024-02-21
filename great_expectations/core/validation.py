from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from great_expectations.core.batch_config import BatchConfig
    from great_expectations.core.expectation_suite import ExpectationSuite
    from great_expectations.datasource.fluent.interfaces import DataAsset


class Validation:
    """
    TBD
    """

    def __init__(
        self,
        name: str,
        data: DataAsset | BatchConfig,
        expectation_suite: ExpectationSuite,
    ) -> None:
        self._name = name
        self._data = data
        self._suite = expectation_suite

    @property
    def name(self) -> str:
        return self._name

    @property
    def data(self) -> BatchConfig:
        return self._data

    @property
    def suite(self) -> ExpectationSuite:
        return self._suite

    def run(self):
        raise NotImplementedError
