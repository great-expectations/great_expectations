from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Type

from great_expectations.datasource.fluent.interfaces import Datasource

if TYPE_CHECKING:
    from great_expectations.execution_engine import SparkDFExecutionEngine

logger = logging.getLogger(__name__)


class SparkDatasourceError(Exception):
    pass


class _SparkDatasource(Datasource):
    # Abstract Methods
    @property
    def execution_engine_type(self) -> Type[SparkDFExecutionEngine]:
        """Return the SparkDFExecutionEngine unless the override is set"""
        from great_expectations.execution_engine.sparkdf_execution_engine import (
            SparkDFExecutionEngine,
        )

        return SparkDFExecutionEngine

    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the _SparkDatasource.

        Args:
            test_assets: If assets have been passed to the _SparkDatasource,
                         an attempt can be made to test them as well.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        raise NotImplementedError(
            """One needs to implement "test_connection" on a _SparkDatasource subclass."""
        )

    # End Abstract Methods
