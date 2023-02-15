from __future__ import annotations

import logging
import pathlib
import re
from typing import TYPE_CHECKING, ClassVar, Dict, List, Optional, Type, Union

from typing_extensions import Literal

from great_expectations.experimental.datasources.filesystem_data_asset import (
    _FilesystemDataAsset,
)
from great_expectations.experimental.datasources.interfaces import (
    BatchSortersDefinition,
    DataAsset,
    Datasource,
    TestConnectionError,
    _batch_sorter_from_list,
)

if TYPE_CHECKING:
    from great_expectations.execution_engine import SparkDFExecutionEngine

logger = logging.getLogger(__name__)


class SparkDatasourceError(Exception):
    pass


class CSVSparkAsset(_FilesystemDataAsset):
    # Overridden inherited instance fields
    type: Literal["csv_spark"] = "csv_spark"

    # Spark Filesystem specific attributes
    header: bool = False
    inferSchema: bool = False

    def _get_reader_method(self) -> str:
        return f"{self.type[0:-6]}"

    def _get_reader_options_include(self) -> set[str] | None:
        return {"header", "inferSchema"}


class _SparkDatasource(Datasource):
    # class attributes
    asset_types: ClassVar[List[Type[DataAsset]]] = [CSVSparkAsset]

    # instance attributes
    assets: Dict[
        str,
        CSVSparkAsset,
    ] = {}

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


class SparkDatasource(_SparkDatasource):
    # instance attributes
    type: Literal["spark"] = "spark"
    name: str
    base_directory: pathlib.Path
    assets: Dict[str, CSVSparkAsset] = {}

    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the SparkDatasource.

        Args:
            test_assets: If assets have been passed to the SparkDatasource, whether to test them as well.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        if not self.base_directory.exists():
            raise TestConnectionError(
                f"Path: {self.base_directory.resolve()} does not exist."
            )

        if self.assets and test_assets:
            for asset in self.assets.values():
                asset.test_connection()

    def add_csv_asset(
        self,
        name: str,
        regex: Union[str, re.Pattern],
        header: bool = False,
        infer_schema: bool = False,
        order_by: Optional[BatchSortersDefinition] = None,
    ) -> CSVSparkAsset:
        """Adds a csv asset to this Spark datasource

        Args:
            name: The name of the csv asset
            regex: regex pattern that matches csv filenames that is used to label the batches
            header: boolean (default False) indicating whether or not first line of CSV file is header line
            infer_schema: boolean (default False) instructing Spark to attempt to infer schema of CSV file heuristically
            order_by: sorting directive via either list[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
        """
        if isinstance(regex, str):
            regex = re.compile(regex)
        asset = CSVSparkAsset(
            name=name,
            regex=regex,
            header=header,
            inferSchema=infer_schema,
            order_by=_batch_sorter_from_list(order_by or []),
        )
        return self.add_asset(asset)
