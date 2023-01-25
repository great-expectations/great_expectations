from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Dict, List, Optional, Type, Union

from typing_extensions import ClassVar, Literal

from great_expectations.alias_types import PathStr
from great_expectations.experimental.datasources.csv_data_asset import CsvDataAsset
from great_expectations.experimental.datasources.interfaces import (
    BatchSortersDefinition,
    DataAsset,
    Datasource,
)

if TYPE_CHECKING:
    from great_expectations.execution_engine import ExecutionEngine

LOGGER = logging.getLogger(__name__)


class PandasDatasourceError(Exception):
    pass


class PandasDatasource(Datasource):
    # class attrs
    asset_types: ClassVar[List[Type[DataAsset]]] = [CsvDataAsset]

    # instance attrs
    type: Literal["pandas"] = "pandas"
    name: str
    assets: Dict[str, CsvDataAsset] = {}

    @property
    def execution_engine_type(self) -> Type[ExecutionEngine]:
        """Return the PandasExecutionEngine unless the override is set"""
        from great_expectations.execution_engine.pandas_execution_engine import (
            PandasExecutionEngine,
        )

        return PandasExecutionEngine

    def add_csv_asset(
        self,
        name: str,
        base_directory: PathStr,
        regex: Union[str, re.Pattern],
        order_by: Optional[BatchSortersDefinition] = None,
    ) -> CsvDataAsset:
        """Adds a csv asset to the present Pandas datasource

        Args:
            name: The name of the csv asset
            base_directory: base directory path, relative to which file paths will be collected
            regex: regex pattern that matches CSV filenames and whose groups are used to label the Batch samples
            order_by: one of "asc" (ascending) or "desc" (descending) -- the method by which to sort "Asset" parts.
        """
        asset = CsvDataAsset(
            name=name,
            base_directory=base_directory,
            regex=regex,
            order_by=order_by,
        )
        return self.add_asset(asset=asset)
