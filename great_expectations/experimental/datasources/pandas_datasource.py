from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Dict, List, Optional, Type, Union

from typing_extensions import ClassVar, Literal

from great_expectations.alias_types import PathStr
from great_expectations.experimental.datasources.dynamic_pandas import (
    _generate_pandas_data_asset_models,
)
from great_expectations.experimental.datasources.filesystem_data_asset import (
    _FilesystemDataAsset,
)
from great_expectations.experimental.datasources.interfaces import (
    BatchSortersDefinition,
    DataAsset,
    Datasource,
)

if TYPE_CHECKING:
    from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


class PandasDatasourceError(Exception):
    pass


_ASSET_MODELS = _generate_pandas_data_asset_models(
    _FilesystemDataAsset,
    whitelist=(
        "read_csv",
        "read_json",
        "read_excel",
        "read_parquet",
    ),
)


CSVAsset = _ASSET_MODELS["csv"]
ExcelAsset = _ASSET_MODELS["excel"]
JSONAsset = _ASSET_MODELS["json"]
ParquetAsset = _ASSET_MODELS["parquet"]


def _build_filesystem_data_asset_class(
    superclass: Type[_FilesystemDataAsset],
) -> Type[_FilesystemDataAsset]:
    def _get_reader_method(self) -> str:
        return f"read_{self.type}"

    def _get_reader_options_include(self) -> set[str] | None:
        return None

    setattr(superclass, "_get_reader_method", _get_reader_method)
    setattr(superclass, "_get_reader_options_include", _get_reader_options_include)

    return superclass


CSVPandasAsset = _build_filesystem_data_asset_class(CSVAsset)
ExcelPandasAsset = _build_filesystem_data_asset_class(ExcelAsset)
JSONPandasAsset = _build_filesystem_data_asset_class(JSONAsset)
ParquetPandasAsset = _build_filesystem_data_asset_class(ParquetAsset)


class PandasDatasource(Datasource):
    # class attributes
    asset_types: ClassVar[List[Type[DataAsset]]] = [
        CSVPandasAsset,
        ExcelPandasAsset,
        JSONPandasAsset,
        ParquetPandasAsset,
    ]

    # instance attributes
    type: Literal["pandas"] = "pandas"
    name: str
    assets: Dict[  # type: ignore[valid-type]
        str,
        Union[
            _FilesystemDataAsset,
            CSVPandasAsset,
            ExcelPandasAsset,
            ParquetPandasAsset,
            JSONPandasAsset,
        ],
    ] = {}

    @property
    def execution_engine_type(self) -> Type[ExecutionEngine]:
        """Return the PandasExecutionEngine unless the override is set"""
        from great_expectations.execution_engine.pandas_execution_engine import (
            PandasExecutionEngine,
        )

        return PandasExecutionEngine

    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the PandasDatasource.

        Args:
            test_assets: If assets have been passed to the PandasDatasource, whether to test them as well.

        Raises:
            TestConnectionError
        """
        # Only self.assets can be tested for PandasDatasource
        if self.assets and test_assets:
            for asset in self.assets.values():
                asset.test_connection()  # type: ignore[union-attr]

    def add_csv_asset(
        self,
        name: str,
        base_directory: PathStr,
        regex: Union[str, re.Pattern],
        order_by: Optional[BatchSortersDefinition] = None,
        **kwargs,  # TODO: update signature to have specific keys & types
    ) -> CSVPandasAsset:  # type: ignore[valid-type]
        """Adds a CSV DataAsst to the present "PandasDatasource" object.

        Args:
            name: The name of the csv asset
            base_directory: base directory path, relative to which CSV file paths will be collected
            regex: regex pattern that matches csv filenames that is used to label the batches
            order_by: sorting directive via either List[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_csv`` keyword args
        """
        asset = CSVPandasAsset(
            name=name,
            base_directory=base_directory,  # type: ignore[arg-type]  # str will be coerced to Path
            regex=regex,  # type: ignore[arg-type]  # str with will coerced to Pattern
            order_by=order_by or [],  # type: ignore[arg-type]  # coerce list[str]
            **kwargs,
        )
        return self.add_asset(asset)

    def add_excel_asset(
        self,
        name: str,
        base_directory: PathStr,
        regex: Union[str, re.Pattern],
        order_by: Optional[BatchSortersDefinition] = None,
        **kwargs,  # TODO: update signature to have specific keys & types
    ) -> ExcelPandasAsset:  # type: ignore[valid-type]
        """Adds an Excel DataAsst to the present "PandasDatasource" object.

        Args:
            name: The name of the csv asset
            base_directory: base directory path, relative to which CSV file paths will be collected
            regex: regex pattern that matches csv filenames that is used to label the batches
            order_by: sorting directive via either List[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_excel`` keyword args
        """
        asset = ExcelPandasAsset(
            name=name,
            base_directory=base_directory,  # type: ignore[arg-type]  # str will be coerced to Path
            regex=regex,  # type: ignore[arg-type]  # str with will coerced to Pattern
            order_by=order_by or [],  # type: ignore[arg-type]  # coerce list[str]
            **kwargs,
        )
        return self.add_asset(asset)

    def add_json_asset(
        self,
        name: str,
        base_directory: PathStr,
        regex: Union[str, re.Pattern],
        order_by: Optional[BatchSortersDefinition] = None,
        **kwargs,  # TODO: update signature to have specific keys & types
    ) -> JSONPandasAsset:  # type: ignore[valid-type]
        """Adds a JSON DataAsst to the present "PandasDatasource" object.

        Args:
            name: The name of the csv asset
            base_directory: base directory path, relative to which CSV file paths will be collected
            regex: regex pattern that matches csv filenames that is used to label the batches
            order_by: sorting directive via either List[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_json`` keyword args
        """
        asset = JSONPandasAsset(
            name=name,
            base_directory=base_directory,  # type: ignore[arg-type]  # str will be coerced to Path
            regex=regex,  # type: ignore[arg-type]  # str with will coerced to Pattern
            order_by=order_by or [],  # type: ignore[arg-type]  # coerce list[str]
            **kwargs,
        )
        return self.add_asset(asset)

    def add_parquet_asset(
        self,
        name: str,
        base_directory: PathStr,
        regex: Union[str, re.Pattern],
        order_by: Optional[BatchSortersDefinition] = None,
        **kwargs,  # TODO: update signature to have specific keys & types
    ) -> ParquetPandasAsset:  # type: ignore[valid-type]
        """dds a Parquet DataAsst to the present "PandasDatasource" object.

        Args:
            name: The name of the csv asset
            base_directory: base directory path, relative to which CSV file paths will be collected
            regex: regex pattern that matches csv filenames that is used to label the batches
            order_by: sorting directive via either List[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_parquet`` keyword args
        """
        asset = ParquetPandasAsset(
            name=name,
            base_directory=base_directory,  # type: ignore[arg-type]  # str will be coerced to Path
            regex=regex,  # type: ignore[arg-type]  # str with will coerced to Pattern
            order_by=order_by or [],  # type: ignore[arg-type]  # coerce list[str]
            **kwargs,
        )
        return self.add_asset(asset)
