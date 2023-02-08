from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, ClassVar, Dict, List, Optional, Type, Union

from typing_extensions import Literal

from great_expectations.alias_types import PathStr  # noqa: TCH001
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
    from great_expectations.execution_engine import PandasExecutionEngine

logger = logging.getLogger(__name__)


class PandasDatasourceError(Exception):
    pass


_BLACK_LIST = (
    # "read_csv",
    # "read_json",
    # "read_excel",
    # "read_parquet",
    # "read_clipboard",
    # "read_feather",
    "read_fwf",  # unhandled type
    # "read_gbq",
    # "read_hdf",
    # "read_html",
    # "read_orc",
    # "read_pickle",
    # "read_sas",  # invalid json schema
    # "read_spss",
    "read_sql",  # type-name conflict
    # "read_sql_query",
    # "read_sql_table",
    "read_table",  # type-name conflict
    # "read_xml",
)

_ASSET_MODELS = _generate_pandas_data_asset_models(
    _FilesystemDataAsset,
    blacklist=_BLACK_LIST,
    use_docstring_from_method=True,
)
try:
    # variables only needed for type-hinting
    CSVAsset = _ASSET_MODELS["csv"]
    ExcelAsset = _ASSET_MODELS["excel"]
    JSONAsset = _ASSET_MODELS["json"]
    ORCAsset = _ASSET_MODELS["orc"]
    ParquetAsset = _ASSET_MODELS["parquet"]
except KeyError as key_err:
    logger.info(f"zep - {key_err} asset model could not be generated")
    CSVAsset = _FilesystemDataAsset
    ExcelAsset = _FilesystemDataAsset
    JSONAsset = _FilesystemDataAsset
    ORCAsset = _FilesystemDataAsset
    ParquetAsset = _FilesystemDataAsset


class PandasDatasource(Datasource):
    # class attributes
    asset_types: ClassVar[List[Type[DataAsset]]] = list(_ASSET_MODELS.values())

    # instance attributes
    type: Literal["pandas"] = "pandas"
    name: str
    assets: Dict[
        str,
        _FilesystemDataAsset,
    ] = {}

    @property
    def execution_engine_type(self) -> Type[PandasExecutionEngine]:
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
                asset.test_connection()

    def add_csv_asset(
        self,
        name: str,
        base_directory: PathStr,
        regex: Union[str, re.Pattern],
        order_by: Optional[BatchSortersDefinition] = None,
        **kwargs,  # TODO: update signature to have specific keys & types
    ) -> CSVAsset:  # type: ignore[valid-type]
        """Adds a CSV DataAsst to the present "PandasDatasource" object.

        Args:
            name: The name of the csv asset
            base_directory: base directory path, relative to which CSV file paths will be collected
            regex: regex pattern that matches csv filenames that is used to label the batches
            order_by: sorting directive via either List[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_csv`` keyword args
        """
        asset = CSVAsset(
            name=name,
            base_directory=base_directory,  # type: ignore[arg-type]
            regex=regex,  # type: ignore[arg-type]  # type: ignore[arg-type]
            order_by=order_by or [],  # type: ignore[arg-type]
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
    ) -> ExcelAsset:  # type: ignore[valid-type]
        """Adds an Excel DataAsst to the present "PandasDatasource" object.

        Args:
            name: The name of the csv asset
            base_directory: base directory path, relative to which CSV file paths will be collected
            regex: regex pattern that matches csv filenames that is used to label the batches
            order_by: sorting directive via either List[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_excel`` keyword args
        """
        asset = ExcelAsset(
            name=name,
            base_directory=base_directory,  # type: ignore[arg-type]
            regex=regex,  # type: ignore[arg-type]
            order_by=order_by or [],  # type: ignore[arg-type]
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
    ) -> JSONAsset:  # type: ignore[valid-type]
        """Adds a JSON DataAsst to the present "PandasDatasource" object.

        Args:
            name: The name of the csv asset
            base_directory: base directory path, relative to which CSV file paths will be collected
            regex: regex pattern that matches csv filenames that is used to label the batches
            order_by: sorting directive via either List[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_json`` keyword args
        """
        asset = JSONAsset(
            name=name,
            base_directory=base_directory,  # type: ignore[arg-type]
            regex=regex,  # type: ignore[arg-type]
            order_by=order_by or [],  # type: ignore[arg-type]
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
    ) -> ParquetAsset:  # type: ignore[valid-type]
        """Adds a Parquet DataAsst to the present "PandasDatasource" object.

        Args:
            name: The name of the csv asset
            base_directory: base directory path, relative to which CSV file paths will be collected
            regex: regex pattern that matches csv filenames that is used to label the batches
            order_by: sorting directive via either List[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_parquet`` keyword args
        """
        asset = ParquetAsset(
            name=name,
            base_directory=base_directory,  # type: ignore[arg-type]
            regex=regex,  # type: ignore[arg-type]
            order_by=order_by or [],  # type: ignore[arg-type]
            **kwargs,
        )
        return self.add_asset(asset)
