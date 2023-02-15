from __future__ import annotations

import logging
import pathlib
import re
from typing import TYPE_CHECKING, ClassVar, Dict, List, Optional, Type, Union

from typing_extensions import Literal

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
    TestConnectionError,
    _batch_sorter_from_list,
)

if TYPE_CHECKING:
    from great_expectations.execution_engine import PandasExecutionEngine
    from great_expectations.experimental.datasources.interfaces import (
        Batch,
        BatchRequest,
        BatchRequestOptions,
    )

logger = logging.getLogger(__name__)


class PandasDatasourceError(Exception):
    pass


class _PandasDataAsset(DataAsset):
    def test_connection(self) -> None:
        ...

    def batch_request_options_template(
        self,
    ) -> BatchRequestOptions:
        ...

    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> list[Batch]:
        ...

    def build_batch_request(
        self, options: Optional[BatchRequestOptions] = None
    ) -> BatchRequest:
        ...

    def _validate_batch_request(self, batch_request: BatchRequest) -> None:
        ...


_FILESYSTEM_BLACK_LIST = (
    # "read_csv",
    # "read_json",
    # "read_excel",
    # "read_parquet",
    "read_clipboard",  # not path based
    # "read_feather",
    "read_fwf",  # unhandled type
    "read_gbq",  # not path based
    # "read_hdf",
    # "read_html",
    # "read_orc",
    # "read_pickle",
    # "read_sas",  # invalid json schema
    # "read_spss",
    "read_sql",  # not path based & type-name conflict
    "read_sql_query",  # not path based
    "read_sql_table",  # not path based
    "read_table",  # type-name conflict
    # "read_xml",
)

_FILESYSTEM_ASSET_MODELS = _generate_pandas_data_asset_models(
    _FilesystemDataAsset,
    blacklist=_FILESYSTEM_BLACK_LIST,
    use_docstring_from_method=True,
    skip_first_param=True,
)

_PANDAS_BLACK_LIST = (
    # "read_csv",
    # "read_json",
    # "read_excel",
    # "read_parquet",
    # "read_clipboard",
    # "read_feather",
    # "read_fwf",
    # "read_gbq",
    # "read_hdf",
    # "read_html",
    # "read_orc",
    # "read_pickle",
    # "read_sas",
    # "read_spss",
    # "read_sql",
    # "read_sql_query",
    # "read_sql_table",
    # "read_table",
    # "read_xml",
)

_PANDAS_ASSET_MODELS = _generate_pandas_data_asset_models(
    _PandasDataAsset,
    blacklist=_PANDAS_BLACK_LIST,
    use_docstring_from_method=True,
    skip_first_param=False,
)

try:
    # variables only needed for type-hinting
    CSVAsset = _FILESYSTEM_ASSET_MODELS["csv"]
    ExcelAsset = _FILESYSTEM_ASSET_MODELS["excel"]
    JSONAsset = _FILESYSTEM_ASSET_MODELS["json"]
    ORCAsset = _FILESYSTEM_ASSET_MODELS["orc"]
    ParquetAsset = _FILESYSTEM_ASSET_MODELS["parquet"]
except KeyError as key_err:
    logger.info(f"zep - {key_err} asset model could not be generated")
    CSVAsset = _FilesystemDataAsset
    ExcelAsset = _FilesystemDataAsset
    JSONAsset = _FilesystemDataAsset
    ORCAsset = _FilesystemDataAsset
    ParquetAsset = _FilesystemDataAsset


class _PandasDatasource(Datasource):
    # class attributes
    asset_types: ClassVar[List[Type[DataAsset]]] = []

    # Abstract Methods
    @property
    def execution_engine_type(self) -> Type[PandasExecutionEngine]:
        """Return the PandasExecutionEngine unless the override is set"""
        from great_expectations.execution_engine.pandas_execution_engine import (
            PandasExecutionEngine,
        )

        return PandasExecutionEngine

    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the _PandasDatasource.

        Args:
            test_assets: If assets have been passed to the _PandasDatasource,
                         an attempt can be made to test them as well.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        raise NotImplementedError(
            """One needs to implement "test_connection" on a _PandasDatasource subclass."""
        )

    # End Abstract Methods


class PandasDatasource(_PandasDatasource):
    # class attributes
    asset_types: ClassVar[List[Type[DataAsset]]] = list(_PANDAS_ASSET_MODELS.values())

    # instance attributes
    type: Literal["pandas"] = "pandas"
    name: str
    assets: Dict[
        str,
        _PandasDataAsset,
    ] = {}

    def test_connection(self) -> None:
        ...


class PandasFilesystemDatasource(_PandasDatasource):
    # class attributes
    asset_types: ClassVar[List[Type[DataAsset]]] = list(
        _FILESYSTEM_ASSET_MODELS.values()
    )

    # instance attributes
    type: Literal["pandas_filesystem"] = "pandas_filesystem"
    name: str
    base_directory: pathlib.Path
    assets: Dict[
        str,
        _FilesystemDataAsset,
    ] = {}

    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the PandasDatasource.

        Args:
            test_assets: If assets have been passed to the PandasDatasource, whether to test them as well.

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
        regex: Union[re.Pattern, str],
        order_by: Optional[BatchSortersDefinition] = None,
        **kwargs,  # TODO: update signature to have specific keys & types
    ) -> CSVAsset:  # type: ignore[valid-type]
        """Adds a CSV DataAsst to the present "PandasDatasource" object.

        Args:
            name: The name of the csv asset
            regex: regex pattern that matches csv filenames that is used to label the batches
            order_by: sorting directive via either list[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_csv`` keyword args
        """
        if isinstance(regex, str):
            regex = re.compile(regex)
        asset = CSVAsset(
            name=name,
            regex=regex,
            order_by=_batch_sorter_from_list(order_by or []),
            **kwargs,
        )
        return self.add_asset(asset)

    def add_excel_asset(
        self,
        name: str,
        regex: Union[str, re.Pattern],
        order_by: Optional[BatchSortersDefinition] = None,
        **kwargs,  # TODO: update signature to have specific keys & types
    ) -> ExcelAsset:  # type: ignore[valid-type]
        """Adds an Excel DataAsst to the present "PandasDatasource" object.

        Args:
            name: The name of the csv asset
            regex: regex pattern that matches csv filenames that is used to label the batches
            order_by: sorting directive via either list[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_excel`` keyword args
        """
        if isinstance(regex, str):
            regex = re.compile(regex)
        asset = ExcelAsset(
            name=name,
            regex=regex,
            order_by=_batch_sorter_from_list(order_by or []),
            **kwargs,
        )
        return self.add_asset(asset)

    def add_json_asset(
        self,
        name: str,
        regex: Union[str, re.Pattern],
        order_by: Optional[BatchSortersDefinition] = None,
        **kwargs,  # TODO: update signature to have specific keys & types
    ) -> JSONAsset:  # type: ignore[valid-type]
        """Adds a JSON DataAsst to the present "PandasDatasource" object.

        Args:
            name: The name of the csv asset
            regex: regex pattern that matches csv filenames that is used to label the batches
            order_by: sorting directive via either list[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_json`` keyword args
        """
        if isinstance(regex, str):
            regex = re.compile(regex)
        asset = JSONAsset(
            name=name,
            regex=regex,
            order_by=_batch_sorter_from_list(order_by or []),
            **kwargs,
        )
        return self.add_asset(asset)

    def add_parquet_asset(
        self,
        name: str,
        regex: Union[str, re.Pattern],
        order_by: Optional[BatchSortersDefinition] = None,
        **kwargs,  # TODO: update signature to have specific keys & types
    ) -> ParquetAsset:  # type: ignore[valid-type]
        """Adds a Parquet DataAsst to the present "PandasDatasource" object.

        Args:
            name: The name of the csv asset
            regex: regex pattern that matches csv filenames that is used to label the batches
            order_by: sorting directive via either list[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_parquet`` keyword args
        """
        if isinstance(regex, str):
            regex = re.compile(regex)
        asset = ParquetAsset(
            name=name,
            regex=regex,
            order_by=_batch_sorter_from_list(order_by or []),
            **kwargs,
        )
        return self.add_asset(asset)
