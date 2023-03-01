from __future__ import annotations

import copy
import dataclasses
import logging
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Dict,
    Generic,
    List,
    MutableMapping,
    Optional,
    Set,
    Type,
)

import pydantic
from typing_extensions import Literal

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.batch_spec import PandasBatchSpec
from great_expectations.experimental.datasources.dynamic_pandas import (
    _generate_pandas_data_asset_models,
)
from great_expectations.experimental.datasources.interfaces import (
    Batch,
    BatchRequest,
    DataAsset,
    Datasource,
    _DataAssetT,
)
from great_expectations.experimental.datasources.signatures import _merge_signatures
from great_expectations.experimental.datasources.sources import (
    DEFAULT_PANDAS_DATA_ASSET_NAME,
)

if TYPE_CHECKING:
    from great_expectations.execution_engine import PandasExecutionEngine
    from great_expectations.experimental.datasources.interfaces import (
        BatchRequestOptions,
    )
    from great_expectations.validator.validator import Validator

logger = logging.getLogger(__name__)


class PandasDatasourceError(Exception):
    pass


class _PandasDataAsset(DataAsset):
    _EXCLUDE_FROM_READER_OPTIONS: ClassVar[Set[str]] = {
        "name",
        "order_by",
        "type",
    }

    class Config:
        """
        Need to allow extra fields for the base type because pydantic will first create
        an instance of `_PandasDataAsset` before we select and create the more specific
        asset subtype.
        Each specific subtype should `forbid` extra fields.
        """

        extra = pydantic.Extra.allow

    def _get_reader_method(self) -> str:
        raise NotImplementedError(
            """One needs to explicitly provide "reader_method" for Pandas DataAsset extensions as temporary \
work-around, until "type" naming convention and method for obtaining 'reader_method' from it are established."""
        )

    def test_connection(self) -> None:
        pass

    def batch_request_options_template(
        self,
    ) -> BatchRequestOptions:
        return {}

    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> list[Batch]:
        self._validate_batch_request(batch_request)
        batch_list: List[Batch] = []

        batch_spec = PandasBatchSpec(
            reader_method=self._get_reader_method(),
            reader_options=self.dict(
                exclude=self._EXCLUDE_FROM_READER_OPTIONS,
                exclude_unset=True,
                by_alias=True,
            ),
        )
        execution_engine: PandasExecutionEngine = self.datasource.get_execution_engine()
        data, markers = execution_engine.get_batch_data_and_markers(
            batch_spec=batch_spec
        )

        # batch_definition (along with batch_spec and markers) is only here to satisfy a
        # legacy constraint when computing usage statistics in a validator. We hope to remove
        # it in the future.
        # imports are done inline to prevent a circular dependency with core/batch.py
        from great_expectations.core import IDDict
        from great_expectations.core.batch import BatchDefinition

        batch_definition = BatchDefinition(
            datasource_name=self.datasource.name,
            data_connector_name="experimental",
            data_asset_name=self.name,
            batch_identifiers=IDDict(batch_request.options),
            batch_spec_passthrough=None,
        )

        batch_metadata = copy.deepcopy(batch_request.options)

        # Some pydantic annotations are postponed due to circular imports.
        # Batch.update_forward_refs() will set the annotations before we
        # instantiate the Batch class since we can import them in this scope.
        Batch.update_forward_refs()
        batch_list.append(
            Batch(
                datasource=self.datasource,
                data_asset=self,
                batch_request=batch_request,
                data=data,
                metadata=batch_metadata,
                legacy_batch_markers=markers,
                legacy_batch_spec=batch_spec,
                legacy_batch_definition=batch_definition,
            )
        )
        return batch_list

    def build_batch_request(
        self, options: Optional[BatchRequestOptions] = None
    ) -> BatchRequest:
        if options:
            actual_keys = set(options.keys())
            raise gx_exceptions.InvalidBatchRequestError(
                "Data Assets associated with PandasDatasource can only contain a single batch,\n"
                "therefore BatchRequest options cannot be supplied. BatchRequest options with keys:\n"
                f"{actual_keys}\nwere passed.\n"
            )

        return BatchRequest(
            datasource_name=self.datasource.name,
            data_asset_name=self.name,
            options={},
        )

    def _validate_batch_request(self, batch_request: BatchRequest) -> None:
        """Validates the batch_request has the correct form.

        Args:
            batch_request: A batch request object to be validated.
        """
        if not (
            batch_request.datasource_name == self.datasource.name
            and batch_request.data_asset_name == self.name
            and not batch_request.options
        ):
            expect_batch_request_form = BatchRequest(
                datasource_name=self.datasource.name,
                data_asset_name=self.name,
                options={},
            )
            raise gx_exceptions.InvalidBatchRequestError(
                "BatchRequest should have form:\n"
                f"{pf(dataclasses.asdict(expect_batch_request_form))}\n"
                f"but actually has form:\n{pf(dataclasses.asdict(batch_request))}\n"
            )


_PANDAS_READER_METHOD_UNSUPPORTED_LIST = (
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
    # "read_sas",
    # "read_spss",
    # "read_sql",
    # "read_sql_query",
    # "read_sql_table",
    # "read_stata",
    # "read_table",
    # "read_xml",
)


_PANDAS_ASSET_MODELS = _generate_pandas_data_asset_models(
    _PandasDataAsset,
    blacklist=_PANDAS_READER_METHOD_UNSUPPORTED_LIST,
    use_docstring_from_method=True,
    skip_first_param=False,
)


try:
    ClipboardAsset = _PANDAS_ASSET_MODELS["clipboard"]
    CSVAsset = _PANDAS_ASSET_MODELS["csv"]
    ExcelAsset = _PANDAS_ASSET_MODELS["excel"]
    FeatherAsset = _PANDAS_ASSET_MODELS["feather"]
    GBQAsset = _PANDAS_ASSET_MODELS["gbq"]
    HDFAsset = _PANDAS_ASSET_MODELS["hdf"]
    HTMLAsset = _PANDAS_ASSET_MODELS["html"]
    JSONAsset = _PANDAS_ASSET_MODELS["json"]
    ORCAsset = _PANDAS_ASSET_MODELS["orc"]
    ParquetAsset = _PANDAS_ASSET_MODELS["parquet"]
    PickleAsset = _PANDAS_ASSET_MODELS["pickle"]
    SQLAsset = _PANDAS_ASSET_MODELS["sql"]
    SQLQueryAsset = _PANDAS_ASSET_MODELS["sql_query"]
    SQLTableAsset = _PANDAS_ASSET_MODELS["sql_table"]
    SASAsset = _PANDAS_ASSET_MODELS["sas"]
    SPSSAsset = _PANDAS_ASSET_MODELS["spss"]
    StataAsset = _PANDAS_ASSET_MODELS["stata"]
    TableAsset = _PANDAS_ASSET_MODELS["table"]
    XMLAsset = _PANDAS_ASSET_MODELS["xml"]
except KeyError as key_err:
    logger.info(f"zep - {key_err} asset model could not be generated")
    ClipboardAsset = _PandasDataAsset
    CSVAsset = _PandasDataAsset
    ExcelAsset = _PandasDataAsset
    FeatherAsset = _PandasDataAsset
    GBQAsset = _PandasDataAsset
    HDFAsset = _PandasDataAsset
    HTMLAsset = _PandasDataAsset
    JSONAsset = _PandasDataAsset
    ORCAsset = _PandasDataAsset
    ParquetAsset = _PandasDataAsset
    PickleAsset = _PandasDataAsset
    SQLAsset = _PandasDataAsset
    SQLQueryAsset = _PandasDataAsset
    SQLTableAsset = _PandasDataAsset
    SASAsset = _PandasDataAsset
    SPSSAsset = _PandasDataAsset
    StataAsset = _PandasDataAsset
    TableAsset = _PandasDataAsset
    XMLAsset = _PandasDataAsset


class _PandasDatasource(Datasource, Generic[_DataAssetT]):
    # class attributes
    asset_types: ClassVar[List[Type[DataAsset]]] = []

    # instance attributes
    assets: MutableMapping[
        str,
        _DataAssetT,
    ] = {}

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

    # private attributes
    _data_context = pydantic.PrivateAttr()

    # instance attributes
    type: Literal["pandas"] = "pandas"
    assets: Dict[
        str,
        _PandasDataAsset,
    ] = {}

    def test_connection(self, test_assets: bool = True) -> None:
        ...

    def _get_validator(self, asset: _PandasDataAsset) -> Validator:
        batch_request: BatchRequest = asset.build_batch_request()
        return self._data_context.get_validator(batch_request=batch_request)

    def add_clipboard_asset(self, name: str, **kwargs) -> ClipboardAsset:  # type: ignore[valid-type]
        asset = ClipboardAsset(
            name=name,
            **kwargs,
        )
        return self.add_asset(asset=asset)

    def read_clipboard(self, name: Optional[str] = None, **kwargs) -> Validator:
        if not name:
            name = DEFAULT_PANDAS_DATA_ASSET_NAME
        asset: ClipboardAsset = self.add_clipboard_asset(name=name, **kwargs)  # type: ignore[valid-type]
        return self._get_validator(asset=asset)

    def add_csv_asset(
        self, name: str, filepath_or_buffer: pydantic.FilePath, **kwargs
    ) -> CSVAsset:  # type: ignore[valid-type]
        asset = CSVAsset(
            name=name,
            filepath_or_buffer=filepath_or_buffer,
            **kwargs,
        )
        return self.add_asset(asset=asset)

    def read_csv(
        self,
        filepath_or_buffer: pydantic.FilePath,
        asset_name: Optional[str] = None,
        **kwargs,
    ) -> Validator:
        if not asset_name:
            asset_name = DEFAULT_PANDAS_DATA_ASSET_NAME
        asset: CSVAsset = self.add_csv_asset(  # type: ignore[valid-type]
            name=asset_name,
            filepath_or_buffer=filepath_or_buffer,
            **kwargs,
        )
        return self._get_validator(asset=asset)

    def add_excel_asset(
        self, name: str, io: str, **kwargs
    ) -> ExcelAsset:  # type: ignore[valid-type]
        asset = ExcelAsset(
            name=name,
            io=io,
            **kwargs,
        )
        return self.add_asset(asset=asset)

    def read_excel(
        self,
        io: str,
        asset_name: Optional[str] = None,
        **kwargs,
    ) -> Validator:
        if not asset_name:
            asset_name = DEFAULT_PANDAS_DATA_ASSET_NAME
        asset: ExcelAsset = self.add_excel_asset(  # type: ignore[valid-type]
            name=asset_name,
            io=io,
            **kwargs,
        )
        return self._get_validator(asset=asset)

    def add_feather_asset(
        self, name: str, path: pydantic.FilePath, **kwargs
    ) -> FeatherAsset:  # type: ignore[valid-type]
        asset = FeatherAsset(
            name=name,
            path=path,
            **kwargs,
        )
        return self.add_asset(asset=asset)

    def read_feather(
        self,
        path: pydantic.FilePath,
        asset_name: Optional[str] = None,
        **kwargs,
    ) -> Validator:
        if not asset_name:
            asset_name = DEFAULT_PANDAS_DATA_ASSET_NAME
        asset: FeatherAsset = self.add_feather_asset(  # type: ignore[valid-type]
            name=asset_name,
            path=path,
            **kwargs,
        )
        return self._get_validator(asset=asset)

    def add_gbq_asset(
        self, name: str, query: str, **kwargs
    ) -> GBQAsset:  # type: ignore[valid-type]
        asset = GBQAsset(
            name=name,
            query=query,
            **kwargs,
        )
        return self.add_asset(asset=asset)

    def read_gbq(
        self,
        query: str,
        asset_name: Optional[str] = None,
        **kwargs,
    ) -> Validator:
        if not asset_name:
            asset_name = DEFAULT_PANDAS_DATA_ASSET_NAME
        asset: GBQAsset = self.add_gbq_asset(  # type: ignore[valid-type]
            name=asset_name,
            query=query,
            **kwargs,
        )
        return self._get_validator(asset=asset)

    def add_hdf_asset(
        self, name: str, path_or_buf: str, **kwargs
    ) -> HDFAsset:  # type: ignore[valid-type]
        asset = HDFAsset(
            name=name,
            path_or_buf=path_or_buf,
            **kwargs,
        )
        return self.add_asset(asset=asset)

    def read_hdf(
        self,
        path_or_buf: str,
        asset_name: Optional[str] = None,
        **kwargs,
    ) -> Validator:
        if not asset_name:
            asset_name = DEFAULT_PANDAS_DATA_ASSET_NAME
        asset: HDFAsset = self.add_hdf_asset(  # type: ignore[valid-type]
            name=asset_name,
            path_or_buf=path_or_buf,
            **kwargs,
        )
        return self._get_validator(asset=asset)

    def add_html_asset(
        self, name: str, io: str, **kwargs
    ) -> HTMLAsset:  # type: ignore[valid-type]
        asset = HTMLAsset(
            name=name,
            io=io,
            **kwargs,
        )
        return self.add_asset(asset=asset)

    def read_html(
        self,
        io: str,
        asset_name: Optional[str] = None,
        **kwargs,
    ) -> Validator:
        if not asset_name:
            asset_name = DEFAULT_PANDAS_DATA_ASSET_NAME
        asset: HTMLAsset = self.add_html_asset(  # type: ignore[valid-type]
            name=asset_name,
            io=io,
            **kwargs,
        )
        return self._get_validator(asset=asset)

    def add_json_asset(
        self, name: str, path_or_buf: str, **kwargs
    ) -> JSONAsset:  # type: ignore[valid-type]
        asset = JSONAsset(
            name=name,
            path_or_buf=path_or_buf,
            **kwargs,
        )
        return self.add_asset(asset=asset)

    def read_json(
        self,
        path_or_buf: str,
        asset_name: Optional[str] = None,
        **kwargs,
    ) -> Validator:
        if not asset_name:
            asset_name = DEFAULT_PANDAS_DATA_ASSET_NAME
        asset: JSONAsset = self.add_json_asset(  # type: ignore[valid-type]
            name=asset_name,
            path_or_buf=path_or_buf,
            **kwargs,
        )
        return self._get_validator(asset=asset)

    def add_orc_asset(
        self, name: str, path: str, **kwargs
    ) -> ORCAsset:  # type: ignore[valid-type]
        asset = ORCAsset(
            name=name,
            path=path,
            **kwargs,
        )
        return self.add_asset(asset=asset)

    def read_orc(
        self,
        path: str,
        asset_name: Optional[str] = None,
        **kwargs,
    ) -> Validator:
        if not asset_name:
            asset_name = DEFAULT_PANDAS_DATA_ASSET_NAME
        asset: ORCAsset = self.add_orc_asset(  # type: ignore[valid-type]
            name=asset_name,
            path=path,
            **kwargs,
        )
        return self._get_validator(asset=asset)

    def add_parquet_asset(
        self, name: str, path: str, **kwargs
    ) -> ParquetAsset:  # type: ignore[valid-type]
        asset = ParquetAsset(
            name=name,
            path=path,
            **kwargs,
        )
        return self.add_asset(asset=asset)

    def read_parquet(
        self,
        path: str,
        asset_name: Optional[str] = None,
        **kwargs,
    ) -> Validator:
        if not asset_name:
            asset_name = DEFAULT_PANDAS_DATA_ASSET_NAME
        asset: ParquetAsset = self.add_parquet_asset(  # type: ignore[valid-type]
            name=asset_name,
            path=path,
            **kwargs,
        )
        return self._get_validator(asset=asset)

    def add_pickle_asset(
        self, name: str, filepath_or_buffer: pydantic.FilePath, **kwargs
    ) -> PickleAsset:  # type: ignore[valid-type]
        asset = PickleAsset(
            name=name,
            filepath_or_buffer=filepath_or_buffer,
            **kwargs,
        )
        return self.add_asset(asset=asset)

    def read_pickle(
        self,
        filepath_or_buffer: pydantic.FilePath,
        asset_name: Optional[str] = None,
        **kwargs,
    ) -> Validator:
        if not asset_name:
            asset_name = DEFAULT_PANDAS_DATA_ASSET_NAME
        asset: PickleAsset = self.add_pickle_asset(  # type: ignore[valid-type]
            name=asset_name,
            filepath_or_buffer=filepath_or_buffer,
            **kwargs,
        )
        return self._get_validator(asset=asset)

    def add_sas_asset(
        self, name: str, filepath_or_buffer: pydantic.FilePath, **kwargs
    ) -> SASAsset:  # type: ignore[valid-type]
        asset = SASAsset(
            name=name,
            filepath_or_buffer=filepath_or_buffer,
            **kwargs,
        )
        return self.add_asset(asset=asset)

    def read_sas(
        self,
        filepath_or_buffer: pydantic.FilePath,
        asset_name: Optional[str] = None,
        **kwargs,
    ) -> Validator:
        if not asset_name:
            asset_name = DEFAULT_PANDAS_DATA_ASSET_NAME
        asset: SASAsset = self.add_sas_asset(  # type: ignore[valid-type]
            name=asset_name,
            filepath_or_buffer=filepath_or_buffer,
            **kwargs,
        )
        return self._get_validator(asset=asset)

    def add_spss_asset(
        self, name: str, path: str, **kwargs
    ) -> SPSSAsset:  # type: ignore[valid-type]
        asset = SPSSAsset(
            name=name,
            path=path,
            **kwargs,
        )
        return self.add_asset(asset=asset)

    def read_spss(
        self,
        path: str,
        asset_name: Optional[str] = None,
        **kwargs,
    ) -> Validator:
        if not asset_name:
            asset_name = DEFAULT_PANDAS_DATA_ASSET_NAME
        asset: SPSSAsset = self.add_parquet_asset(  # type: ignore[valid-type]
            name=asset_name,
            path=path,
            **kwargs,
        )
        return self._get_validator(asset=asset)

    def add_sql_asset(
        self, name: str, sql: str, con: str, **kwargs
    ) -> SQLAsset:  # type: ignore[valid-type]
        asset = SQLAsset(
            name=name,
            sql=sql,
            con=con,
            **kwargs,
        )
        return self.add_asset(asset=asset)

    def read_sql(
        self,
        sql: str,
        con: str,
        asset_name: Optional[str] = None,
        **kwargs,
    ) -> Validator:
        if not asset_name:
            asset_name = DEFAULT_PANDAS_DATA_ASSET_NAME
        asset: SQLAsset = self.add_sql_asset(  # type: ignore[valid-type]
            name=asset_name,
            sql=sql,
            con=con,
            **kwargs,
        )
        return self._get_validator(asset=asset)

    def add_sql_query_asset(
        self, name: str, sql: str, con: str, **kwargs
    ) -> SQLQueryAsset:  # type: ignore[valid-type]
        asset = SQLQueryAsset(
            name=name,
            sql=sql,
            con=con,
            **kwargs,
        )
        return self.add_asset(asset=asset)

    def read_sql_query(
        self,
        sql: str,
        con: str,
        asset_name: Optional[str] = None,
        **kwargs,
    ) -> Validator:
        if not asset_name:
            asset_name = DEFAULT_PANDAS_DATA_ASSET_NAME
        asset: SQLQueryAsset = self.add_sql_query_asset(  # type: ignore[valid-type]
            name=asset_name,
            sql=sql,
            con=con,
            **kwargs,
        )
        return self._get_validator(asset=asset)

    def add_sql_table_asset(
        self, name: str, table_name: str, con: str, **kwargs
    ) -> SQLTableAsset:  # type: ignore[valid-type]
        asset = SQLTableAsset(
            name=name,
            table_name=table_name,
            con=con,
            **kwargs,
        )
        return self.add_asset(asset=asset)

    def read_sql_table(
        self,
        table_name: str,
        con: str,
        asset_name: Optional[str] = None,
        **kwargs,
    ) -> Validator:
        if not asset_name:
            asset_name = DEFAULT_PANDAS_DATA_ASSET_NAME
        asset: SQLTableAsset = self.add_sql_table_asset(  # type: ignore[valid-type]
            name=asset_name,
            table_name=table_name,
            con=con,
            **kwargs,
        )
        return self._get_validator(asset=asset)

    def add_stata_asset(
        self, name: str, filepath_or_buffer: pydantic.FilePath, **kwargs
    ) -> StataAsset:  # type: ignore[valid-type]
        asset = StataAsset(
            name=name,
            filepath_or_buffer=filepath_or_buffer,
            **kwargs,
        )
        return self.add_asset(asset=asset)

    def read_stata(
        self,
        filepath_or_buffer: pydantic.FilePath,
        asset_name: Optional[str] = None,
        **kwargs,
    ) -> Validator:
        if not asset_name:
            asset_name = DEFAULT_PANDAS_DATA_ASSET_NAME
        asset: StataAsset = self.add_stata_asset(  # type: ignore[valid-type]
            name=asset_name,
            filepath_or_buffer=filepath_or_buffer,
            **kwargs,
        )
        return self._get_validator(asset=asset)

    def add_table_asset(
        self, name: str, filepath_or_buffer: pydantic.FilePath, **kwargs
    ) -> TableAsset:  # type: ignore[valid-type]
        asset = TableAsset(
            name=name,
            filepath_or_buffer=filepath_or_buffer,
            **kwargs,
        )
        return self.add_asset(asset=asset)

    def read_table(
        self,
        filepath_or_buffer: pydantic.FilePath,
        asset_name: Optional[str] = None,
        **kwargs,
    ) -> Validator:
        if not asset_name:
            asset_name = DEFAULT_PANDAS_DATA_ASSET_NAME
        asset: TableAsset = self.add_table_asset(  # type: ignore[valid-type]
            name=asset_name,
            filepath_or_buffer=filepath_or_buffer,
            **kwargs,
        )
        return self._get_validator(asset=asset)

    # attr-defined issue
    # https://github.com/python/mypy/issues/12472
    add_clipboard_asset.__signature__ = _merge_signatures(add_clipboard_asset, ClipboardAsset, exclude={"type"})  # type: ignore[attr-defined]
    add_csv_asset.__signature__ = _merge_signatures(add_csv_asset, CSVAsset, exclude={"type"})  # type: ignore[attr-defined]
    add_excel_asset.__signature__ = _merge_signatures(add_excel_asset, ExcelAsset, exclude={"type"})  # type: ignore[attr-defined]
    add_feather_asset.__signature__ = _merge_signatures(add_feather_asset, FeatherAsset, exclude={"type"})  # type: ignore[attr-defined]
    add_gbq_asset.__signature__ = _merge_signatures(add_gbq_asset, GBQAsset, exclude={"type"})  # type: ignore[attr-defined]
    add_hdf_asset.__signature__ = _merge_signatures(  # type: ignore[attr-defined]
        add_hdf_asset, HDFAsset, exclude={"type"}
    )
    add_html_asset.__signature__ = _merge_signatures(  # type: ignore[attr-defined]
        add_html_asset, HTMLAsset, exclude={"type"}
    )
    add_json_asset.__signature__ = _merge_signatures(  # type: ignore[attr-defined]
        add_json_asset, JSONAsset, exclude={"type"}
    )
    add_orc_asset.__signature__ = _merge_signatures(  # type: ignore[attr-defined]
        add_orc_asset, ORCAsset, exclude={"type"}
    )
    add_parquet_asset.__signature__ = _merge_signatures(  # type: ignore[attr-defined]
        add_parquet_asset, ParquetAsset, exclude={"type"}
    )
    add_pickle_asset.__signature__ = _merge_signatures(  # type: ignore[attr-defined]
        add_pickle_asset, PickleAsset, exclude={"type"}
    )
    add_sas_asset.__signature__ = _merge_signatures(  # type: ignore[attr-defined]
        add_sas_asset, SASAsset, exclude={"type"}
    )
    add_spss_asset.__signature__ = _merge_signatures(  # type: ignore[attr-defined]
        add_spss_asset, SPSSAsset, exclude={"type"}
    )
    add_sql_asset.__signature__ = _merge_signatures(  # type: ignore[attr-defined]
        add_sql_asset, SQLAsset, exclude={"type"}
    )
    add_sql_query_asset.__signature__ = _merge_signatures(  # type: ignore[attr-defined]
        add_sql_query_asset, SQLQueryAsset, exclude={"type"}
    )
    add_sql_table_asset.__signature__ = _merge_signatures(  # type: ignore[attr-defined]
        add_sql_table_asset, SQLTableAsset, exclude={"type"}
    )
    add_stata_asset.__signature__ = _merge_signatures(  # type: ignore[attr-defined]
        add_stata_asset, StataAsset, exclude={"type"}
    )
    add_table_asset.__signature__ = _merge_signatures(  # type: ignore[attr-defined]
        add_table_asset, TableAsset, exclude={"type"}
    )
