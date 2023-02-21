from __future__ import annotations

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
    Type,
)

from typing_extensions import Literal

import great_expectations.exceptions as gx_exceptions
from great_expectations.experimental.datasources.dynamic_pandas import (
    _generate_pandas_data_asset_models,
)
from great_expectations.experimental.datasources.interfaces import (
    BatchRequest,
    DataAsset,
    Datasource,
    _DataAssetT,
)

if TYPE_CHECKING:
    from great_expectations.execution_engine import PandasExecutionEngine
    from great_expectations.experimental.datasources.interfaces import (
        Batch,
        BatchRequestOptions,
    )

logger = logging.getLogger(__name__)


class PandasDatasourceError(Exception):
    pass


class _PandasDataAsset(DataAsset):
    def test_connection(self) -> None:
        pass

    def batch_request_options_template(
        self,
    ) -> BatchRequestOptions:
        return {}

    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> list[Batch]:
        return []

    def build_batch_request(
        self, options: Optional[BatchRequestOptions] = None
    ) -> BatchRequest:
        if options is not None and not self._valid_batch_request_options(options):
            allowed_keys = set(self.batch_request_options_template().keys())
            actual_keys = set(options.keys())
            raise gx_exceptions.InvalidBatchRequestError(
                "Batch request options should only contain keys from the following set:\n"
                f"{allowed_keys}\nbut your specified keys contain\n"
                f"{actual_keys.difference(allowed_keys)}\nwhich is not valid.\n"
            )

        return BatchRequest(
            datasource_name=self.datasource.name,
            data_asset_name=self.name,
            options=options or {},
        )

    def _validate_batch_request(self, batch_request: BatchRequest) -> None:
        """Validates the batch_request has the correct form.

        Args:
            batch_request: A batch request object to be validated.
        """
        if not (
            batch_request.datasource_name == self.datasource.name
            and batch_request.data_asset_name == self.name
            and self._valid_batch_request_options(batch_request.options)
        ):
            expect_batch_request_form = BatchRequest(
                datasource_name=self.datasource.name,
                data_asset_name=self.name,
                options=self.batch_request_options_template(),
            )
            raise gx_exceptions.InvalidBatchRequestError(
                "BatchRequest should have form:\n"
                f"{pf(dataclasses.asdict(expect_batch_request_form))}\n"
                f"but actually has form:\n{pf(dataclasses.asdict(batch_request))}\n"
            )


_PANDAS_BLACK_LIST = (
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
    # "read_table",
    # "read_xml",
)

_PANDAS_ASSET_MODELS = _generate_pandas_data_asset_models(
    _PandasDataAsset,
    blacklist=_PANDAS_BLACK_LIST,
    use_docstring_from_method=True,
    skip_first_param=False,
    type_prefix="pandas",
)


_FILE_TYPE_READER_METHOD_BLACK_LIST = (
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

    # instance attributes
    type: Literal["pandas"] = "pandas"
    name: str
    assets: Dict[
        str,
        _PandasDataAsset,
    ] = {}

    def test_connection(self, test_assets: bool = True) -> None:
        ...
