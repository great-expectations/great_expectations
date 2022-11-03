from __future__ import annotations

import dataclasses
from pprint import pformat as pf
from typing import Any, Dict, List, Optional, Type, Union

from typing_extensions import ClassVar

from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.zep.interfaces import (
    Batch,
    BatchRequest,
    BatchRequestOptions,
    DataAsset,
    Datasource,
)


class PostgresDatasourceError(Exception):
    pass


@dataclasses.dataclass(frozen=True)
class ColumnSplitter:
    method_name: str
    column_name: str
    namespace: str
    template_params: List[str]


class TableAsset(DataAsset):
    # Instance variables
    table_name: str
    column_splitter: Optional[ColumnSplitter]
    _name: str
    _datasource: Datasource

    def __init__(
        self,
        name: str,
        datasource: Datasource,
        table_name: str,
        column_splitter: Optional[ColumnSplitter] = None,
    ) -> None:
        super().__init__(name)
        self.table_name = table_name
        self._datasource = datasource
        self._name = name
        self.column_splitter: Optional[ColumnSplitter] = column_splitter

    @property
    def name(self) -> str:
        return self._name

    @property
    def datasource(self) -> Datasource:
        return self._datasource

    def get_batch_request(
        self, options: Optional[BatchRequestOptions] = None
    ) -> BatchRequest:
        return BatchRequest(
            datasource_name=self.datasource.name,
            data_asset_name=self.name,
            options=options or {},
        )

    # This asset type will support a variety of splitters
    def add_year_and_month_splitter(
        self, column_name: str, namespace: str = ""
    ) -> TableAsset:
        self.column_splitter = ColumnSplitter(
            method_name="split_on_year_and_month",
            column_name=column_name,
            namespace=namespace,
            template_params=["year", "month"],
        )
        return self


class PostgresDatasource(Datasource):
    # class var definitions
    asset_types: ClassVar[List[Type[DataAsset]]] = [TableAsset]

    def __init__(self, name: str, connection_str: str) -> None:
        self.name = name
        self.execution_engine = SqlAlchemyExecutionEngine(
            connection_string=connection_str
        )
        self.assets: Dict[str, TableAsset] = {}

    def add_table_asset(self, name: str, table_name: str) -> TableAsset:
        asset = TableAsset(name=name, datasource=self, table_name=table_name)
        self.assets[name] = asset
        return asset

    def get_asset(self, asset_name: str) -> TableAsset:
        """Returns the TableAsset referred to by name"""
        try:
            return self.assets[asset_name]
        except KeyError as e:
            raise PostgresDatasourceError(
                f"No table asset named {asset_name}. Available assets are {self.assets.keys()}"
            ) from e

    # When we have multiple types of DataAssets on a datasource, the batch_request argument will be a Union type.
    # To differentiate we could use single dispatch or use an if/else (note pattern matching doesn't appear until
    # python 3.10)
    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> List[Batch]:
        # We translate the batch_request into a BatchSpec to hook into GX core.
        # NOTE: We only produce 1 batch right now
        data_asset = self.get_asset(batch_request.data_asset_name)

        # We look at the splitters on the data asset and verify that the passed in batch request provides the
        # correct arguments to specify the batch
        batch_identifiers: Dict[str, Any] = {}
        batch_spec_kwargs: Dict[str, Any] = {
            "type": "table",
            "data_asset_name": data_asset.name,
            "table_name": data_asset.table_name,
            "batch_identifiers": batch_identifiers,
        }
        if data_asset.column_splitter:
            column_splitter = data_asset.column_splitter
            batch_spec_kwargs["splitter_method"] = column_splitter.method_name
            batch_spec_kwargs["splitter_kwargs"] = {
                "column_name": column_splitter.column_name
            }

            ns = column_splitter.namespace
            param_lookup = batch_request.options[ns] if ns else batch_request.options
            column_splitter_kwargs = {}
            try:
                for param_name in column_splitter.template_params:
                    column_splitter_kwargs[param_name] = param_lookup[param_name]
                batch_spec_kwargs["batch_identifiers"].update(
                    {column_splitter.column_name: column_splitter_kwargs}
                )
            except KeyError as e:
                params_dict: Union[Dict[str, str], Dict[str, Dict[str, str]]]
                params_dict = {p: "<value>" for p in column_splitter.template_params}
                if ns:
                    params_dict = {ns: params_dict}
                raise PostgresDatasourceError(
                    "\nYou must specify the batch request splitter options as a dictionary that looks like "
                    f"this: {pf(params_dict)}\nYou've specified {pf(batch_request.options)}"
                ) from e

        # Now, that we've verified the arguments, we can create the batch_spec and then the batch.
        batch_spec = SqlAlchemyDatasourceBatchSpec(**batch_spec_kwargs)
        data, _ = self.execution_engine.get_batch_data_and_markers(
            batch_spec=batch_spec
        )
        return [
            Batch(
                datasource=self,
                data_asset=data_asset,
                batch_request=batch_request,
                data=data,
            )
        ]
