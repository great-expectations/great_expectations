from __future__ import annotations

import dataclasses
from typing import Dict, List, Optional, Set

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
    def __init__(
        self,
        datasource: Datasource,
        name: str,
        table_name: str,
        # TODO(bdirks): Provide a mechanism to add splitters in __init__
    ):
        super().__init__(name)
        self.table_name = table_name
        self._datasource = datasource
        self._name = name
        self._batch_template: Dict[str, Set[str]] = {}
        self._column_splitter: Optional[ColumnSplitter] = None

    @property
    def name(self):
        return self._name

    @property
    def datasource(self):
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
    def add_year_and_month_splitter(self, column_name: str, namespace: str = ""):
        self._column_splitter = ColumnSplitter(
            method_name="split_on_year_and_month",
            column_name=column_name,
            namespace=namespace,
            template_params=["year", "month"],
        )
        return self


class PostgresDatasource(Datasource):
    asset_types = [TableAsset]

    def __init__(self, name: str, connection_str: str):
        self.name = name
        self.execution_engine = SqlAlchemyExecutionEngine(
            connection_string=connection_str
        )
        self.assets: Dict[str, DataAsset] = {}

    def add_table_asset(self, name: str, table_name: str) -> TableAsset:
        asset = TableAsset(datasource=self, name=name, table_name=table_name)
        self.assets[name] = asset
        return asset

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
        batch_spec_kwargs = {
            "type": "table",
            "data_asset_name": data_asset.name,
            "table_name": data_asset.table_name,
            "batch_identifiers": {},
        }
        if column_splitter := data_asset._column_splitter:
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
                params_dict = {p: "<value>" for p in column_splitter.template_params}
                if ns:
                    params_dict = {ns: {params_dict}}
                raise PostgresDatasourceError(
                    "\nYou must specify the batch request splitter options as a dictionary that looks like "
                    f"this: {params_dict}\nYou've specified {batch_request.options}"
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
