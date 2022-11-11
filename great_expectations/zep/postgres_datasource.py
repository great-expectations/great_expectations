from __future__ import annotations

from pprint import pformat as pf
from typing import Any, Dict, List, MutableMapping, Optional, Type

from pydantic import dataclasses as dc
from typing_extensions import ClassVar, Literal

from great_expectations.zep.logger import init_logger

init_logger()

from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec

# from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.zep.fakes import (
    FakeSqlAlchemyExecutionEngine as SqlAlchemyExecutionEngine,
)
from great_expectations.zep.interfaces import (
    Batch,
    BatchRequest,
    BatchRequestOptions,
    DataAsset,
    Datasource,
)


class PostgresDatasourceError(Exception):
    pass


@dc.dataclass(frozen=True)
class ColumnSplitter:
    method_name: str
    column_name: str
    name: str
    template_params: List[str]


class TableAsset(DataAsset):
    # Instance fields
    type: Literal["table"] = "table"
    table_name: str
    column_splitter: Optional[ColumnSplitter] = None
    name: str

    @property
    def datasource(self) -> PostgresDatasource:
        return super().datasource  # type: ignore[return-value] # subclass of Datasource

    def get_batch_request(
        self, options: Optional[BatchRequestOptions] = None
    ) -> BatchRequest:
        """A batch request that can be used to obtain batches for this DataAsset.

        Args:
            options: A dict that can be used to limit the number of batches returned from the asset.
                The dict structure depends on the asset type. A template of the dict can be obtained by
                calling batch_request_template.

        Returns:
            A BatchRequest object that can be used to obtain a batch list from a Datasource by calling the
            get_batch_list_from_batch_request method.
        """
        return BatchRequest(
            datasource_name=self.datasource.name,
            data_asset_name=self.name,
            options=options or {},
        )

    def batch_request_template(
        self,
    ) -> BatchRequestOptions:
        """A BatchRequestOptions template that can be used when calling get_batch_request.

        Returns:
            A BatchRequestOptions dictionary with the correct shape that get_batch_request
            will understand. All the option values will be filled in with the placeholder "value".
        """
        if not self.column_splitter:
            template: BatchRequestOptions = {}
            return template
        params_dict: BatchRequestOptions
        params_dict = {p: "<value>" for p in self.column_splitter.template_params}
        if self.column_splitter.name:
            params_dict = {self.column_splitter.name: params_dict}
        return params_dict

    # This asset type will support a variety of splitters
    def add_year_and_month_splitter(
        self, column_name: str, name: str = ""
    ) -> TableAsset:
        """Associates a year month splitter with this DataAsset

        Args:
            column_name: A column name of the date column where year and month will be parsed out.
            name: A name for the splitter that will be used to namespace the batch request options.
                Leaving this empty, "", will add the options to the global namespace.

        Returns:
            This TableAsset so we can use this method fluently.
        """
        self.column_splitter = ColumnSplitter(
            method_name="split_on_year_and_month",
            column_name=column_name,
            name=name,
            template_params=["year", "month"],
        )
        return self


class PostgresDatasource(Datasource):
    # class var definitions
    asset_types: ClassVar[List[Type[DataAsset]]] = [TableAsset]

    # right side of the operator determines the type name
    # left side enforces the names on instance creation
    type: Literal["postgres"] = "postgres"
    connection_str: str
    execution_engine: SqlAlchemyExecutionEngine
    assets: MutableMapping[str, TableAsset]

    def add_table_asset(self, name: str, table_name: str) -> TableAsset:
        """Adds a table asset to this datasource.

        Args:
            name: The name of this table asset.
            table_name: The table where the data resides.

        Returns:
            The TableAsset that is added to the datasource.
        """
        asset = TableAsset(name=name, datasource=self, table_name=table_name)
        self.assets[name] = asset
        return asset

    def get_asset(self, asset_name: str) -> TableAsset:
        """Returns the TableAsset referred to by name"""
        return super().get_asset(asset_name)  # type: ignore[return-value] # value is subclass

    # When we have multiple types of DataAssets on a datasource, the batch_request argument will be a Union type.
    # To differentiate we could use single dispatch or use an if/else (note pattern matching doesn't appear until
    # python 3.10)
    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> List[Batch]:
        """A list of batches that match the BatchRequest.

        Args:
            batch_request: A batch request for this asset. Usually obtained by calling
                get_batch_request on the asset.

        Returns:
            A list of batches that match the options specified in the batch request.
        """
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
            try:
                param_lookup = (
                    batch_request.options[column_splitter.name]
                    if column_splitter.name
                    else batch_request.options
                )
            except KeyError as e:
                raise PostgresDatasourceError(
                    "One must specify the batch request options in this form: "
                    f"{pf(data_asset.batch_request_template())}. It was specified like {pf(batch_request.options)}"
                ) from e

            column_splitter_kwargs = {}
            for param_name in column_splitter.template_params:
                column_splitter_kwargs[param_name] = (
                    param_lookup[param_name] if param_name in param_lookup else None
                )
                batch_spec_kwargs["batch_identifiers"].update(
                    {column_splitter.column_name: column_splitter_kwargs}
                )

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
