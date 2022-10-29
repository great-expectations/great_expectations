import dataclasses
from typing import List, Dict, Any, Optional

from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.zep.interfaces import Batch, BatchRequestOptions, BatchRequest, DataAsset, Datasource
from great_expectations.zep.metadatasource import MetaDatasouce


# DataAssets can be thought of as a way to connect to a datasource and a collection of templates. When the template
# values are specified, this results in a batch. The possible templates are the Optional fields in the DataAsset
# constructor (eg splitter for a TableAssetSplitter). If left as None, Batches are not specified by this dimension since
# there is no template to actualize. It not None, one can pass in the values to be used in the BatchRequest.
# For a TableAsset one uses a BatchRequest[TableAssetOptions] to specify the template values.


@dataclasses.dataclass(frozen=True)
class TableAssetSplitter:
    method_name: str
    column_name: str


@dataclasses.dataclass(frozen=True)
class TableAssetOptions(BatchRequestOptions):
    # Depending on the splitter method passed in the possible values one could pass in vary. For example for the
    # splitter `split_on_year_and_month` the possible kwarg keys are `year` and `month`. However, for the splitter
    # method `split_on_year_and_month_and_day` the possible kwarg keys are `year`, `month` and `day`. Because of
    # this the table_splitter_kwargs is a loosely typed dictionary.
    table_splitter_kwargs: Dict[str, Any]


class TableAsset(DataAsset[TableAssetOptions]):
    def __init__(
            self,
            datasource: Datasource,
            name: str,
            table_name: str,
            splitter: Optional[TableAssetSplitter] = None,
    ):
        super().__init__(name)
        self.table_name = table_name
        self.splitter = splitter
        self._datasource = datasource
        self._name = name

    @property
    def name(self):
        return self._name

    @property
    def datasource(self):
        return self._datasource

    def add_splitter(self, splitter):
        self.splitter = splitter
        # Update self._meta?

    def get_batch_request(self, options: Optional[TableAssetOptions]=None) -> BatchRequest[TableAssetOptions]:
        return BatchRequest[TableAssetOptions](
            datasource_name=self.datasource.name,
            data_asset_name=self.name,
            options=options
        )


class PostgresDatasource(metaclass=MetaDatasouce):
    asset_types = [TableAsset]

    def __init__(self, name: str, connection_str: str):
        self.name = name
        self.execution_engine = SqlAlchemyExecutionEngine(
            connection_string=connection_str
        )
        self.assets: Dict[str, DataAsset] = {}

    def add_table_asset(self,
                        name: str,
                        table_name: str,
                        splitter: Optional[TableAssetSplitter]=None) -> TableAsset:
        asset = TableAsset(datasource=self, name=name, table_name=table_name, splitter=splitter)
        self.assets[name] = asset
        return asset

    def get_asset(self, name) -> DataAsset:
        return self.assets[name]

    # When we have multiple types of DataAssets on a datasource, the batch_request argument will be a Union type.
    # We could pattern match on this type to differentiate. Since we need to support python < 3.10, we can use an
    # if/else condition instead of match and call a helper function fo each specific type.
    def get_batch_list_from_batch_request(
            self, batch_request: BatchRequest[TableAssetOptions]
    ) -> List[Batch[TableAssetOptions]]:
        # This only supports producing 1 batch right now
        data_asset = self.get_asset(batch_request.data_asset_name)
        # batch_identifiers will get populated with the "template" values
        batch_identifiers = {}
        # The splitter is one of the templatized
        splitter_params = {}
        if data_asset.splitter:
            splitter = dataclasses.asdict(data_asset.splitter)
            splitter_params["splitter_method"] = splitter.pop("method_name")
            splitter_params["splitter_kwargs"] = splitter
            # Populate the splitter parameters into batch_identifiers
            batch_identifiers.update({data_asset.splitter.column_name: batch_request.options.table_splitter_kwargs})
        batch_spec = SqlAlchemyDatasourceBatchSpec(**{
            "type": "table",
            "data_asset_name": data_asset.name,
            "table_name": data_asset.table_name,
            **splitter_params,
            "batch_identifiers": batch_identifiers,
        })
        data, _ = self.execution_engine.get_batch_data_and_markers(batch_spec=batch_spec)
        return [
            Batch(
                datasource=self,
                data_asset=data_asset,
                batch_request=batch_request,
                data=data
            )
        ]
