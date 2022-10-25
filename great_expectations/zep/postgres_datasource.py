import dataclasses
from typing import List, Dict, Any, Optional

from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.zep.interfaces import Batch, BatchParams, BatchRequest, DataAsset


class PostgresDataAsset(DataAsset):
    def __init__(
            self,
            name: str,
            table_name: str,
            # This maybe better a just "splitter" which takes a callable. However we want
            # to leverage the code in our sqlalchemy execution engine so we are leaving it
            # like this for now. To make it a callable we could do something like:
            # splitter = lambda: split_on_year_and_month(column_name="tpep_pickup_datetime")
            splitter_method: Optional[str],  # Optional to support fluent api
            splitter_kwargs: Optional[Dict[str, Any]],  # Optional to support fluent api
    ):
        super().__init__(name)
        self.table_name = table_name
        self.splitter_method = splitter_method
        self.splitter_kwargs = splitter_kwargs


@dataclasses.dataclass(frozen=True)
class PostgresBatchParams(BatchParams):
    splitter_filter_kwargs: Optional[Dict[str, Any]]


class PostgresDatasource():
    def __init__(self, name: str, connection_str: str):
        self.name = name
        self.execution_engine = SqlAlchemyExecutionEngine(
            connection_string=connection_str
        )
        self.assets: Dict[str, DataAsset] = {}

    def add_table_asset(self, data_asset_name: str, asset: DataAsset) -> None:
        self.assets[data_asset_name] = asset

    # BDIRKS: get asset can be added to the base class once I understand how
    #         Gabriel is managing this.
    def get_asset(self, name) -> DataAsset:
        return self.assets[name]

    def get_batch_list_from_batch_request(
            self, batch_request: BatchRequest[PostgresBatchParams]
    ) -> List[Batch]:
        data_asset = self.get_asset(batch_request.data_asset_name)
        batch_params: PostgresBatchParams = batch_request.batch_params
        batch_spec = SqlAlchemyDatasourceBatchSpec(**{
            "type": "table",
            "data_asset_name": data_asset.name,
            "table_name": data_asset.table_name,
            "splitter_method":  data_asset.splitter_method,
            "splitter_kwargs": data_asset.splitter_kwargs,
            # BDIRKS: batch_params may still be confusing to the user and we can revisit.
            "batch_identifiers": batch_params.splitter_filter_kwargs,
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
