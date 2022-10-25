from typing import List, Dict

from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.zep.interfaces import Batch, BatchRequest, DataAsset


class PostgresDatasource():
    def __init__(self, connection_str):
        # "postgresql+psycopg2://postgres:@localhost/test_ci"
        self.execution_engine = SqlAlchemyExecutionEngine(
            connection_string=connection_str
        )
        self.assets: Dict[str, DataAsset] = {}

    # BDIRKS: add and get asset can be added to the base class once I understand how
    #         Gabriel is managing this.
    def add_asset(self, data_asset_name: str, asset: DataAsset) -> None:
        self.assets[data_asset_name] = asset

    def get_asset(self, name) -> DataAsset:
        return self.assets[name]

    def get_batch_list_from_batch_request(
            self, batch_request: BatchRequest[SqlAlchemyDatasourceBatchSpec]
    ) -> List[Batch]:
        data_asset = self.get_asset(batch_request.data_asset_name)
        data, _ = self.execution_engine.get_batch_data_and_markers(
            batch_spec=batch_request.batch_spec
        )
        return [
            Batch(
                datasource=self,
                data_asset=data_asset,
                batch_request=batch_request,
                data=data
            )
        ]

