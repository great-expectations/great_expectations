
from typing import TypeVar, Union
from contrib.experimental.metrics.mp_asset import MPBatchDefinition
from contrib.experimental.metrics.mp_asset import MPBatchParameters, MPPartitioner, MPTableAsset


SnowflakeAssetPartitionerT = TypeVar("SnowflakeAssetPartitionerT", bound=Union["SnowflakeTableAssetColumnYearlyPartitioner", "SnowflakeTableAssetColumnDailyPartitioner"])

class SnowflakeTableAssetColumnYearlyPartitioner(MPPartitioner):
    column: str

    def get_where_clause_str(self, batch_parameters: MPBatchParameters) -> str:
        return f"YEAR({self.column}) = {batch_parameters['year']}"


class SnowflakeTableAssetColumnDailyPartitioner(MPPartitioner):
    column: str

    def get_where_clause_str(self, batch_parameters: MPBatchParameters) -> str:
        return f"YEAR({self.column}) = {batch_parameters['year']} AND MONTH({self.column}) = {batch_parameters['month']} AND DAY({self.column}) = {batch_parameters['day']}"



class SnowflakeMPBatchDefinition(MPBatchDefinition[MPTableAsset, SnowflakeAssetPartitionerT]):
    def get_selectable_str(self, batch_parameters: MPBatchParameters) -> str:
        return f"{self.data_asset.table_name} WHERE {self.partitioner.get_where_clause_str(batch_parameters=batch_parameters)}"


