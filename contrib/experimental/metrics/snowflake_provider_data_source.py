import uuid
from contrib.experimental.metrics.data_source import MPDataSource
from contrib.experimental.metrics.snowflake_provider import SnowflakeConnectionMetricProvider
from contrib.experimental.metrics.snowflake_provider_asset import SnowflakeMPTableAsset
from contrib.experimental.metrics.snowflake_provider_batch_definition import SnowflakeMPBatchDefinition, SnowflakeTableAssetColumnDailyPartitioner
from great_expectations.compatibility import pydantic

from great_expectations.core.partitioners import ColumnPartitionerDaily
from great_expectations.datasource.fluent.config_str import ConfigStr


from abc import ABC, abstractmethod

# pip install snowflake-connector-python
from great_expectations.datasource.fluent.snowflake_datasource import ConnectionDetails, SnowflakeDatasource
import snowflake
import snowflake.connector

from great_expectations.datasource.fluent.sql_datasource import TableAsset


class SnowflakeConnector(ABC, pydantic.BaseModel):
    @abstractmethod
    def get_connection(self) -> snowflake.connector.SnowflakeConnection:
        pass

class SnowflakeUsernamePasswordConnector(SnowflakeConnector):
    user: str
    password: str
    account: str
    warehouse: str
    database: str
    schema_: str
    role: str

    def get_connection(self) -> snowflake.connector.SnowflakeConnection:
        return snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema_,
            role=self.role,
        )
    

class MPSnowflakeDataSource(MPDataSource):
    def __init__(self, connector: SnowflakeConnector):
        self._connector = connector
        self._assets = []

    def add_table_asset(self, table_asset: SnowflakeMPTableAsset):
        self._assets.append(table_asset)

    def get_metric_provider(self) -> SnowflakeConnectionMetricProvider:
        connection = self._connector.get_connection()
        return SnowflakeConnectionMetricProvider(connection=connection)


def build_MetricProviderSnowflakeDatasource_from_SnowflakeDatasource(datasource: SnowflakeDatasource) -> "MPSnowflakeDataSource":
    if datasource.schema_ is None:
        raise ValueError("schema_ is required")
    if datasource.role is None:
        raise ValueError("role is required")
    if datasource.warehouse is None:
        raise ValueError("warehouse is required")
    if datasource.database is None:
        raise ValueError("database is required")
    
    if isinstance(datasource.connection_string, ConnectionDetails):
        account = datasource.connection_string.account
        user = datasource.connection_string.user
        password = datasource.connection_string.password
        if isinstance(password, ConfigStr):
            password = password.get_secret_value()
            
    else:
        raise ValueError("PROTOTYPE: connection_string must be a ConnectionDetails object")
    

    new_datasource = MPSnowflakeDataSource(
        connector=SnowflakeUsernamePasswordConnector(
            user=user,
            password=password,
            account=account,
            schema_=datasource.schema_,
            database=datasource.database,
            warehouse=datasource.warehouse,
            role=datasource.role,
        )
    )
    for asset in datasource.assets:
        if isinstance(asset, TableAsset):
            asset_id = asset.id.hex if asset.id else uuid.uuid4().hex
            new_asset = SnowflakeMPTableAsset(id=asset_id, name=asset.name, table_name=asset.table_name)
            for batch_definition in asset.batch_definitions:
                if isinstance(batch_definition.partitioner, ColumnPartitionerDaily):
                    batch_definition_id = batch_definition.id if batch_definition.id else uuid.uuid4().hex
                    new_asset.add_batch_definition(
                        SnowflakeMPBatchDefinition(
                            id=batch_definition_id,
                            name=batch_definition.name,
                            data_asset=new_asset,
                            partitioner=SnowflakeTableAssetColumnDailyPartitioner(column=batch_definition.partitioner.column_name),
                        )
                    )
                else:
                    raise ValueError(f"Unsupported partitioner {batch_definition.partitioner}")
            new_datasource.add_table_asset(table_asset=new_asset)
    return new_datasource

