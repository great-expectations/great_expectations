from typing import Annotated, ClassVar, List, Literal, Optional, Type, Union

from pydantic import Field

from great_expectations.core.batch_definition import BatchDefinition, PartitionerT
from great_expectations.core.partitioners import ColumnPartitionerYearly
from great_expectations.datasource.fluent.interfaces import DataAsset, Datasource

# class SnowflakeTableAsset(DataAsset):
#     type: Literal["table"] = "table"

#     def test_connection(self) -> None:
#         """Test the connection for the DataAsset.

#         Raises:
#             TestConnectionError: If the connection test fails.
#         """
#         pass

#     def get_batch_parameters_keys(
#         self, partitioner: Optional[PartitionerT] = None
#     ) -> tuple[str, ...]:
#         raise NotImplementedError(
#             """One needs to implement "get_batch_parameters_keys" on a DataAsset subclass."""
#         )

#     def build_batch_request(
#         self,
#         options: Optional[BatchParameters] = None,
#         batch_slice: Optional[BatchSlice] = None,
#         partitioner: Optional[PartitionerT] = None,
#     ) -> BatchRequest[PartitionerT]:
#         """A batch request that can be used to obtain batches for this DataAsset.

#         Args:
#             options: A dict that can be used to filter the batch groups returned from the asset.
#                 The dict structure depends on the asset type. The available keys for dict can be obtained by
#                 calling get_batch_parameters_keys(...).
#             batch_slice: A python slice that can be used to limit the sorted batches by index.
#                 e.g. `batch_slice = "[-5:]"` will request only the last 5 batches after the options filter is applied.
#             partitioner: A Partitioner used to narrow the data returned from the asset.

#         Returns:
#             A BatchRequest object that can be used to obtain a batch list from a Datasource by calling the
#             get_batch_list_from_batch_request method.
#         """  # noqa: E501
#         raise NotImplementedError(
#             """One must implement "build_batch_request" on a DataAsset subclass."""
#         )

#     def get_batch_list_from_batch_request(self, batch_request: BatchRequest) -> List[Batch]:
#         raise NotImplementedError

#     def _validate_batch_request(self, batch_request: BatchRequest) -> None:
#         """Validates the batch_request has the correct form.

#         Args:
#             batch_request: A batch request object to be validated.
#         """
#         raise NotImplementedError(
#             """One must implement "_validate_batch_request" on a DataAsset subclass."""
#         )

#     # End Abstract Methods

#     def add_batch_definition(
#         self,
#         name: str,
#         partitioner: Optional[PartitionerT] = None,
#     ) -> BatchDefinition[PartitionerT]:
#         """Add a BatchDefinition to this DataAsset.
#         BatchDefinition names must be unique within a DataAsset.

#         If the DataAsset is tied to a DataContext, the BatchDefinition will be persisted.

#         Args:
#             name (str): Name of the new batch definition.
#             partitioner: Optional Partitioner to partition this BatchDefinition

#         Returns:
#             BatchDefinition: The new batch definition.
#         """
#         batch_definition_names = {bc.name for bc in self.batch_definitions}
#         if name in batch_definition_names:
#             raise ValueError(  # noqa: TRY003
#                 f'"{name}" already exists (all existing batch_definition names are {", ".join(batch_definition_names)})'  # noqa: E501
#             )

#         # Let mypy know that self.datasource is a Datasource (it is currently bound to MetaDatasource)  # noqa: E501
#         assert isinstance(self.datasource, Datasource)

#         batch_definition = BatchDefinition[PartitionerT](name=name, partitioner=partitioner)
#         batch_definition.set_data_asset(self)
#         self.batch_definitions.append(batch_definition)
#         self.update_batch_definition_field_set()
#         if self.datasource.data_context:
#             try:
#                 batch_definition = self.datasource.add_batch_definition(batch_definition)
#             except Exception:
#                 self.batch_definitions.remove(batch_definition)
#                 self.update_batch_definition_field_set()
#                 raise
#         self.update_batch_definition_field_set()
#         return batch_definition


#     def add_batch_definition_yearly(
#         self, name: str, column: str, sort_ascending: bool = True
#     ) -> BatchDefinition:
#         return self.add_batch_definition(
#             name=name,
#             partitioner=ColumnPartitionerYearly(column_name=column, sort_ascending=sort_ascending),
#         )

# class SnowflakeViewAsset(DataAsset):
#     type: Literal["view"] = "view"

# AssetTypes = Annotated[Union[SnowflakeTableAsset, SnowflakeViewAsset], Field(discriminator="type")]


# class SnowflakeDatasource(Datasource):
#     asset_types: ClassVar[List[Type[DataAsset]]] = [SnowflakeTableAsset]
#     type: Literal["snowflake"] = "snowflake"

#     assets: List[AssetTypes] = []

#     def test_connection(self, test_assets: bool = True) -> None:
#         pass


from abc import ABC, abstractmethod

from pydantic import BaseModel

# pip install snowflake-connector-python
import snowflake
import snowflake.connector


class SnowflakeConnector(ABC, BaseModel):
    @abstractmethod
    def get_connection(self) -> str:
        pass

class SnowflakeUsernamePasswordConnection(SnowflakeConnector):
    user: str
    password: str
    account: str
    warehouse: str
    database: str
    schema: str

    def get_connection(self) -> snowflake.connector.SnowflakeConnection:
        return snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema
        )


class SnowflakeDatasource:
    def __init__(self, connector: SnowflakeConnector):
        self._con = connector.get_connection()


class SnowflakeTableAsset:
    def __init__(self, table: str):
        self._table = table

    @property
    def table(self):
        return self._table
    

class MetricProviderBatchDefinition(ABC, Generic[MP]):
    @abstractmethod
    def get_batch(self):
        pass


class SnowflakeTableAssetDailyBatchDefinition(BatchDefinition):
    def __init__(self, table: SnowflakeTableAsset, column: str):
        self._table = table
        self._column = column

    def get_batch(self, batch_parameters: BatchParameters):
        return f"FROM {self._table.table} WHERE {self._column} = {batch_parameters.get_batch_parameter('hourly')}"