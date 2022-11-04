# import abc
from typing import Dict, List, Type, Union

from pydantic import BaseModel, constr, validator
from typing_extensions import ClassVar, TypeAlias

from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.zep.metadatasource import MetaDatasource

LowerStr: TypeAlias = constr(to_lower=True, strict=True)  # type: ignore[misc]


class DataAsset(BaseModel):
    name: str
    type: LowerStr


# TODO: resolve metaclass conflict with pydantic BaseModel
class Datasource(metaclass=MetaDatasource):
    # class attrs
    asset_types: ClassVar[List[Type[DataAsset]]] = []

    class Datasource(BaseModel):
        name: str
        engine: str
        execution_engine: ExecutionEngine
        assets: Dict[str, DataAsset]

        @validator("engine", pre=True)
        def load_execution_engine(cls, v, values):
            print("\n\nvalidating engine")
            print(v)
            print(values)
            return v

        class Config:
            # TODO: revisit this
            arbitrary_types_allowed = True

    # instance attrs
    name: str
    execution_engine: ExecutionEngine
    assets: Dict[str, DataAsset]

    def get_batch_list_from_batch_request(
        self, batch_request: Union[BatchRequest, RuntimeBatchRequest]
    ) -> List[Batch]:
        """Processes a batch request and returns a list of batches.

        Args:
            batch_request: contains parameters necessary to retrieve batches.

        Returns:
            A list of batches. The list may be empty.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement `.get_batch_list_from_batch_request()`"
        )

    def get_asset(self, asset_name: str) -> DataAsset:
        """Returns the DataAsset referred to by name"""
        # This default implementation will be used if protocol is inherited
        return self.assets[asset_name]
