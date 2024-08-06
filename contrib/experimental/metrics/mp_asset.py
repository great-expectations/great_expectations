from abc import abstractmethod
import hashlib
import json
from typing import Any, Generic, TypeVar
from great_expectations.compatibility import pydantic


from pydantic import ConfigDict, StrictStr

MPAssetT = TypeVar("MPAssetT", bound="MPAsset")
MPPartitionerT = TypeVar("MPPartitionerT", bound="MPPartitioner")

class MPAsset(pydantic.BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    name: str
    _batch_definitions: list["MPBatchDefinition"] = pydantic.PrivateAttr(default_factory=list)

    def add_batch_definition(self, batch_definition: "MPBatchDefinition") -> None:
        self._batch_definitions.append(batch_definition)

    def get_batch_definition(self, name: str) -> "MPBatchDefinition":
        return next(batch_definition for batch_definition in self._batch_definitions if batch_definition.name == name)


class MPPartitioner(pydantic.BaseModel):
    model_config = ConfigDict(frozen=True)

class MPBatchParameters(dict[StrictStr, Any]):
    @property
    def id(self) -> str:
        return hashlib.md5(json.dumps(self, sort_keys=True).encode("utf-8")).hexdigest()


class MPBatchDefinition(pydantic.GenericModel, Generic[MPAssetT, MPPartitionerT]):
    model_config = ConfigDict(frozen=True)

    id: str
    name: str
    data_asset: MPAssetT
    partitioner: MPPartitionerT

    def get_id(self, batch_parameters: MPBatchParameters) -> str:
        # TODO: quick-and-dirty naive implementation
        return "__".join((
            self.data_asset.id,
            self.id,
            batch_parameters.id
        ))


class MPTableAsset(MPAsset):
    table_name: str
