from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, TypeVar, Union

import pydantic
from pydantic import BaseModel

from great_expectations.compatibility.typing_extensions import override
from great_expectations.experimental.metric_repository.data_store import DataStore
from great_expectations.experimental.metric_repository.metrics import MetricRun

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

StorableTypes: TypeAlias = Union[MetricRun,]  # TODO: are there better approaches?

T = TypeVar("T", bound=StorableTypes)


def map_to_url(value: StorableTypes) -> str:
    if isinstance(value, MetricRun):
        return "/metric-runs"


def map_to_resource_type(value: StorableTypes) -> str:
    if isinstance(value, MetricRun):
        return "metric-run"


class PayloadData(BaseModel):
    type: str
    attributes: Dict[str, Any]

    class Config:
        extra = pydantic.Extra.forbid


class Payload(BaseModel):
    data: PayloadData

    class Config:
        extra = pydantic.Extra.forbid


class CloudDataStore(DataStore[StorableTypes]):
    def build_payload(self, value: StorableTypes) -> dict:
        payload = Payload(
            data=PayloadData(
                type=map_to_resource_type(value),
                attributes=value.dict(),
            )
        )
        return payload.dict()

    def build_url(self, value: StorableTypes) -> str:
        config = self._context.ge_cloud_config
        return f"{config.base_url}/organizations/{config.organization_id}{map_to_url(value)}"

    @override
    def add(self, value: T) -> T:
        # TODO: implementation
        # TODO: Serialize with organization_id from the context
        print(f"Creating item of type {value.__class__.__name__}")
        print(f" in {self.__class__.__name__}")
        print("  sending a POST request to the cloud.")
        print(value)
        return value
