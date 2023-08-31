from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, TypeVar, Union

import pydantic
from pydantic import BaseModel

from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.http import create_session
from great_expectations.experimental.metric_repository.data_store import DataStore
from great_expectations.experimental.metric_repository.metrics import MetricRun

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    from great_expectations.data_context import CloudDataContext

StorableTypes: TypeAlias = Union[MetricRun,]

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
    @override
    def __init__(self, context: CloudDataContext):
        super().__init__(context=context)
        self._session = create_session(
            access_token=context.ge_cloud_config.access_token
        )

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
        url = self.build_url(value)
        payload = self.build_payload(value)
        self._session.post(url=url, data=payload)
        return value
