from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, Dict, TypeVar

from great_expectations.compatibility.pydantic import BaseModel
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.http import create_session
from great_expectations.experimental.metric_repository.data_store import DataStore
from great_expectations.experimental.metric_repository.metrics import MetricRun

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    from great_expectations.data_context import CloudDataContext

# When more types are storable, convert StorableTypes to a Union and add them to the type alias:
StorableTypes: TypeAlias = MetricRun

T = TypeVar("T", bound=StorableTypes)


class PayloadData(BaseModel):
    type: str
    attributes: Dict[str, Any]

    class Config:
        extra = "forbid"


def orjson_dumps(v, *, default):
    import orjson  # Import here since this is only installed in the cloud environment

    # orjson.dumps returns bytes, to match standard json.dumps we need to decode
    return orjson.dumps(
        v,
        default=default,
        option=orjson.OPT_SERIALIZE_NUMPY,
    ).decode()


def orjson_loads(v, *args, **kwargs):
    import orjson  # Import here since this is only installed in the cloud environment

    return orjson.loads(v)


class Payload(BaseModel):
    data: PayloadData

    class Config:
        extra = "forbid"
        json_dumps = orjson_dumps
        json_loads = orjson_loads


class CloudDataStore(DataStore[StorableTypes]):
    """DataStore implementation for GX Cloud.

    Uses JSON:API https://jsonapi.org/
    """

    @override
    def __init__(self, context: CloudDataContext):
        super().__init__(context=context)
        assert context.ge_cloud_config is not None
        assert self._context.ge_cloud_config is not None
        self._session = create_session(
            access_token=context.ge_cloud_config.access_token
        )

    def _map_to_url(self, value: StorableTypes) -> str:
        if isinstance(value, MetricRun):
            return "/metric-runs"

    def _map_to_resource_type(self, value: StorableTypes) -> str:
        if isinstance(value, MetricRun):
            return "metric-run"

    def _build_payload(self, value: StorableTypes) -> str:
        payload = Payload(
            data=PayloadData(
                type=self._map_to_resource_type(value),
                attributes=value.dict(
                    exclude={"metrics": {"__all__": {"__orig_class__"}}}
                ),
            )
        )
        return payload.json()

    def _build_url(self, value: StorableTypes) -> str:
        assert self._context.ge_cloud_config is not None
        config = self._context.ge_cloud_config
        return f"{config.base_url}/organizations/{config.organization_id}{self._map_to_url(value)}"

    @override
    def add(self, value: T) -> uuid.UUID:
        """Add a value to the DataStore.

        Args:
            value: Value to add to the DataStore. Must be one of StorableTypes.

        Returns:
            id of the created resource.
        """
        url = self._build_url(value)
        payload = self._build_payload(value)
        response = self._session.post(url=url, data=payload)
        response.raise_for_status()

        response_json = response.json()
        return uuid.UUID(response_json["id"])
