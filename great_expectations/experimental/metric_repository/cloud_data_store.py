from typing import TypeVar, Union

from typing_extensions import TypeAlias

from great_expectations.core.http import create_session
from great_expectations.data_context import AbstractDataContext
from great_expectations.experimental.metric_repository.data_store import DataStore
from great_expectations.experimental.metric_repository.metrics import MetricRun

StorableTypes: TypeAlias = Union[MetricRun,]  # TODO: are there better approaches?

T = TypeVar("T", bound=StorableTypes)


class CloudDataStore(DataStore[StorableTypes]):
    def __init__(self, context: AbstractDataContext):
        super().__init__(context=context)
        config = self._context._cloud_config
        self._session = create_session(access_token=config.access_token)
        self._url = (
            f"{config.base_url}/organizations/{config.organization_id}/metric-runs"
        )

    def _construct_json_payload(self, metric_run: MetricRun):
        result = {
            "id": str(metric_run.id),
            "metrics": [],
        }
        for metric in metric_run.metrics:
            result["metrics"].append(
                {
                    "id": str(metric.id),
                    "batch_id": metric.batch.id,
                    "data_asset_id": str(metric.batch.data_asset.id),
                    "metric_name": metric.metric_name,
                    "exception_message": metric.exception.exception_message,
                    "exception_type": metric.exception.exception_type,
                    "value": metric.value,
                    "metric_type": type(metric).__name__,
                    "value_type": type(metric.value).__name__,
                    "value_parameter_names": [],
                }
            )
        return {"data": {"attributes": result}}

    def add(self, value: T) -> T:
        print(f"Creating item of type {value.__class__.__name__}")
        print(f" in {self.__class__.__name__}")
        print("  sending a POST request to the cloud.")
        print(value)
        data = self._construct_json_payload(value)
        self._session.post(self._url, data)
        return value
