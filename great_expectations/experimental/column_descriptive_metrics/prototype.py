from __future__ import annotations

from typing import TYPE_CHECKING, List, Union

from pydantic import BaseModel

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    from great_expectations.datasource.fluent import BatchRequest, DataAsset


class Metric(BaseModel):
    name: str
    # TODO: Add fields


class Metrics(BaseModel):
    """Collection of Metric objects."""

    metrics: List[Metric]
    # TODO: Add fields
    # TODO: Consider adding CRUD methods?


CloudStorableTypes: TypeAlias = Union[Metrics,]  # TODO: are there better approaches?


class CloudStoreBackend:  # TODO: Name this better
    pass
    # TODO: Add methods
    # TODO: Investigate - If only implementing Create, can we use existing?

    def create(
        self, value_type: CloudStorableTypes, value: CloudStorableTypes
    ) -> None:  # TODO: How to annotate?
        print(
            f"Creating item of type {value_type.__name__} in CloudStoreBackend - sending a POST REST API request to the cloud."
        )
        print(value)


class ColumnDescriptiveMetricsStore:
    pass
    # TODO: Add methods

    def __init__(self, backend: CloudStoreBackend):
        self._backend = backend

    def create(self, metrics: Metrics) -> None:
        print("Creating metric in ColumnDescriptiveMetricsStore")
        self._backend.create(
            value_type=Metrics, value=metrics
        )  # TODO: How to annotate/implement?


# TODO: How does agent pass batch request? Assuming batch_request is necessary to specify a specific batch for introspection
#  rather than just the data asset as a full batch.
def inspect_asset(asset: DataAsset, batch_request: BatchRequest | None) -> Metrics:
    """Inspect a DataAsset and return Metrics."""
    print("This method would be invoked via an AgentAction")
    print(f"Inspecting data asset: {asset.name}")
    print(
        "NOTE: Here is where we can use things like DomainBuilder and MetricCalculator to calculate metrics"
    )
    if batch_request is None:
        batch_request = asset.build_batch_request()
    metrics = Metrics(
        metrics=[Metric(name="my_metric")]
    )  # TODO: Use DomainBuilder and MetricCalculator to generate.
    return metrics


# TODO: Alternative approach - use an Inspector class:
class AssetInspector:  # TODO: Name this better, or is this just a single method instead of class?
    pass
    # TODO: Add methods

    def __init__(self, asset: DataAsset) -> None:
        self._asset = asset

    def inspect_batch(
        self, batch_request: BatchRequest
    ) -> Metrics:  # TODO: Should this take a batch instead of a batch request?
        print("This method would be invoked via an AgentAction")
        print(
            f"Inspecting data asset: {self._asset.name} with batch request:\n{batch_request}"
        )
        print(
            "NOTE: Here is where we can use things like DomainBuilder and MetricCalculator to calculate metrics"
        )
        metrics = Metrics(
            metrics=[Metric(name="my_metric")]
        )  # TODO: Use DomainBuilder and MetricCalculator to generate.
        return metrics
