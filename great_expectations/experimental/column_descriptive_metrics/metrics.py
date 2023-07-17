from __future__ import annotations

import uuid
from typing import Any, Sequence, Union

import pydantic
from pydantic import BaseModel, Field


class CDMRBaseModel(BaseModel):  # TODO: Better name
    """Base class for all ColumnDescriptiveMetricRepository related models."""

    class Config:
        extra = pydantic.Extra.forbid


class RunId(CDMRBaseModel):
    id: uuid.UUID = Field(description="Run id")
    organization_id: uuid.UUID = Field(
        description="Organization id"
    )  # TODO: Is this filled in by the backend?
    # created_at, created_by filled in by the backend.


class Value(CDMRBaseModel):
    value: Any  # TODO: Better than Any


class BatchPointer(CDMRBaseModel):  # TODO: Better name
    datasource_name: str = Field(description="Datasource name")
    data_asset_name: str = Field(description="Data asset name")
    batch_id: str = Field(description="Batch id")


class Metric(CDMRBaseModel):
    id: uuid.UUID = Field(description="Metric id")
    organization_id: uuid.UUID = Field(
        description="Organization id"
    )  # TODO: Is this filled in by the backend?
    run_id: uuid.UUID = Field(description="Run id")
    batch_pointer: BatchPointer = Field(description="Batch pointer")
    metric_name: str = Field(description="Metric name")
    metric_domain_kwargs: dict = Field(description="Metric domain kwargs")
    metric_value_kwargs: dict = Field(description="Metric value kwargs")
    column: Union[str, None] = Field(description="Column name for column metrics")
    value: Value = Field(description="Metric value")
    details: dict = Field(description="Metric details")


class Metrics(CDMRBaseModel):
    """Collection of Metric objects."""

    metrics: Sequence[Metric]
