from __future__ import annotations

import uuid
from typing import Any, Sequence, Union

import pydantic
from pydantic import BaseModel, Field


class CDMRBaseModel(BaseModel):  # TODO: Better name
    """Base class for all ColumnDescriptiveMetricRepository related models."""

    class Config:
        extra = pydantic.Extra.forbid


# TODO: Is run id just the "Metrics" id? ie the collection of metrics associated with a run?
class RunId(CDMRBaseModel):
    id: uuid.UUID = Field(description="Run id")
    # created_at, created_by filled in by the backend.


class Value(CDMRBaseModel):
    value: Any  # TODO: Better than Any


class Metric(CDMRBaseModel):
    id: uuid.UUID = Field(description="Metric id")
    run_id: uuid.UUID = Field(description="Run id")
    # TODO: reimplement batch param
    # batch: Batch = Field(description="Batch")
    metric_name: str = Field(description="Metric name")
    metric_domain_kwargs: dict = Field(description="Metric domain kwargs")
    metric_value_kwargs: dict = Field(description="Metric value kwargs")
    column: Union[str, None] = Field(description="Column name for column metrics")
    value: Value = Field(description="Metric value")
    details: dict = Field(description="Metric details")


class Metrics(CDMRBaseModel):
    """Collection of Metric objects."""

    metrics: Sequence[Metric]
