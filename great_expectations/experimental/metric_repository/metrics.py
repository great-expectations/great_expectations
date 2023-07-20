from __future__ import annotations

import uuid
from abc import ABC
from typing import Any, List, Sequence, Union

import pydantic
from pydantic import BaseModel, Field


class MetricRepositoryBaseModel(BaseModel, ABC):
    """Base class for all MetricRepository related models."""

    class Config:
        extra = pydantic.Extra.forbid


class Value(MetricRepositoryBaseModel):
    value: Any  # TODO: Better than Any


class Metric(MetricRepositoryBaseModel, ABC):
    id: uuid.UUID = Field(description="Metric id")
    run_id: uuid.UUID = Field(description="Run id")
    # TODO: reimplement batch param
    # batch: Batch = Field(description="Batch")
    metric_name: str = Field(description="Metric name")
    metric_domain_kwargs: dict = Field(description="Metric domain kwargs")
    metric_value_kwargs: dict = Field(description="Metric value kwargs")
    details: dict = Field(description="Metric details")


class TableMetric(Metric):
    pass


class NumericTableMetric(TableMetric):
    value: Union[int, float] = Field(description="Metric value")


class ColumnMetric(Metric):
    column: str = Field(description="Column name")


# TODO: Add ColumnPairMetric, MultiColumnMetric


# TODO: Add metric type specific metrics e.g. with value kwargs and value
#  specific to the metric and subclassing from one of the domain type metrics
#  e.g. QuantileValues(ColumnMetric)


class NumericColumnMetric(ColumnMetric):
    value: float = Field(description="Metric value")


class QuantileValues(ColumnMetric):
    quantiles: List[float] = Field(description="Quantiles to compute")
    allow_relative_error: Union[str, float] = Field(
        description="Relative error interpolation type (pandas) or limit (e.g. spark) depending on data source"
    )
    value: List[float] = Field(description="Metric value")


class Metrics(MetricRepositoryBaseModel):
    """Collection of Metric objects."""

    id: uuid.UUID = Field(description="Run id")
    # created_at, created_by filled in by the backend.
    metrics: Sequence[Metric]
