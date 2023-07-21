from __future__ import annotations

import uuid
from abc import ABC
from typing import TYPE_CHECKING, List, Optional, Sequence, Union

import pydantic
from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import Batch


class MetricRepositoryBaseModel(BaseModel, ABC):
    """Base class for all MetricRepository related models."""

    class Config:
        extra = pydantic.Extra.forbid


class MetricException(MetricRepositoryBaseModel):
    exception_type: Optional[str] = Field(
        description="Exception type if an exception is thrown", default=None
    )
    exception_message: Optional[str] = Field(
        description="Exception message if an exception is thrown", default=None
    )


class Metric(MetricRepositoryBaseModel, ABC):
    """Abstract computed metric. Domain, value and parameters are metric dependent.

    Note: This implementation does not currently take into account
    other domain modifiers, e.g. row_condition, condition_parser, ignore_row_if
    """

    id: uuid.UUID = Field(description="Metric id")
    run_id: uuid.UUID = Field(description="Run id")
    batch: Batch = Field(description="Batch")
    metric_name: str = Field(description="Metric name")
    exception: MetricException = Field(description="Exception info if thrown")


# Metric domain types


class TableMetric(Metric, ABC):
    pass


class ColumnMetric(Metric, ABC):
    column: str = Field(description="Column name")


# TODO: Add ColumnPairMetric, MultiColumnMetric


class NumericMetric(Metric, ABC):
    value: Union[int, float] = Field(description="Metric value")


class NumericListMetric(Metric, ABC):
    value: List[float] = Field(description="Metric value")


class StringListMetric(Metric, ABC):
    value: List[str] = Field(description="Metric value")


# Concrete Metrics
# This is where the concrete metric types are defined that
# bring together a domain type, value type and any parameters (aka metric_value_kwargs)

# TODO: Add metrics here for all Column Descriptive Metrics


class NumericTableMetric(TableMetric, NumericMetric):
    pass


class StringListTableMetric(TableMetric, StringListMetric):
    pass


class QuantileValuesColumnMetric(ColumnMetric, NumericListMetric):
    quantiles: List[float] = Field(description="Quantiles to compute")
    allow_relative_error: Union[str, float] = Field(
        description="Relative error interpolation type (pandas) or limit (e.g. spark) depending on data source"
    )


class MetricRun(MetricRepositoryBaseModel):
    """Collection of Metric objects produced during the same execution run."""

    id: uuid.UUID = Field(description="Run id")
    # created_at, created_by filled in by the backend.
    metrics: Sequence[Metric]
