from __future__ import annotations

import uuid
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Dict,
    Generic,
    List,
    Mapping,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

import pydantic
from pydantic import BaseModel, Field

from great_expectations.compatibility.typing_extensions import override

if TYPE_CHECKING:
    MappingIntStrAny = Mapping[Union[int, str], Any]
    AbstractSetIntStr = AbstractSet[Union[int, str]]


class MetricRepositoryBaseModel(BaseModel):
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


_ValueType = TypeVar("_ValueType")


class Metric(MetricRepositoryBaseModel, Generic[_ValueType]):
    """Abstract computed metric. Domain, value and parameters are metric dependent.

    Note: This implementation does not currently take into account
    other domain modifiers, e.g. row_condition, condition_parser, ignore_row_if
    """

    def __new__(cls, *args, **kwargs):
        if cls is Metric:
            raise NotImplementedError("Metric is an abstract class.")
        instance = super().__new__(cls)
        return instance

    batch_id: str = Field(description="Batch id")
    metric_name: str = Field(description="Metric name")
    value: _ValueType = Field(description="Metric value")
    exception: Optional[MetricException] = Field(
        description="Exception info if thrown", default=None
    )

    @classmethod
    def update_forward_refs(cls):
        from great_expectations.datasource.fluent.interfaces import Batch

        super().update_forward_refs(
            Batch=Batch,
        )

    @property
    def value_type(self) -> str:
        type_ = self.__orig_class__.__args__[0]
        string_rep = str(type_)
        if string_rep.startswith("<class"):
            return type_.__name__
        else:
            return string_rep

    @property
    def metric_type(self) -> str:
        return self.__class__.__name__

    @classmethod
    def get_properties(cls):
        return [
            prop for prop in cls.__dict__ if isinstance(cls.__dict__[prop], property)
        ]

    @override
    def dict(  # noqa: PLR0913
        self,
        *,
        include: AbstractSetIntStr | MappingIntStrAny | None = None,
        exclude: AbstractSetIntStr | MappingIntStrAny | None = None,
        by_alias: bool = False,
        skip_defaults: Optional[bool] = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
    ) -> Dict[str, Any]:
        """Override the dict function to include @property fields, in pydandic v2 we can use computed_field.
        https://docs.pydantic.dev/latest/usage/computed_fields/
        """
        attribs = super().dict(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            skip_defaults=skip_defaults,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
        )
        props = self.get_properties()

        # Include and exclude properties
        if include:
            props = [prop for prop in props if prop in include]
        if exclude:
            props = [prop for prop in props if prop not in exclude]

        # Update the attribute dict with the properties
        if props:
            attribs.update({prop: getattr(self, prop) for prop in props})
        return attribs


# Metric domain types


class TableMetric(Metric, Generic[_ValueType]):
    pass


class ColumnMetric(Metric, Generic[_ValueType]):
    column: str = Field(description="Column name")


# TODO: Add ColumnPairMetric, MultiColumnMetric


# Metrics with parameters (aka metric_value_kwargs)
# This is where the concrete metric types are defined that
# bring together a domain type, value type and any parameters (aka metric_value_kwargs)

# TODO: Add metrics here for all Column Descriptive Metrics
#  ColumnQuantileValuesMetric is an example of a metric that has parameters


class ColumnQuantileValuesMetric(ColumnMetric[List[float]]):
    quantiles: List[float] = Field(description="Quantiles to compute")
    allow_relative_error: Union[float, str] = Field(
        description="Relative error interpolation type (pandas) or limit (e.g. spark) depending on data source"
    )

    @property
    @override
    def value_type(self) -> str:
        return "list[float]"

    @property
    def metric_type(self) -> str:
        return self.__class__.__name__


class MetricRun(MetricRepositoryBaseModel):
    """Collection of Metric objects produced during the same execution run."""

    data_asset_id: Union[uuid.UUID, None] = Field(
        description="Data asset id", default=None
    )
    # created_at, created_by filled in by the backend.
    metrics: Sequence[Metric]
