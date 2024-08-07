from typing import Protocol
from contrib.experimental.metrics.mp_asset import MPBatchDefinition, MPBatchParameters
from great_expectations.compatibility import pydantic


class MPConditionBlock(pydantic.BaseModel):
    """This is a placeholder for however we want to represent
    condition blocks / row conditions. They need to be part of the domain id, and
    they need to be part of how the provider-specific implementation computes the metric.
    """

    def get_id(self, batch_parameters: MPBatchParameters) -> str:
        return "TODO"


class MetricDomain(Protocol):
    batch_definition: MPBatchDefinition
    batch_parameters: MPBatchParameters
    domain_conditions: MPConditionBlock | None

    @property
    def id(self) -> str:
        ...


class ColumnMetricDomain(pydantic.BaseModel):
    batch_definition: MPBatchDefinition
    batch_parameters: MPBatchParameters
    domain_conditions: MPConditionBlock | None = None
    column: str

    @property
    def id(self) -> str:
        return "__".join((
            self.batch_definition.get_id(self.batch_parameters),
            self.domain_conditions.get_id(self.batch_parameters) if self.domain_conditions is not None else "",
            self.column
        ))