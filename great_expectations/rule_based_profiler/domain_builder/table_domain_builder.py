from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.rule_based_profiler.domain import Domain
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,
)

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )


class TableDomainBuilder(DomainBuilder):
    def __init__(
        self,
        data_context: Optional[AbstractDataContext] = None,
    ) -> None:
        """
        Args:
            data_context: AbstractDataContext associated with this DomainBuilder
        """
        super().__init__(data_context=data_context)

    @property
    def domain_type(self) -> MetricDomainTypes:
        return MetricDomainTypes.TABLE

    """
    The interface method of TableDomainBuilder emits a single Domain object, corresponding to the implied Batch (table).

    Note that for appropriate use-cases, it should be readily possible to build a multi-batch implementation, where a
    separate Domain object is emitted for each individual Batch (using its respective batch_id).  (This is future work.)
    """

    def _get_domains(
        self,
        rule_name: str,
        variables: Optional[ParameterContainer] = None,
    ) -> List[Domain]:
        domains: List[Domain] = [
            Domain(
                domain_type=self.domain_type,
                rule_name=rule_name,
            ),
        ]

        return domains
