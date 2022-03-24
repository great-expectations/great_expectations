from typing import List, Optional, Union

from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer


class TableDomainBuilder(DomainBuilder):
    def __init__(
        self,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[
            Union[str, BatchRequest, RuntimeBatchRequest, dict]
        ] = None,
        data_context: Optional["DataContext"] = None,  # noqa: F821
    ):
        """
        Args:
            batch_list: explicitly specified Batch objects for use in DomainBuilder
            batch_request: specified in DomainBuilder configuration to get Batch objects for domain computation.
            data_context: DataContext
        """
        super().__init__(
            batch_list=batch_list,
            batch_request=batch_request,
            data_context=data_context,
        )

    @property
    def domain_type(self) -> Union[str, MetricDomainTypes]:
        return MetricDomainTypes.TABLE

    """
    The interface method of TableDomainBuilder emits a single Domain object, corresponding to the implied Batch (table).

    Note that for appropriate use-cases, it should be readily possible to build a multi-batch implementation, where a
    separate Domain object is emitted for each individual Batch (using its respective batch_id).  (This is future work.)
    """

    def _get_domains(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> List[Domain]:
        domains: List[Domain] = [
            Domain(
                domain_type=self.domain_type,
            )
        ]

        return domains
