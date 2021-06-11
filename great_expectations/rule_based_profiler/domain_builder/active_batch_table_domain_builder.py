from typing import List, Optional

import great_expectations.exceptions as ge_exceptions
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder.domain import Domain
from great_expectations.rule_based_profiler.domain_builder.domain_builder import (
    DomainBuilder,
)
from great_expectations.validator.validator import Validator


class ActiveBatchTableDomainBuilder(DomainBuilder):
    """
    The interface method of ActiveBatchTableDomainBuilder emits a single Domain object, corresponding to the active
    Batch (hence the name).  Note that for appropriate use-cases, it should be readily possible to build a different
    implementation, where a separate Domain object is emitted for each individual batch_id (this use-case is deferred).
    """

    def _get_domains(
        self,
        *,
        validator: Optional[Validator] = None,
        batch_ids: Optional[List[str]] = None,
    ) -> List[Domain]:
        if validator is None:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"{self.__class__.__name__} requires a reference to an instance of the Validator class."
            )

        domains: List[Domain] = [
            Domain(
                domain_type=MetricDomainTypes.TABLE,
                domain_kwargs={
                    "batch_id": validator.active_batch_id,
                },
            )
        ]

        return domains
