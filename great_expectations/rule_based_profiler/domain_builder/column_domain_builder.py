from typing import List, Optional

import great_expectations.exceptions as ge_exceptions
from great_expectations.rule_based_profiler.domain_builder.domain import Domain
from great_expectations.rule_based_profiler.domain_builder.domain_builder import (
    DomainBuilder,
)
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.validator.validator import Validator


class ColumnDomainBuilder(DomainBuilder):
    def _get_domains(
        self,
        *,
        validator: Optional[Validator] = None,
    ) -> List[Domain]:
        """
        Obtains and returns domains for all columns of a table.
        """
        if validator is None:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"{self.__class__.__name__} requires a reference to an instance of the Validator class."
            )

        table_column_names: List[str] = validator.get_metric(
            metric=MetricConfiguration(
                metric_name="table.columns",
                metric_domain_kwargs={
                    "batch_id": validator.active_batch_id,
                },
                metric_value_kwargs=None,
                metric_dependencies=None,
            )
        )

        column_name: str
        domains: List[Domain] = [
            Domain(
                domain_kwargs={
                    "column": column_name,
                    "batch_id": validator.active_batch_id,
                }
            )
            for column_name in table_column_names
        ]

        return domains
