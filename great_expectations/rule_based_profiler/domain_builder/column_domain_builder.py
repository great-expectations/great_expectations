from typing import List, Optional

from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder.domain import Domain
from great_expectations.rule_based_profiler.domain_builder.domain_builder import (
    DomainBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder.parameter_container import (
    ParameterContainer,
)
from great_expectations.validator.validation_graph import MetricConfiguration


class ColumnDomainBuilder(DomainBuilder):
    def _get_domains(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> List[Domain]:
        """
        Obtains and returns domains for all columns of a table.
        """
        batch_id: str = self.get_batch_id(variables=variables)
        table_column_names: List[str] = self.get_validator(
            variables=variables
        ).get_metric(
            metric=MetricConfiguration(
                metric_name="table.columns",
                metric_domain_kwargs={
                    "batch_id": batch_id,
                },
                metric_value_kwargs=None,
                metric_dependencies=None,
            )
        )

        # Note: providing "batch_id" in "Domain.domain_kwargs" is important, because this "domain_kwargs" serves as the
        # default "metric_domain_kwargs" for "ParameterBuilder" logic.  Hence, it can be used for metric computations
        # (overridable if "batch_request" and/or "metric_domain_kwargs" override are configured for "ParameterBuilder").
        column_name: str
        domains: List[Domain] = [
            Domain(
                domain_type=MetricDomainTypes.COLUMN,
                domain_kwargs={
                    "column": column_name,
                    "batch_id": batch_id,
                },
            )
            for column_name in table_column_names
        ]

        return domains
