from typing import List, Optional

import great_expectations.exceptions as ge_exceptions
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.profiler.domain_builder.domain import Domain, StorageDomainTypes
from great_expectations.profiler.domain_builder.domain_builder import DomainBuilder
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.validator.validator import Validator


class ColumnDomainBuilder(DomainBuilder):
    def _get_domains(
        self,
        *,
        validator: Optional[Validator] = None,
        batch_ids: Optional[List[str]] = None,
        include_batch_id: Optional[bool] = True,
        domain_type: Optional[MetricDomainTypes] = None,
        **kwargs,
    ) -> List[Domain]:
        """
        Obtains and returns a given column
        """
        if not ((domain_type is None) or (domain_type == MetricDomainTypes.COLUMN)):
            raise ge_exceptions.ProfilerConfigurationError(
                message=f"{self.__class__.__name__} requires a COLUMN domain."
            )
        domains: List[Domain] = []
        columns: List[str] = validator.get_metric(
            metric=MetricConfiguration(
                metric_name="table.columns",
                metric_domain_kwargs={},
                metric_value_kwargs=None,
                metric_dependencies=None,
            )
        )

        column: str
        for column in columns:
            if include_batch_id:
                # TODO: <Alex>ALEX -- Should we use the "active_batch_id" or is there a reason to use one of the passed "batch_ids"?  Further, should we, in fact, include both, the "active_batch_id" as well as all "batch_ids" -- for multibatch case?</Alex>
                domains.append(
                    Domain(
                        domain_kwargs={
                            "column": column,
                            "batch_id": validator.active_batch_id,
                        },
                        domain_type=StorageDomainTypes.COLUMN,
                    )
                )
            else:
                domains.append(
                    Domain(
                        domain_kwargs={"column": column},
                        domain_type=StorageDomainTypes.COLUMN,
                    )
                )
        return domains

    # TODO: <Alex>ALEX -- this public method is a utility method; it is defined, but not used anywhere in the codebase.  If it is useful, then it should be moved to a utility module and declared as a static method.</Alex>
    def get_column_domains(
        self,
        *,
        validator: Optional[Validator] = None,
        batch_ids: Optional[List[str]] = None,
        include_batch_id: Optional[bool] = True,
        **kwargs,
    ) -> List[Domain]:
        """
        Pops column domain out of a dict of certain domain kwargs and requests this domain
        """
        domain_type: MetricDomainTypes = kwargs.pop(
            "domain_type", MetricDomainTypes.COLUMN
        )
        return self.get_domains(
            validator=validator,
            batch_ids=batch_ids,
            include_batch_id=include_batch_id,
            domain_type=domain_type,
            **kwargs,
        )
