from typing import Iterable, List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.domain_types import (
    DomainTypes,
    MetricDomainTypes,
    SemanticDomainTypes,
)
from great_expectations.profiler.domain_builder.column_domain_builder import (
    ColumnDomainBuilder,
)
from great_expectations.profiler.domain_builder.domain import Domain
from great_expectations.profiler.util import (
    translate_table_column_type_to_semantic_domain_type,
)
from great_expectations.validator.validator import MetricConfiguration, Validator


class SimpleColumnSuffixDomainBuilder(ColumnDomainBuilder):
    """
    This DomainBuilder uses a column suffix to identify domains.
    """

    def __init__(self, column_name_suffixes: Optional[List[str]] = None):
        if column_name_suffixes is None:
            column_name_suffixes = []
        self._column_name_suffixes = column_name_suffixes

    def _get_domains(
        self,
        *,
        validator: Optional[Validator] = None,
        batch_ids: Optional[List[str]] = None,
        domain_type: Optional[MetricDomainTypes] = None,
        **kwargs,
    ) -> List[Domain]:
        """
        Find the column suffix for each column and return all domains matching the specified suffix.
        """
        if validator is None:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"{self.__class__.__name__} requires a reference to an instance of the Validator class."
            )

        config: dict = kwargs
        column_name_suffixes: Union[str, Iterable, List[str]] = config.get(
            "column_name_suffixes"
        )
        if column_name_suffixes is None:
            column_name_suffixes = self._column_name_suffixes
        elif isinstance(column_name_suffixes, str):
            column_name_suffixes = [column_name_suffixes]
        elif isinstance(column_name_suffixes, (Iterable, List)):
            pass
        else:
            raise ValueError(
                "Unrecognized column_name_suffixes directive -- must be a list or a string."
            )

        column_names: List[str] = validator.get_metric(
            metric=MetricConfiguration(
                metric_name="table.columns",
                metric_domain_kwargs={},
                metric_value_kwargs=None,
                metric_dependencies=None,
            )
        )
        domains: List[Domain] = []
        column_name: str

        for column_name in column_names:
            if column_name.endswith(tuple(column_name_suffixes)):
                domains.append(
                    Domain(
                        domain_kwargs={
                            "column": column_name,
                            "batch_id": validator.active_batch_id,
                        },
                        # TODO: AJB 20210503 This should be from an enum
                        domain_type="column",
                    )
                )

        return domains
