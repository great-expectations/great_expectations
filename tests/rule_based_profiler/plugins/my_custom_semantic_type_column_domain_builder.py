from typing import List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.domain_types import SemanticDomainTypes
from great_expectations.rule_based_profiler.domain_builder.domain import Domain
from great_expectations.rule_based_profiler.domain_builder.domain_builder import (
    DomainBuilder,
)
from great_expectations.validator.validator import MetricConfiguration, Validator


class MyCustomSemanticTypeColumnDomainBuilder(DomainBuilder):
    """
    This custom DomainBuilder defines and filters for "user_id" semantic type fields
    """

    def __init__(
        self,
        semantic_types: Optional[
            Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
        ] = None,
        column_name_suffixes: Optional[List[str]] = None,
    ):
        if semantic_types is None:
            semantic_types = ["user_id"]
        self._semantic_types = semantic_types

        if column_name_suffixes is None:
            column_name_suffixes = [
                "_id",
            ]
        self._column_name_suffixes = column_name_suffixes

    def _get_domains(
        self,
        *,
        validator: Optional[Validator] = None,
        batch_ids: Optional[List[str]] = None,
        **kwargs,
    ) -> List[Domain]:
        """
        Find the semantic column type for each column and return all domains matching the specified type or types.
        """
        if validator is None:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"{self.__class__.__name__} requires a reference to an instance of the Validator class."
            )

        table_column_names: List[str] = validator.get_metric(
            metric=MetricConfiguration(
                metric_name="table.columns",
                metric_domain_kwargs={},
                metric_value_kwargs=None,
                metric_dependencies=None,
            )
        )

        # First check the column name ends in "_id"
        candidate_column_names: List[str] = list(
            filter(
                lambda candidate_column_name: candidate_column_name.endswith(
                    tuple(self._column_name_suffixes)
                ),
                table_column_names,
            )
        )

        column_name: str
        domains: List[Domain] = [
            Domain(
                domain_kwargs={
                    "column": column_name,
                    "batch_id": validator.active_batch_id,
                },
                domain_type=SemanticDomainTypes.IDENTITY,
            )
            for column_name in candidate_column_names
        ]

        return domains
