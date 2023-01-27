from typing import List, Optional, Union

from great_expectations import DataContext
from great_expectations.core.domain import Domain, SemanticDomainTypes
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.helpers.util import (
    build_domains_from_column_names,
)
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,
)
from great_expectations.validator.metric_configuration import MetricConfiguration


class MyCustomSemanticTypeColumnDomainBuilder(DomainBuilder):
    """
    This custom DomainBuilder defines and filters for "user_id" semantic type fields
    """

    def __init__(
        self,
        data_context: DataContext,
        semantic_types: Optional[
            Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
        ] = None,
        column_name_suffixes: Optional[List[str]] = None,
    ):
        super().__init__(data_context=data_context)

        if semantic_types is None:
            semantic_types = ["user_id"]

        self._semantic_types = semantic_types

        if column_name_suffixes is None:
            column_name_suffixes = [
                "_id",
            ]

        self._column_name_suffixes = column_name_suffixes

    @property
    def domain_type(self) -> MetricDomainTypes:
        return MetricDomainTypes.COLUMN

    @property
    def semantic_types(
        self,
    ) -> Optional[
        Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
    ]:
        return self._semantic_types

    @property
    def column_name_suffixes(self) -> Optional[List[str]]:
        return self._column_name_suffixes

    def _get_domains(
        self,
        rule_name: str,
        variables: Optional[ParameterContainer] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> List[Domain]:
        """
        Find the semantic column type for each column and return all domains matching the specified type or types.
        """
        batch_ids: List[str] = self.get_batch_ids(variables=variables)
        table_column_names: List[str] = self.get_validator(
            variables=variables
        ).get_metric(
            metric=MetricConfiguration(
                metric_name="table.columns",
                metric_domain_kwargs={
                    "batch_id": batch_ids[-1],  # active_batch_id
                },
                metric_value_kwargs=None,
            )
        )

        # First check the column name ends in "_id".
        candidate_column_names: List[str] = list(
            filter(
                lambda candidate_column_name: candidate_column_name.endswith(
                    tuple(self.column_name_suffixes)
                ),
                table_column_names,
            )
        )

        column_name: str
        domains: List[Domain] = build_domains_from_column_names(
            rule_name=rule_name,
            column_names=candidate_column_names,
            domain_type=self.domain_type,
            table_column_name_to_inferred_semantic_domain_type_map=None,
        )

        return domains
