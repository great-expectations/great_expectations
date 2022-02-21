from typing import List, Optional, Union

from great_expectations import DataContext
from great_expectations.core.batch import Batch, BatchRequest
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.types import (
    Domain,
    ParameterContainer,
    SemanticDomainTypes,
)
from great_expectations.validator.metric_configuration import MetricConfiguration


class MyCustomSemanticTypeColumnDomainBuilder(DomainBuilder):
    """
    This custom DomainBuilder defines and filters for "user_id" semantic type fields
    """

    def __init__(
        self,
        data_context: DataContext,
        batch: Optional[Batch] = None,
        batch_request: Union[BatchRequest, dict] = None,
        semantic_types: Optional[
            Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
        ] = None,
        column_name_suffixes: Optional[List[str]] = None,
    ):
        super().__init__(
            data_context=data_context,
            batch=batch,
            batch_request=batch_request,
        )

        if semantic_types is None:
            semantic_types = ["user_id"]

        self._semantic_types = semantic_types

        if column_name_suffixes is None:
            column_name_suffixes = [
                "_id",
            ]

        self._column_name_suffixes = column_name_suffixes

    @property
    def domain_type(self) -> Union[str, MetricDomainTypes]:
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
        variables: Optional[ParameterContainer] = None,
    ) -> List[Domain]:
        """
        Find the semantic column type for each column and return all domains matching the specified type or types.
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
        domains: List[Domain] = [
            Domain(
                domain_type=MetricDomainTypes.COLUMN,
                domain_kwargs={
                    "column": column_name,
                },
            )
            for column_name in candidate_column_names
        ]

        return domains
