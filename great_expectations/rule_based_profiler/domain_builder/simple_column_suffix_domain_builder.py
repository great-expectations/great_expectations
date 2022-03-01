from typing import Iterable, List, Optional, Union

from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer
from great_expectations.validator.metric_configuration import MetricConfiguration


class SimpleColumnSuffixDomainBuilder(DomainBuilder):
    """
    This DomainBuilder uses a column suffix to identify domains.
    """

    def __init__(
        self,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
        data_context: Optional["DataContext"] = None,  # noqa: F821
        column_name_suffixes: Optional[List[str]] = None,
    ):
        """
        Args:
            batch_list: explicitly specified Batch objects foruse in DomainBuilder
            batch_request: specified in DomainBuilder configuration to get Batch objects for domain computation.
            data_context: DataContext
        """
        super().__init__(
            batch_list=batch_list,
            batch_request=batch_request,
            data_context=data_context,
        )

        if column_name_suffixes is None:
            column_name_suffixes = []

        self._column_name_suffixes = column_name_suffixes

    @property
    def domain_type(self) -> Union[str, MetricDomainTypes]:
        return MetricDomainTypes.COLUMN

    @property
    def column_name_suffixes(self) -> Optional[List[str]]:
        return self._column_name_suffixes

    def _get_domains(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> List[Domain]:
        """
        Find the column suffix for each column and return all domains matching the specified suffix.
        """
        column_name_suffixes: Union[
            str, Iterable, List[str]
        ] = self.column_name_suffixes
        if isinstance(column_name_suffixes, str):
            column_name_suffixes = [column_name_suffixes]
        else:
            if not isinstance(column_name_suffixes, (Iterable, List)):
                raise ValueError(
                    "Unrecognized column_name_suffixes directive -- must be a list or a string."
                )

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

        candidate_column_names: List[str] = list(
            filter(
                lambda candidate_column_name: candidate_column_name.endswith(
                    tuple(column_name_suffixes)
                ),
                table_column_names,
            )
        )

        column_name: str
        domains: List[Domain] = [
            Domain(
                domain_type=self.domain_type,
                domain_kwargs={
                    "column": column_name,
                },
            )
            for column_name in candidate_column_names
        ]

        return domains
